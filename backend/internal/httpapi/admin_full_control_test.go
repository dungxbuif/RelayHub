package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/dungxbuif/RelayHub/internal/store"
)

func TestAdminFunctionFacadeIsAppScopedAndCredentialFree(t *testing.T) {
	router, apps, functions := newAdminFullControlRouter(t)
	app, _, err := apps.Create(context.Background(), service.CreateApp{Name: "orders", DeliveryMode: domain.DeliveryQueue})
	if err != nil {
		t.Fatal(err)
	}
	admin := adminSessionHeaders(t, router)

	detail := requestJSON(t, router, http.MethodGet, "/api/v1/admin/apps/"+app.ID, nil, admin)
	if detail.Code != http.StatusOK || strings.Contains(detail.Body.String(), "api_key") || strings.Contains(detail.Body.String(), "hmac_secret") {
		t.Fatalf("app detail status=%d body=%s", detail.Code, detail.Body.String())
	}

	created := requestJSON(t, router, http.MethodPost, "/api/v1/admin/apps/"+app.ID+"/functions", []byte(`{"name":"calculate","timeout_seconds":5}`), admin)
	if created.Code != http.StatusCreated || strings.Contains(created.Body.String(), "secret") {
		t.Fatalf("function create status=%d body=%s", created.Code, created.Body.String())
	}
	var function domain.Function
	if err := json.Unmarshal(created.Body.Bytes(), &function); err != nil {
		t.Fatal(err)
	}

	listed := requestJSON(t, router, http.MethodGet, "/api/v1/admin/apps/"+app.ID+"/functions", nil, admin)
	if listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), function.ID) {
		t.Fatalf("function list status=%d body=%s", listed.Code, listed.Body.String())
	}
	updated := requestJSON(t, router, http.MethodPatch, "/api/v1/admin/apps/"+app.ID+"/functions/"+function.ID, []byte(`{"timeout_seconds":10,"enabled":false}`), admin)
	if updated.Code != http.StatusOK || !strings.Contains(updated.Body.String(), `"timeout_seconds":10`) {
		t.Fatalf("function update status=%d body=%s", updated.Code, updated.Body.String())
	}

	if err := functions.Delete(context.Background(), app.ID, function.ID); err != nil {
		t.Fatal(err)
	}
	disabled, _, err := apps.Create(context.Background(), service.CreateApp{Name: "disabled", DeliveryMode: domain.DeliveryQueue})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := apps.Disable(context.Background(), disabled.ID); err != nil {
		t.Fatal(err)
	}
	disabledFunctions := requestJSON(t, router, http.MethodGet, "/api/v1/admin/apps/"+disabled.ID+"/functions", nil, admin)
	if disabledFunctions.Code != http.StatusNotFound {
		t.Fatalf("disabled app status=%d body=%s", disabledFunctions.Code, disabledFunctions.Body.String())
	}
	unknown := requestJSON(t, router, http.MethodGet, "/api/v1/admin/apps/app_missing/functions", nil, admin)
	if unknown.Code != http.StatusNotFound {
		t.Fatalf("unknown app status=%d body=%s", unknown.Code, unknown.Body.String())
	}
}

func TestAdminFunctionFacadeInvokePreservesIdempotencyAndBounds(t *testing.T) {
	router, apps, functions := newAdminFullControlRouter(t)
	app, _, err := apps.Create(context.Background(), service.CreateApp{Name: "orders", DeliveryMode: domain.DeliveryQueue})
	if err != nil {
		t.Fatal(err)
	}
	admin := adminSessionHeaders(t, router)
	created := requestJSON(t, router, http.MethodPost, "/api/v1/admin/apps/"+app.ID+"/functions", []byte(`{"name":"calculate","timeout_seconds":5}`), admin)
	var function domain.Function
	if created.Code != http.StatusCreated || json.Unmarshal(created.Body.Bytes(), &function) != nil {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}

	invoke := requestJSON(t, router, http.MethodPost, "/api/v1/admin/apps/"+app.ID+"/functions/"+function.ID+"/invoke", []byte(`{"input":{"amount":7}}`), withHeaders(admin, "Idempotency-Key", "admin-invoke-1"))
	if invoke.Code != http.StatusOK || !strings.Contains(invoke.Body.String(), `"ok":true`) {
		t.Fatalf("invoke status=%d body=%s", invoke.Code, invoke.Body.String())
	}
	replay := requestJSON(t, router, http.MethodPost, "/api/v1/admin/apps/"+app.ID+"/functions/"+function.ID+"/invoke", []byte(`{"input":{"amount":7}}`), withHeaders(admin, "Idempotency-Key", "admin-invoke-1"))
	if replay.Code != http.StatusOK || replay.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay status=%d headers=%v body=%s", replay.Code, replay.Header(), replay.Body.String())
	}
	tooLarge := requestJSON(t, router, http.MethodPost, "/api/v1/admin/apps/"+app.ID+"/functions/"+function.ID+"/invoke", []byte(`{"input":{"value":"`+strings.Repeat("x", 1<<20)+`"}}`), withHeaders(admin, "Idempotency-Key", "large"))
	if tooLarge.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("large invoke status=%d body=%s", tooLarge.Code, tooLarge.Body.String())
	}
	_ = functions
}

func newAdminFullControlRouter(t *testing.T) (http.Handler, *service.AppService, *service.FunctionService) {
	t.Helper()
	appRepository := newHTTPMemoryStore()
	now := func() time.Time { return time.Date(2026, 10, 31, 10, 0, 0, 0, time.UTC) }
	apps := service.NewAppService(appRepository, service.AppOptions{Now: now})
	functionRepository := newFunctionMemory()
	var functions *service.FunctionService
	functions = service.NewFunctionService(functionRepository, service.FunctionOptions{Notifier: adminFunctionNotifier(func(ctx context.Context, invocation domain.Invocation) error {
		if err := functions.ClaimInvocation(ctx, invocation.OwnerAppID, "admin-control", invocation.ID); err != nil {
			return err
		}
		if err := functions.AcknowledgeInvocation(ctx, invocation.OwnerAppID, "admin-control", invocation.ID); err != nil {
			return err
		}
		return functions.CompleteResult(ctx, invocation.OwnerAppID, "admin-control", domain.RPCResult{InvocationID: invocation.ID, OK: true, Result: json.RawMessage(`{"ok":true}`)})
	})})
	users := &adminUserMemoryStore{password: "admin-bootstrap"}
	return NewRouter(Dependencies{Admin: fstest.MapFS{}, AdminSessions: testAdminSessions(t), AdminUsers: users, Apps: apps, Functions: functions, Metrics: http.NotFoundHandler(), Now: now}), apps, functions
}

type adminFunctionNotifier func(context.Context, domain.Invocation) error

func (notifier adminFunctionNotifier) PublishInvocation(ctx context.Context, invocation domain.Invocation) error {
	return notifier(ctx, invocation)
}

func withHeaders(headers map[string]string, name, value string) map[string]string {
	copy := make(map[string]string, len(headers)+1)
	for key, item := range headers {
		copy[key] = item
	}
	copy[name] = value
	return copy
}

func (m *functionMemory) UpdateFunction(_ context.Context, owner string, function domain.Function) (domain.Function, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.functions[function.ID]
	if !ok || current.AppID != owner {
		return domain.Function{}, store.ErrNotFound
	}
	for id, old := range m.functions {
		if id != function.ID && old.AppID == owner && old.Name == function.Name {
			return domain.Function{}, store.ErrConflict
		}
	}
	m.functions[function.ID] = function
	return function, nil
}
