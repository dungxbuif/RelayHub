package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/dungxbuif/RelayHub/internal/adminread"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/dungxbuif/RelayHub/internal/store"
)

type httpAdminLifecycleStore struct {
	command  adminread.ReplayCommand
	replayed bool
	err      error
}

func (*httpAdminLifecycleStore) GetAdminEventTimeline(_ context.Context, eventID string) (adminread.EventTimeline, error) {
	return adminread.EventTimeline{
		Event: adminread.EventDetail{EventSummary: adminread.EventSummary{ID: eventID}, Data: json.RawMessage(`{"safe":true}`)},
		Items: []adminread.TimelineItem{{ID: "event:" + eventID, Type: "event.created", EventID: eventID}},
	}, nil
}

func (stub *httpAdminLifecycleStore) ReplayAdminDeadLetters(_ context.Context, command adminread.ReplayCommand) (adminread.ReplayResult, bool, error) {
	stub.command = command
	if stub.err != nil {
		return adminread.ReplayResult{}, false, stub.err
	}
	items := make([]adminread.ReplayItem, len(command.DeliveryIDs))
	for index, id := range command.DeliveryIDs {
		items[index] = adminread.ReplayItem{DeliveryID: id, Generation: 2, Status: "pending"}
	}
	return adminread.ReplayResult{Items: items}, stub.replayed, nil
}

func TestAdminLifecycleRoutesRequireAdminAndExposeTimelineAndReplay(t *testing.T) {
	router, repository := adminLifecycleTestRouter(t, nil)
	unauthorized := httptest.NewRecorder()
	router.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/v1/admin/events/evt_1/timeline", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d", unauthorized.Code)
	}
	timeline := adminLifecycleRequest(t, router, http.MethodGet, "/api/v1/admin/events/evt_1/timeline", nil, "")
	if timeline.Code != http.StatusOK || !strings.Contains(timeline.Body.String(), `"type":"event.created"`) {
		t.Fatalf("timeline status=%d body=%s", timeline.Code, timeline.Body.String())
	}
	single := adminLifecycleRequest(t, router, http.MethodPost, "/api/v1/admin/dlq/dlv_1/replay", nil, "single-key")
	if single.Code != http.StatusOK || repository.command.ActorID != "admin_user:adm_1" || strings.Contains(single.Body.String(), "single-key") {
		t.Fatalf("single status=%d command=%#v body=%s", single.Code, repository.command, single.Body.String())
	}
	batch := adminLifecycleRequest(t, router, http.MethodPost, "/api/v1/admin/dlq/replay", strings.NewReader(`{"delivery_ids":["dlv_2","dlv_1"]}`), "batch-key")
	if batch.Code != http.StatusOK || strings.Join(repository.command.DeliveryIDs, ",") != "dlv_1,dlv_2" {
		t.Fatalf("batch status=%d command=%#v body=%s", batch.Code, repository.command, batch.Body.String())
	}
	repository.replayed = true
	duplicate := adminLifecycleRequest(t, router, http.MethodPost, "/api/v1/admin/dlq/dlv_1/replay", nil, "single-key")
	if duplicate.Code != http.StatusOK || duplicate.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatalf("duplicate status=%d headers=%v", duplicate.Code, duplicate.Header())
	}
}

func TestAdminLifecycleReplayValidatesInputAndRedactsErrors(t *testing.T) {
	router, repository := adminLifecycleTestRouter(t, nil)
	cases := []struct {
		path, key, body string
	}{
		{"/api/v1/admin/dlq/dlv_1/replay", "", ""},
		{"/api/v1/admin/dlq/replay", "key", `{}`},
		{"/api/v1/admin/dlq/replay", "key", `{"delivery_ids":["same","same"]}`},
		{"/api/v1/admin/dlq/replay", "key", `{"delivery_ids":["dlv"],"unknown":"value"}`},
	}
	for _, candidate := range cases {
		response := adminLifecycleRequest(t, router, http.MethodPost, candidate.path, strings.NewReader(candidate.body), candidate.key)
		if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), `"code":"invalid_request"`) {
			t.Fatalf("POST %s status=%d body=%s", candidate.path, response.Code, response.Body.String())
		}
	}
	repository.err = store.ErrConflict
	response := adminLifecycleRequest(t, router, http.MethodPost, "/api/v1/admin/dlq/dlv_SENTINEL/replay", nil, "conflict-key")
	if response.Code != http.StatusConflict || strings.Contains(response.Body.String(), "SENTINEL") || strings.Contains(response.Body.String(), "dead_letter") {
		t.Fatalf("conflict response status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestAdminLifecycleCookieReplayRequiresCSRF(t *testing.T) {
	now := time.Date(2026, 9, 20, 9, 0, 0, 0, time.UTC)
	sessions, err := service.NewAdminSessionService(&httpSessionStore{}, &adminUserMemoryStore{password: "admin-bootstrap"}, func() time.Time { return now }, bytes.NewReader(bytes.Repeat([]byte{0x44}, 4096)))
	if err != nil {
		t.Fatal(err)
	}
	router, _ := adminLifecycleTestRouter(t, sessions)
	login := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/admin/session", strings.NewReader(`{"email":"dungbui.dungbui.00@gmail.com","password":"admin-bootstrap"}`))
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(login, request)
	if login.Code != http.StatusOK || len(login.Result().Cookies()) != 1 {
		t.Fatalf("login status=%d body=%s", login.Code, login.Body.String())
	}
	var sessionBody adminSessionResponse
	if err := json.Unmarshal(login.Body.Bytes(), &sessionBody); err != nil {
		t.Fatal(err)
	}
	replay := func(csrf string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/admin/dlq/dlv_1/replay", nil)
		request.AddCookie(login.Result().Cookies()[0])
		request.Header.Set("Idempotency-Key", "cookie-key")
		request.Header.Set("X-RelayHub-CSRF", csrf)
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		return response
	}
	if response := replay(""); response.Code != http.StatusForbidden {
		t.Fatalf("missing CSRF status=%d body=%s", response.Code, response.Body.String())
	}
	if response := replay(sessionBody.CSRFToken); response.Code != http.StatusOK {
		t.Fatalf("valid CSRF status=%d body=%s", response.Code, response.Body.String())
	}
}

func adminLifecycleTestRouter(t *testing.T, sessions *service.AdminSessionService) (http.Handler, *httpAdminLifecycleStore) {
	t.Helper()
	repository := &httpAdminLifecycleStore{}
	lifecycle, err := service.NewAdminLifecycleService(repository, service.AdminLifecycleOptions{Now: func() time.Time { return time.Date(2026, 9, 20, 8, 0, 0, 0, time.UTC) }})
	if err != nil {
		t.Fatal(err)
	}
	adminFS := fs.FS(fstest.MapFS{"index.html": {Data: []byte("admin")}})
	if sessions == nil {
		sessions = testAdminSessions(t)
	}
	return NewRouter(Dependencies{AdminToken: "admin-bootstrap", AdminSessions: sessions, AdminLifecycle: lifecycle, Admin: adminFS, Metrics: http.NotFoundHandler()}), repository
}

func adminLifecycleRequest(t *testing.T, router http.Handler, method, path string, body *strings.Reader, key string) *httptest.ResponseRecorder {
	var request *http.Request
	if body == nil {
		request = httptest.NewRequest(method, path, nil)
	} else {
		request = httptest.NewRequest(method, path, body)
	}
	for key, value := range adminSessionHeaders(t, router) {
		request.Header.Set(key, value)
	}
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	return response
}
