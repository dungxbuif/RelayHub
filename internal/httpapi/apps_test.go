package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/dungxbuif/RelayHub/internal/auth"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/dungxbuif/RelayHub/internal/store"
)

func TestAdminCreateAndListRequireBearerAndRedactCredentials(t *testing.T) {
	router, _, _ := newAppTestRouter(t)
	body := []byte(`{"name":"orders","delivery_mode":"queue"}`)

	unauthorized := requestJSON(t, router, http.MethodPost, "/api/v1/apps", body, nil)
	assertStatusAndJSON(t, unauthorized, http.StatusUnauthorized, `{"error":{"code":"unauthorized","message":"Authentication failed."}}`)

	created := requestJSON(t, router, http.MethodPost, "/api/v1/apps", body, map[string]string{"Authorization": "Bearer admin-test-token"})
	if created.Code != http.StatusCreated {
		t.Fatalf("POST /apps status = %d, want 201; body = %s", created.Code, created.Body.String())
	}
	var credentials service.AppCredentials
	if err := json.Unmarshal(created.Body.Bytes(), &credentials); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if credentials.AppID == "" || credentials.APIKey == "" || credentials.HMACSecret == "" {
		t.Fatalf("create response = %#v, want one-time credentials", credentials)
	}

	listed := requestJSON(t, router, http.MethodGet, "/api/v1/apps", nil, map[string]string{"Authorization": "Bearer admin-test-token"})
	if listed.Code != http.StatusOK {
		t.Fatalf("GET /apps status = %d, want 200; body = %s", listed.Code, listed.Body.String())
	}
	if strings.Contains(listed.Body.String(), credentials.APIKey) || strings.Contains(listed.Body.String(), credentials.HMACSecret) || strings.Contains(listed.Body.String(), "api_key") || strings.Contains(listed.Body.String(), "hmac_secret") {
		t.Fatalf("list response exposed credentials: %s", listed.Body.String())
	}
	var apps []domain.App
	if err := json.Unmarshal(listed.Body.Bytes(), &apps); err != nil || len(apps) != 1 || apps[0].ID != credentials.AppID {
		t.Fatalf("list response = %s, error = %v", listed.Body.String(), err)
	}
}

func TestSignedAppCanReadAndUpdateOnlyItself(t *testing.T) {
	router, appService, _ := newAppTestRouter(t)
	app, credentials, err := appService.Create(context.Background(), service.CreateApp{Name: "orders", DeliveryMode: domain.DeliveryQueue})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	read := signedRequest(t, router, credentials, http.MethodGet, "/api/v1/apps/"+app.ID, nil)
	if read.Code != http.StatusOK {
		t.Fatalf("signed GET status = %d, want 200; body = %s", read.Code, read.Body.String())
	}
	if strings.Contains(read.Body.String(), credentials.APIKey) || strings.Contains(read.Body.String(), credentials.HMACSecret) {
		t.Fatalf("signed GET exposed credentials: %s", read.Body.String())
	}

	name := []byte(`{"name":"orders-v2"}`)
	updated := signedRequest(t, router, credentials, http.MethodPatch, "/api/v1/apps/"+app.ID, name)
	if updated.Code != http.StatusOK || !strings.Contains(updated.Body.String(), `"name":"orders-v2"`) {
		t.Fatalf("signed PATCH status = %d body = %s, want updated app", updated.Code, updated.Body.String())
	}

	forbidden := signedRequest(t, router, credentials, http.MethodGet, "/api/v1/apps/app_someone_else", nil)
	assertStatusAndJSON(t, forbidden, http.StatusForbidden, `{"error":{"code":"forbidden","message":"The authenticated application cannot access this resource."}}`)
}

func TestSignedRoutesRejectMalformedAndExpiredAuthentication(t *testing.T) {
	router, appService, _ := newAppTestRouter(t)
	app, credentials, err := appService.Create(context.Background(), service.CreateApp{Name: "orders", DeliveryMode: domain.DeliveryQueue})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	target := "/api/v1/apps/" + app.ID
	validSignature := auth.Sign([]byte(credentials.HMACSecret), "1789120800", http.MethodGet, target, nil)

	tests := []struct {
		name    string
		headers map[string]string
	}{
		{name: "missing headers"},
		{name: "unknown API key", headers: map[string]string{"X-RelayHub-Api-Key": "unknown", "X-RelayHub-Timestamp": "1789120800", "X-RelayHub-Signature": validSignature}},
		{name: "malformed timestamp", headers: map[string]string{"X-RelayHub-Api-Key": credentials.APIKey, "X-RelayHub-Timestamp": "today", "X-RelayHub-Signature": validSignature}},
		{name: "expired timestamp", headers: map[string]string{"X-RelayHub-Api-Key": credentials.APIKey, "X-RelayHub-Timestamp": "1789120499", "X-RelayHub-Signature": auth.Sign([]byte(credentials.HMACSecret), "1789120499", http.MethodGet, target, nil)}},
		{name: "bad signature", headers: map[string]string{"X-RelayHub-Api-Key": credentials.APIKey, "X-RelayHub-Timestamp": "1789120800", "X-RelayHub-Signature": strings.Repeat("0", 64)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := requestJSON(t, router, http.MethodGet, target, nil, tt.headers)
			assertStatusAndJSON(t, response, http.StatusUnauthorized, `{"error":{"code":"unauthorized","message":"Authentication failed."}}`)
			if strings.Contains(response.Body.String(), credentials.APIKey) || strings.Contains(response.Body.String(), credentials.HMACSecret) || strings.Contains(response.Body.String(), validSignature) {
				t.Fatalf("authentication error exposed credential material: %s", response.Body.String())
			}
		})
	}
}

func TestAdminRotateAndDisableInvalidateCredentials(t *testing.T) {
	router, appService, _ := newAppTestRouter(t)
	app, oldCredentials, err := appService.Create(context.Background(), service.CreateApp{Name: "orders", DeliveryMode: domain.DeliveryQueue})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	admin := map[string]string{"Authorization": "Bearer admin-test-token"}

	rotated := requestJSON(t, router, http.MethodPost, "/api/v1/apps/"+app.ID+"/rotate-secret", nil, admin)
	if rotated.Code != http.StatusOK {
		t.Fatalf("rotate status = %d, want 200; body = %s", rotated.Code, rotated.Body.String())
	}
	var newCredentials service.AppCredentials
	if err := json.Unmarshal(rotated.Body.Bytes(), &newCredentials); err != nil {
		t.Fatalf("decode rotate response: %v", err)
	}
	assertStatusAndJSON(t, signedRequest(t, router, oldCredentials, http.MethodGet, "/api/v1/apps/"+app.ID, nil), http.StatusUnauthorized, `{"error":{"code":"unauthorized","message":"Authentication failed."}}`)
	if response := signedRequest(t, router, newCredentials, http.MethodGet, "/api/v1/apps/"+app.ID, nil); response.Code != http.StatusOK {
		t.Fatalf("new credentials GET status = %d, want 200; body = %s", response.Code, response.Body.String())
	}

	disabled := requestJSON(t, router, http.MethodDelete, "/api/v1/apps/"+app.ID, nil, admin)
	if disabled.Code != http.StatusOK || !strings.Contains(disabled.Body.String(), `"enabled":false`) {
		t.Fatalf("disable status = %d body = %s, want disabled app", disabled.Code, disabled.Body.String())
	}
	assertStatusAndJSON(t, signedRequest(t, router, newCredentials, http.MethodGet, "/api/v1/apps/"+app.ID, nil), http.StatusUnauthorized, `{"error":{"code":"unauthorized","message":"Authentication failed."}}`)
}

func TestAppValidationUsesStandardErrorEnvelope(t *testing.T) {
	router, _, _ := newAppTestRouter(t)
	response := requestJSON(t, router, http.MethodPost, "/api/v1/apps", []byte(`{"name":"orders","delivery_mode":"callback","callback_url":"http://example.com/events"}`), map[string]string{"Authorization": "Bearer admin-test-token"})
	assertStatusAndJSON(t, response, http.StatusBadRequest, `{"error":{"code":"invalid_request","message":"The request is invalid."}}`)
}

func TestSocketTokenIssuanceIsSignedScopedAndBounded(t *testing.T) {
	router, appService, issuer := newAppTestRouter(t)
	app, credentials, err := appService.Create(context.Background(), service.CreateApp{Name: "orders", DeliveryMode: domain.DeliveryQueue})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	body := []byte(`{"scopes":["ws:connect","ws:subscribe"],"ttl_seconds":900}`)

	response := signedRequest(t, router, credentials, http.MethodPost, "/api/v1/socket/token", body)
	if response.Code != http.StatusCreated {
		t.Fatalf("socket token status = %d, want 201; body = %s", response.Code, response.Body.String())
	}
	var payload struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode socket token response: %v", err)
	}
	claims, err := issuer.Verify(payload.Token, "ws:subscribe")
	if err != nil || claims.AppID != app.ID {
		t.Fatalf("Verify(socket token) claims = %#v error = %v, want app-scoped token", claims, err)
	}
	if !payload.ExpiresAt.Equal(time.Date(2026, 9, 11, 10, 15, 0, 0, time.UTC)) {
		t.Fatalf("expires_at = %v, want 15-minute expiry", payload.ExpiresAt)
	}

	invalid := signedRequest(t, router, credentials, http.MethodPost, "/api/v1/socket/token", []byte(`{"scopes":["admin:all"],"ttl_seconds":901}`))
	assertStatusAndJSON(t, invalid, http.StatusBadRequest, `{"error":{"code":"invalid_request","message":"The request is invalid."}}`)
}

func newAppTestRouter(t *testing.T) (http.Handler, *service.AppService, *auth.TokenIssuer) {
	t.Helper()
	repository := newHTTPMemoryStore()
	random := make([]byte, 4096)
	for index := range random {
		random[index] = byte(index%251 + 1)
	}
	now := func() time.Time { return time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC) }
	appService := service.NewAppService(repository, service.AppOptions{Now: now, Random: bytes.NewReader(random)})
	issuer := auth.NewTokenIssuer([]byte("server-socket-signing-secret"), now)
	router := NewRouter(Dependencies{
		Health:      repository,
		Docs:        fstest.MapFS{"index.html": {Data: []byte("docs")}},
		Metrics:     http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
		Apps:        appService,
		AdminToken:  "admin-test-token",
		TokenIssuer: issuer,
		Now:         now,
		SigningSkew: 300 * time.Second,
	})
	return router, appService, issuer
}

func signedRequest(t *testing.T, handler http.Handler, credentials service.AppCredentials, method, target string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	timestamp := "1789120800"
	return requestJSON(t, handler, method, target, body, map[string]string{
		"X-RelayHub-Api-Key":   credentials.APIKey,
		"X-RelayHub-Timestamp": timestamp,
		"X-RelayHub-Signature": auth.Sign([]byte(credentials.HMACSecret), timestamp, method, target, body),
	})
}

func requestJSON(t *testing.T, handler http.Handler, method, target string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, target, bytes.NewReader(body))
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func assertStatusAndJSON(t *testing.T, response *httptest.ResponseRecorder, status int, body string) {
	t.Helper()
	if response.Code != status {
		t.Fatalf("status = %d, want %d; body = %s", response.Code, status, response.Body.String())
	}
	assertJSONResponse(t, response, body)
}

type httpMemoryStore struct {
	mu          sync.Mutex
	apps        map[string]domain.App
	credentials map[string]store.AppCredential
	indexes     map[string]string
}

func newHTTPMemoryStore() *httpMemoryStore {
	return &httpMemoryStore{apps: make(map[string]domain.App), credentials: make(map[string]store.AppCredential), indexes: make(map[string]string)}
}

func (*httpMemoryStore) Ping(context.Context) error { return nil }

func (memory *httpMemoryStore) CreateApplication(_ context.Context, app domain.App, credential store.AppCredential) error {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	if _, exists := memory.apps[app.ID]; exists {
		return store.ErrConflict
	}
	memory.apps[app.ID] = app
	memory.credentials[app.ID] = credential
	memory.indexes[credential.APIKeyHash] = app.ID
	return nil
}

func (memory *httpMemoryStore) ListApplications(context.Context) ([]domain.App, error) {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	result := make([]domain.App, 0, len(memory.apps))
	for _, app := range memory.apps {
		result = append(result, app)
	}
	return result, nil
}

func (memory *httpMemoryStore) GetApplication(_ context.Context, appID string) (domain.App, error) {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	app, ok := memory.apps[appID]
	if !ok {
		return domain.App{}, store.ErrNotFound
	}
	return app, nil
}

func (memory *httpMemoryStore) UpdateApplication(_ context.Context, app domain.App) (domain.App, error) {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	current, ok := memory.apps[app.ID]
	if !ok {
		return domain.App{}, store.ErrNotFound
	}
	current.Name = app.Name
	current.CallbackURL = app.CallbackURL
	current.DeliveryMode = app.DeliveryMode
	current.UpdatedAt = app.UpdatedAt
	memory.apps[app.ID] = current
	return current, nil
}

func (memory *httpMemoryStore) DisableApplication(_ context.Context, appID string, updatedAt time.Time) (domain.App, error) {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	app, ok := memory.apps[appID]
	if !ok {
		return domain.App{}, store.ErrNotFound
	}
	app.Enabled = false
	app.UpdatedAt = updatedAt
	memory.apps[appID] = app
	return app, nil
}

func (memory *httpMemoryStore) FindCredentialByAPIKeyHash(_ context.Context, hash string) (store.AppCredential, error) {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	appID, ok := memory.indexes[hash]
	if !ok {
		return store.AppCredential{}, store.ErrNotFound
	}
	return memory.credentials[appID], nil
}

func (memory *httpMemoryStore) RotateApplicationCredential(_ context.Context, appID string, credential store.AppCredential, updatedAt time.Time) error {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	app, ok := memory.apps[appID]
	if !ok {
		return store.ErrNotFound
	}
	old := memory.credentials[appID]
	delete(memory.indexes, old.APIKeyHash)
	memory.credentials[appID] = credential
	memory.indexes[credential.APIKeyHash] = appID
	app.UpdatedAt = updatedAt
	memory.apps[appID] = app
	return nil
}

var _ store.Store = (*httpMemoryStore)(nil)
