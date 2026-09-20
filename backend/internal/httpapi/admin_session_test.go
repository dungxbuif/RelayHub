package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/dungxbuif/RelayHub/internal/redisstate"
	"github.com/dungxbuif/RelayHub/internal/service"
)

func TestAdminSessionCookieCSRFAndLogout(t *testing.T) {
	router, sessions := newAdminSessionRouter(t)
	login := requestJSON(t, router, http.MethodPost, "/api/v1/admin/session", []byte(`{"email":"dungbui.dungbui.00@gmail.com","password":"admin-bootstrap"}`), nil)
	if login.Code != http.StatusOK {
		t.Fatalf("login status=%d body=%s", login.Code, login.Body.String())
	}
	var loginBody struct {
		CSRFToken string    `json:"csrf_token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := json.Unmarshal(login.Body.Bytes(), &loginBody); err != nil || loginBody.CSRFToken == "" || loginBody.ExpiresAt.IsZero() {
		t.Fatalf("login body=%s error=%v", login.Body.String(), err)
	}
	cookies := login.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("cookies=%v", cookies)
	}
	cookie := cookies[0]
	if cookie.Name != AdminCookieName || !cookie.HttpOnly || !cookie.Secure || cookie.SameSite != http.SameSiteStrictMode || cookie.Path != "/" || cookie.Domain != "" {
		t.Fatalf("unsafe cookie=%+v", cookie)
	}

	createWithoutCSRF := httptest.NewRequest(http.MethodPost, "/api/v1/apps", strings.NewReader(`{"name":"web","delivery_mode":"websocket"}`))
	createWithoutCSRF.Header.Set("Content-Type", "application/json")
	createWithoutCSRF.AddCookie(cookie)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, createWithoutCSRF)
	assertStatusAndJSON(t, response, http.StatusForbidden, `{"error":{"code":"forbidden","message":"The request is not allowed."}}`)

	create := httptest.NewRequest(http.MethodPost, "/api/v1/apps", strings.NewReader(`{"name":"web","delivery_mode":"websocket"}`))
	create.Header.Set("Content-Type", "application/json")
	create.Header.Set("X-RelayHub-CSRF", loginBody.CSRFToken)
	create.AddCookie(cookie)
	response = httptest.NewRecorder()
	router.ServeHTTP(response, create)
	if response.Code != http.StatusCreated {
		t.Fatalf("cookie Admin create status=%d body=%s", response.Code, response.Body.String())
	}

	list := httptest.NewRequest(http.MethodGet, "/api/v1/apps", nil)
	list.AddCookie(cookie)
	response = httptest.NewRecorder()
	router.ServeHTTP(response, list)
	if response.Code != http.StatusOK {
		t.Fatalf("safe cookie request status=%d body=%s", response.Code, response.Body.String())
	}

	logout := httptest.NewRequest(http.MethodDelete, "/api/v1/admin/session", nil)
	logout.Header.Set("X-RelayHub-CSRF", loginBody.CSRFToken)
	logout.AddCookie(cookie)
	response = httptest.NewRecorder()
	router.ServeHTTP(response, logout)
	if response.Code != http.StatusNoContent || !sessions.deleted {
		t.Fatalf("logout status=%d deleted=%v body=%s", response.Code, sessions.deleted, response.Body.String())
	}
	cleared := response.Result().Cookies()
	if len(cleared) != 1 || cleared[0].Name != AdminCookieName || cleared[0].MaxAge >= 0 {
		t.Fatalf("cleared cookie=%+v", cleared)
	}
}

func TestAdminSessionRejectsInvalidInputsAndBearer(t *testing.T) {
	router, _ := newAdminSessionRouter(t)
	for name, body := range map[string][]byte{
		"missing":        nil,
		"wrong password": []byte(`{"email":"dungbui.dungbui.00@gmail.com","password":"wrong"}`),
		"unknown email":  []byte(`{"email":"nobody@example.com","password":"admin-bootstrap"}`),
	} {
		t.Run(name, func(t *testing.T) {
			response := requestJSON(t, router, http.MethodPost, "/api/v1/admin/session", body, nil)
			assertStatusAndJSON(t, response, http.StatusUnauthorized, `{"error":{"code":"unauthorized","message":"Authentication failed."}}`)
		})
	}

	bearer := requestJSON(t, router, http.MethodPost, "/api/v1/apps", []byte(`{"name":"automation","delivery_mode":"websocket"}`), map[string]string{"Authorization": "Bearer admin-bootstrap"})
	assertStatusAndJSON(t, bearer, http.StatusUnauthorized, `{"error":{"code":"unauthorized","message":"Authentication failed."}}`)

	malformed := httptest.NewRequest(http.MethodGet, "/api/v1/apps", nil)
	malformed.Header.Set("Cookie", AdminCookieName+"=bad value")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, malformed)
	assertStatusAndJSON(t, response, http.StatusUnauthorized, `{"error":{"code":"unauthorized","message":"Authentication failed."}}`)
}

func TestAdminSessionSecretsAreNotLogged(t *testing.T) {
	var logs bytes.Buffer
	router, _ := newAdminSessionRouterWithLogger(t, slog.New(slog.NewJSONHandler(&logs, nil)))
	response := requestJSON(t, router, http.MethodPost, "/api/v1/admin/session", []byte(`{"email":"dungbui.dungbui.00@gmail.com","password":"admin-bootstrap"}`), nil)
	if response.Code != http.StatusOK {
		t.Fatalf("login status=%d body=%s", response.Code, response.Body.String())
	}
	var payload struct {
		CSRFToken string `json:"csrf_token"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"admin-bootstrap", payload.CSRFToken, response.Result().Cookies()[0].Value} {
		if secret != "" && strings.Contains(logs.String(), secret) {
			t.Fatalf("secret leaked to request log: %q", secret)
		}
	}
}

func newAdminSessionRouter(t *testing.T) (http.Handler, *httpSessionStore) {
	return newAdminSessionRouterWithLogger(t, nil)
}

func newAdminSessionRouterWithLogger(t *testing.T, logger *slog.Logger) (http.Handler, *httpSessionStore) {
	t.Helper()
	store := &httpSessionStore{}
	now := func() time.Time { return time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC) }
	users := &adminUserMemoryStore{password: "admin-bootstrap"}
	sessions, err := service.NewAdminSessionService(store, users, now, bytes.NewReader(bytes.Repeat([]byte{0x31}, 4096)))
	if err != nil {
		t.Fatal(err)
	}
	apps := service.NewAppService(newHTTPMemoryStore(), service.AppOptions{Now: now, Random: bytes.NewReader(bytes.Repeat([]byte{0x41}, 4096))})
	return NewRouter(Dependencies{
		Logger: logger, Apps: apps, AdminSessions: sessions, AdminUsers: users, Now: now,
		Admin: fstest.MapFS{}, Metrics: http.NotFoundHandler(),
	}), store
}

type httpSessionStore struct {
	session redisstate.AdminSession
	deleted bool
}

func (s *httpSessionStore) Put(_ context.Context, session redisstate.AdminSession) error {
	s.session = session
	return nil
}

func (s *httpSessionStore) Get(context.Context, string) (redisstate.AdminSession, error) {
	return s.session, nil
}

func (s *httpSessionStore) ValidateAndTouch(_ context.Context, id string, now time.Time, idle time.Duration) (redisstate.AdminSession, error) {
	if s.deleted || id == "" || id != s.session.ID {
		return redisstate.AdminSession{}, redisstate.ErrNotFound
	}
	s.session.LastSeenAt = now
	s.session.IdleExpiresAt = now.Add(idle)
	return s.session, nil
}

func (s *httpSessionStore) Delete(context.Context, string) error {
	s.deleted = true
	return nil
}
