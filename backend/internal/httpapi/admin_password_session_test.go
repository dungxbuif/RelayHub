package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/dungxbuif/RelayHub/internal/redisstate"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/dungxbuif/RelayHub/internal/store"
)

func TestAdminPasswordLoginRefreshAndBearerRemoval(t *testing.T) {
	router, _ := newPasswordAdminRouter(t)
	login := requestJSON(t, router, http.MethodPost, "/api/v1/admin/session", []byte(`{"email":"dungbui.dungbui.00@gmail.com","password":"correct horse battery staple"}`), nil)
	if login.Code != http.StatusOK {
		t.Fatalf("login status=%d body=%s", login.Code, login.Body.String())
	}
	var body adminSessionResponse
	if err := json.Unmarshal(login.Body.Bytes(), &body); err != nil || body.CSRFToken == "" || body.User.Email != "dungbui.dungbui.00@gmail.com" {
		t.Fatalf("login body=%s err=%v", login.Body.String(), err)
	}
	cookie := login.Result().Cookies()[0]

	refresh := httptest.NewRequest(http.MethodGet, "/api/v1/admin/session", nil)
	refresh.AddCookie(cookie)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, refresh)
	if response.Code != http.StatusOK {
		t.Fatalf("refresh status=%d body=%s", response.Code, response.Body.String())
	}
	var refreshed adminSessionResponse
	if err := json.Unmarshal(response.Body.Bytes(), &refreshed); err != nil || refreshed.CSRFToken == "" || refreshed.User.Email != body.User.Email {
		t.Fatalf("refresh body=%s err=%v", response.Body.String(), err)
	}

	bearer := requestJSON(t, router, http.MethodPost, "/api/v1/apps", []byte(`{"name":"automation","delivery_mode":"websocket"}`), map[string]string{"Authorization": "Bearer old-bootstrap"})
	assertStatusAndJSON(t, bearer, http.StatusUnauthorized, `{"error":{"code":"unauthorized","message":"Authentication failed."}}`)
}

func TestAdminUsersRequireAdminSession(t *testing.T) {
	router, _ := newPasswordAdminRouter(t)
	create := requestJSON(t, router, http.MethodPost, "/api/v1/admin/users", []byte(`{"email":"other@example.com","password":"another strong password","role":"admin"}`), nil)
	assertStatusAndJSON(t, create, http.StatusUnauthorized, `{"error":{"code":"unauthorized","message":"Authentication failed."}}`)
}

func newPasswordAdminRouter(t *testing.T) (http.Handler, *httpSessionStore) {
	t.Helper()
	sessions := &httpSessionStore{}
	users := &adminUserMemoryStore{password: "correct horse battery staple"}
	now := func() time.Time { return time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC) }
	adminSessions, err := service.NewAdminSessionService(sessions, users, now, bytes.NewReader(bytes.Repeat([]byte{0x31}, 4096)))
	if err != nil {
		t.Fatal(err)
	}
	apps := service.NewAppService(newHTTPMemoryStore(), service.AppOptions{Now: now, Random: bytes.NewReader(bytes.Repeat([]byte{0x41}, 4096))})
	return NewRouter(Dependencies{Apps: apps, AdminSessions: adminSessions, Now: now, Admin: fstest.MapFS{}, Metrics: http.NotFoundHandler()}), sessions
}

type adminUserMemoryStore struct{ password string }

func (s *adminUserMemoryStore) AuthenticateAdminUser(_ context.Context, email, password string) (store.AdminUser, error) {
	if strings.EqualFold(strings.TrimSpace(email), "dungbui.dungbui.00@gmail.com") && password == s.password {
		return store.AdminUser{ID: "adm_1", Email: "dungbui.dungbui.00@gmail.com", Role: "admin", Enabled: true}, nil
	}
	return store.AdminUser{}, store.ErrNotFound
}

func (s *adminUserMemoryStore) CreateAdminUser(context.Context, store.AdminUser, string) error { return nil }
func (s *adminUserMemoryStore) EnsureSeedAdminUser(context.Context, string, string, time.Time, string) error { return nil }

var _ = redisstate.AdminSession{}
