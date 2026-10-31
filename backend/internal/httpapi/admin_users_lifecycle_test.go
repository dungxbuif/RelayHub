package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"testing/fstest"
	"time"

	"github.com/dungxbuif/RelayHub/internal/redisstate"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/dungxbuif/RelayHub/internal/store"
)

func TestAdminUserLifecycleKeepsUsersReadOnly(t *testing.T) {
	users := newLifecycleAdminUserStore()
	sessions := &lifecycleSessionStore{}
	now := func() time.Time { return time.Date(2026, 10, 31, 10, 0, 0, 0, time.UTC) }
	adminSessions, err := service.NewAdminSessionService(sessions, users, now, bytes.NewReader(append(bytes.Repeat([]byte{0x32}, 64), bytes.Repeat([]byte{0x33}, 4096)...)))
	if err != nil {
		t.Fatal(err)
	}
	router := NewRouter(Dependencies{Admin: fstest.MapFS{}, AdminSessions: adminSessions, AdminUsers: users, Metrics: http.NotFoundHandler(), Now: now})

	admin := adminSessionHeadersFor(t, router, `{"email":"admin@example.com","password":"admin-pass"}`)
	list := requestJSON(t, router, http.MethodGet, "/api/v1/admin/users", nil, admin)
	if list.Code != http.StatusOK {
		t.Fatalf("admin list status=%d body=%s", list.Code, list.Body.String())
	}
	created := requestJSON(t, router, http.MethodPost, "/api/v1/admin/users", []byte(`{"email":"new@example.com","password":"new-pass","role":"user"}`), admin)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}

	user := adminSessionHeadersFor(t, router, `{"email":"user@example.com","password":"user-pass"}`)
	read := requestJSON(t, router, http.MethodGet, "/api/v1/admin/users", nil, user)
	if read.Code != http.StatusOK {
		t.Fatalf("user read status=%d body=%s", read.Code, read.Body.String())
	}
	mutation := requestJSON(t, router, http.MethodPatch, "/api/v1/admin/users/adm_user", []byte(`{"enabled":false}`), user)
	assertStatusAndJSON(t, mutation, http.StatusForbidden, `{"error":{"code":"forbidden","message":"The request is not allowed."}}`)
}

func TestAdminUserLifecycleProtectsSelfAndRevokesRotatedSessions(t *testing.T) {
	users := newLifecycleAdminUserStore()
	sessions := &lifecycleSessionStore{}
	now := func() time.Time { return time.Date(2026, 10, 31, 10, 0, 0, 0, time.UTC) }
	adminSessions, err := service.NewAdminSessionService(sessions, users, now, bytes.NewReader(append(bytes.Repeat([]byte{0x32}, 64), bytes.Repeat([]byte{0x33}, 4096)...)))
	if err != nil {
		t.Fatal(err)
	}
	router := NewRouter(Dependencies{Admin: fstest.MapFS{}, AdminSessions: adminSessions, AdminUsers: users, Metrics: http.NotFoundHandler(), Now: now})

	admin := adminSessionHeadersFor(t, router, `{"email":"admin@example.com","password":"admin-pass"}`)
	selfDisable := requestJSON(t, router, http.MethodDelete, "/api/v1/admin/users/adm_admin", nil, admin)
	assertStatusAndJSON(t, selfDisable, http.StatusConflict, `{"error":{"code":"conflict","message":"The request conflicts with an existing resource."}}`)

	user := adminSessionHeadersFor(t, router, `{"email":"user@example.com","password":"user-pass"}`)
	rotate := requestJSON(t, router, http.MethodPost, "/api/v1/admin/users/adm_user/password", []byte(`{"password":"new-user-pass"}`), admin)
	if rotate.Code != http.StatusOK {
		t.Fatalf("rotate status=%d body=%s", rotate.Code, rotate.Body.String())
	}
	oldSession := requestJSON(t, router, http.MethodGet, "/api/v1/admin/users", nil, user)
	assertStatusAndJSON(t, oldSession, http.StatusUnauthorized, `{"error":{"code":"unauthorized","message":"Authentication failed."}}`)
}

func adminSessionHeadersFor(t *testing.T, router http.Handler, body string) map[string]string {
	t.Helper()
	response := requestJSON(t, router, http.MethodPost, "/api/v1/admin/session", []byte(body), nil)
	if response.Code != http.StatusOK || len(response.Result().Cookies()) != 1 {
		t.Fatalf("admin fixture login: status=%d body=%s", response.Code, response.Body.String())
	}
	var payload adminSessionResponse
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	cookie := response.Result().Cookies()[0]
	return map[string]string{"Cookie": cookie.Name + "=" + cookie.Value, "X-RelayHub-CSRF": payload.CSRFToken}
}

type lifecycleAdminUserStore struct {
	users    map[string]store.AdminUser
	password map[string]string
}

var _ store.AdminUserLifecycleStore = (*lifecycleAdminUserStore)(nil)

func newLifecycleAdminUserStore() *lifecycleAdminUserStore {
	return &lifecycleAdminUserStore{
		users: map[string]store.AdminUser{
			"adm_admin": {ID: "adm_admin", Email: "admin@example.com", Role: "admin", Enabled: true},
			"adm_user":  {ID: "adm_user", Email: "user@example.com", Role: "user", Enabled: true},
		},
		password: map[string]string{"adm_admin": "admin-pass", "adm_user": "user-pass"},
	}
}

func (s *lifecycleAdminUserStore) AuthenticateAdminUser(_ context.Context, email, password string) (store.AdminUser, error) {
	for id, user := range s.users {
		if user.Enabled && user.Email == email && s.password[id] == password {
			return user, nil
		}
	}
	return store.AdminUser{}, store.ErrNotFound
}

func (s *lifecycleAdminUserStore) CreateAdminUser(_ context.Context, user store.AdminUser, password string) error {
	s.users[user.ID] = user
	s.password[user.ID] = password
	return nil
}

func (s *lifecycleAdminUserStore) EnsureSeedAdminUser(context.Context, string, string, time.Time, string) error {
	return nil
}

func (s *lifecycleAdminUserStore) ListAdminUsers(context.Context) ([]store.AdminUser, error) {
	result := make([]store.AdminUser, 0, len(s.users))
	for _, user := range s.users {
		result = append(result, user)
	}
	return result, nil
}

func (s *lifecycleAdminUserStore) GetAdminUser(_ context.Context, id string) (store.AdminUser, error) {
	user, ok := s.users[id]
	if !ok {
		return store.AdminUser{}, store.ErrNotFound
	}
	return user, nil
}

func (s *lifecycleAdminUserStore) UpdateAdminUser(_ context.Context, user store.AdminUser) (store.AdminUser, error) {
	if _, ok := s.users[user.ID]; !ok {
		return store.AdminUser{}, store.ErrNotFound
	}
	s.users[user.ID] = user
	return user, nil
}

func (s *lifecycleAdminUserStore) DisableAdminUser(_ context.Context, id string, now time.Time) (store.AdminUser, error) {
	user, ok := s.users[id]
	if !ok {
		return store.AdminUser{}, store.ErrNotFound
	}
	user.Enabled = false
	user.UpdatedAt = now
	s.users[id] = user
	return user, nil
}

func (s *lifecycleAdminUserStore) RotateAdminUserPassword(_ context.Context, id, password string, now time.Time) (store.AdminUser, error) {
	user, ok := s.users[id]
	if !ok {
		return store.AdminUser{}, store.ErrNotFound
	}
	s.password[id] = password
	user.UpdatedAt = now
	s.users[id] = user
	return user, nil
}

type lifecycleSessionStore struct {
	sessions map[string]redisstate.AdminSession
	revoked  map[string]bool
}

func (s *lifecycleSessionStore) Put(_ context.Context, session redisstate.AdminSession) error {
	if s.revoked == nil {
		s.revoked = map[string]bool{}
	}
	if s.sessions == nil {
		s.sessions = map[string]redisstate.AdminSession{}
	}
	s.sessions[session.ID] = session
	return nil
}

func (s *lifecycleSessionStore) Get(_ context.Context, id string) (redisstate.AdminSession, error) {
	session, ok := s.sessions[id]
	if !ok {
		return redisstate.AdminSession{}, redisstate.ErrNotFound
	}
	return session, nil
}

func (s *lifecycleSessionStore) ValidateAndTouch(_ context.Context, id string, now time.Time, idle time.Duration) (redisstate.AdminSession, error) {
	session, ok := s.sessions[id]
	if !ok || s.revoked[session.UserID] {
		return redisstate.AdminSession{}, redisstate.ErrNotFound
	}
	session.LastSeenAt = now
	session.IdleExpiresAt = now.Add(idle)
	s.sessions[id] = session
	return session, nil
}

func (s *lifecycleSessionStore) Delete(context.Context, string) error { return nil }

func (s *lifecycleSessionStore) RevokeAdminUserSessions(_ context.Context, userID string) error {
	if s.revoked == nil {
		s.revoked = map[string]bool{}
	}
	s.revoked[userID] = true
	return nil
}
