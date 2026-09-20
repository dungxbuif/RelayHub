package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/redisstate"
	"github.com/dungxbuif/RelayHub/internal/store"
)

func TestAdminSessionExchangeAuthenticateAndLogout(t *testing.T) {
	now := time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC)
	store := &adminSessionMemoryStore{}
	random := bytes.NewReader(bytes.Repeat([]byte{0x42}, 64))
	users := adminSessionUserStore{password: "bootstrap-secret"}
	sessions, err := NewAdminSessionService(store, users, func() time.Time { return now }, random)
	if err != nil {
		t.Fatal(err)
	}

	sessionID, csrf, expiresAt, user, err := sessions.Exchange(context.Background(), "admin@example.com", "bootstrap-secret")
	if err != nil {
		t.Fatal(err)
	}
	if user.Email != "admin@example.com" || user.Role != "admin" {
		t.Fatalf("user=%+v", user)
	}
	if sessionID == "" || csrf == "" || sessionID == csrf {
		t.Fatalf("unsafe generated values: session=%q csrf=%q", sessionID, csrf)
	}
	if expiresAt != now.Add(12*time.Hour) || store.session.CSRFHash == csrf {
		t.Fatalf("stored session=%+v expires=%v", store.session, expiresAt)
	}
	if _, err := sessions.Authenticate(context.Background(), sessionID, "", false); err != nil {
		t.Fatalf("safe Authenticate() error = %v", err)
	}
	if _, err := sessions.Authenticate(context.Background(), sessionID, csrf, true); err != nil {
		t.Fatalf("mutating Authenticate() error = %v", err)
	}
	if _, err := sessions.Authenticate(context.Background(), sessionID, "wrong", true); !errors.Is(err, ErrForbidden) {
		t.Fatalf("wrong CSRF error = %v, want ErrForbidden", err)
	}
	if err := sessions.Logout(context.Background(), sessionID); err != nil || !store.deleted {
		t.Fatalf("Logout() error=%v deleted=%v", err, store.deleted)
	}
}

func TestAdminSessionRejectsCredentialsAndRandomFailure(t *testing.T) {
	now := func() time.Time { return time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC) }
	users := adminSessionUserStore{password: "bootstrap-secret"}
	sessions, err := NewAdminSessionService(&adminSessionMemoryStore{}, users, now, bytes.NewReader(make([]byte, 64)))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := sessions.Exchange(context.Background(), "admin@example.com", "wrong"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("wrong bootstrap error = %v", err)
	}

	sessions, err = NewAdminSessionService(&adminSessionMemoryStore{}, users, now, errorReader{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := sessions.Exchange(context.Background(), "admin@example.com", "bootstrap-secret"); err == nil || errors.Is(err, ErrUnauthorized) {
		t.Fatalf("random failure error = %v", err)
	}
	if _, err := NewAdminSessionService(nil, nil, nil, nil); err == nil {
		t.Fatal("invalid constructor succeeded")
	}
}

func TestAdminSessionEnforcesIdleAndAbsoluteExpiry(t *testing.T) {
	now := time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC)
	store := &adminSessionMemoryStore{}
	users := adminSessionUserStore{password: "bootstrap-secret"}
	sessions, err := NewAdminSessionService(store, users, func() time.Time { return now }, bytes.NewReader(bytes.Repeat([]byte{0x22}, 128)))
	if err != nil {
		t.Fatal(err)
	}
	sessionID, _, _, _, err := sessions.Exchange(context.Background(), "admin@example.com", "bootstrap-secret")
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(30*time.Minute + time.Millisecond)
	if _, err := sessions.Authenticate(context.Background(), sessionID, "", false); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("idle-expired Authenticate() error = %v", err)
	}

	store = &adminSessionMemoryStore{}
	now = time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC)
	sessions, err = NewAdminSessionService(store, users, func() time.Time { return now }, bytes.NewReader(bytes.Repeat([]byte{0x23}, 128)))
	if err != nil {
		t.Fatal(err)
	}
	sessionID, _, _, _, err = sessions.Exchange(context.Background(), "admin@example.com", "bootstrap-secret")
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(12*time.Hour + time.Millisecond)
	if _, err := sessions.Authenticate(context.Background(), sessionID, "", false); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("absolute-expired Authenticate() error = %v", err)
	}
}

type adminSessionMemoryStore struct {
	session redisstate.AdminSession
	deleted bool
}

func (s *adminSessionMemoryStore) Put(_ context.Context, session redisstate.AdminSession) error {
	s.session = session
	return nil
}

func (s *adminSessionMemoryStore) Get(context.Context, string) (redisstate.AdminSession, error) {
	if s.deleted || s.session.ID == "" {
		return redisstate.AdminSession{}, redisstate.ErrNotFound
	}
	return s.session, nil
}

func (s *adminSessionMemoryStore) ValidateAndTouch(_ context.Context, id string, now time.Time, idle time.Duration) (redisstate.AdminSession, error) {
	if s.deleted || s.session.ID != id || !now.Before(s.session.ExpiresAt) || !now.Before(s.session.IdleExpiresAt) {
		return redisstate.AdminSession{}, redisstate.ErrNotFound
	}
	s.session.LastSeenAt = now
	s.session.IdleExpiresAt = now.Add(idle)
	if s.session.IdleExpiresAt.After(s.session.ExpiresAt) {
		s.session.IdleExpiresAt = s.session.ExpiresAt
	}
	return s.session, nil
}

func (s *adminSessionMemoryStore) Delete(context.Context, string) error {
	s.deleted = true
	return nil
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

type adminSessionUserStore struct{ password string }

func (s adminSessionUserStore) AuthenticateAdminUser(_ context.Context, email, password string) (store.AdminUser, error) {
	if email == "admin@example.com" && password == s.password {
		return store.AdminUser{ID: "adm_1", Email: email, Role: "admin", Enabled: true}, nil
	}
	return store.AdminUser{}, store.ErrNotFound
}

func (s adminSessionUserStore) CreateAdminUser(context.Context, store.AdminUser, string) error {
	return nil
}

func (s adminSessionUserStore) EnsureSeedAdminUser(context.Context, string, string, time.Time, string) error {
	return nil
}
