package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"time"

	"github.com/dungxbuif/RelayHub/internal/redisstate"
	"github.com/dungxbuif/RelayHub/internal/store"
)

const (
	adminSessionIdleTTL     = 30 * time.Minute
	adminSessionAbsoluteTTL = 12 * time.Hour
)

var ErrForbidden = errors.New("forbidden")

type AdminSessionService struct {
	store       redisstate.SessionStore
	users       store.AdminUserStore
	now         func() time.Time
	random      io.Reader
	idleTTL     time.Duration
	absoluteTTL time.Duration
}

type AdminPrincipal struct {
	ID    string `json:"id"`
	Email string `json:"email"`
	Role  string `json:"role"`
}

func NewAdminSessionService(sessionStore redisstate.SessionStore, users store.AdminUserStore, now func() time.Time, random io.Reader) (*AdminSessionService, error) {
	if sessionStore == nil || users == nil {
		return nil, ErrInvalidInput
	}
	if now == nil {
		now = time.Now
	}
	if random == nil {
		random = rand.Reader
	}
	return &AdminSessionService{
		store: sessionStore, users: users, now: now, random: random,
		idleTTL: adminSessionIdleTTL, absoluteTTL: adminSessionAbsoluteTTL,
	}, nil
}

func (s *AdminSessionService) Exchange(ctx context.Context, email, password string) (string, string, time.Time, AdminPrincipal, error) {
	user, err := s.users.AuthenticateAdminUser(ctx, email, password)
	if err != nil || !user.Enabled || user.Role != "admin" {
		return "", "", time.Time{}, AdminPrincipal{}, ErrUnauthorized
	}
	sessionValue, err := s.randomValue()
	if err != nil {
		return "", "", time.Time{}, AdminPrincipal{}, err
	}
	csrfToken, err := s.randomValue()
	if err != nil {
		return "", "", time.Time{}, AdminPrincipal{}, err
	}
	now := s.now().UTC()
	expiresAt := now.Add(s.absoluteTTL)
	csrfHash := sha256.Sum256([]byte(csrfToken))
	session := redisstate.AdminSession{
		ID: "ras_" + sessionValue, UserID: user.ID, Email: user.Email, CSRFHash: hex.EncodeToString(csrfHash[:]), IssuedAt: now, LastSeenAt: now,
		IdleExpiresAt: now.Add(s.idleTTL), ExpiresAt: expiresAt,
	}
	if err := s.store.Put(ctx, session); err != nil {
		return "", "", time.Time{}, AdminPrincipal{}, err
	}
	return session.ID, csrfToken, expiresAt, AdminPrincipal{ID: user.ID, Email: user.Email, Role: user.Role}, nil
}

func (s *AdminSessionService) Authenticate(ctx context.Context, sessionID, csrfToken string, mutate bool) (AdminPrincipal, error) {
	if sessionID == "" {
		return AdminPrincipal{}, ErrUnauthorized
	}
	session, err := s.store.ValidateAndTouch(ctx, sessionID, s.now().UTC(), s.idleTTL)
	if errors.Is(err, redisstate.ErrNotFound) || errors.Is(err, redisstate.ErrCorruptRecord) || errors.Is(err, redisstate.ErrInvalidKeyPart) {
		return AdminPrincipal{}, ErrUnauthorized
	}
	if err != nil {
		return AdminPrincipal{}, err
	}
	if !mutate {
		return AdminPrincipal{ID: session.UserID, Email: session.Email, Role: "admin"}, nil
	}
	got := sha256.Sum256([]byte(csrfToken))
	want, err := hex.DecodeString(session.CSRFHash)
	if err != nil || len(want) != sha256.Size || csrfToken == "" || subtle.ConstantTimeCompare(got[:], want) != 1 {
		return AdminPrincipal{}, ErrForbidden
	}
	return AdminPrincipal{ID: session.UserID, Email: session.Email, Role: "admin"}, nil
}

func (s *AdminSessionService) Refresh(ctx context.Context, sessionID string) (string, time.Time, AdminPrincipal, error) {
	principal, err := s.Authenticate(ctx, sessionID, "", false)
	if err != nil {
		return "", time.Time{}, AdminPrincipal{}, err
	}
	session, err := s.store.Get(ctx, sessionID)
	if err != nil {
		return "", time.Time{}, AdminPrincipal{}, err
	}
	csrfToken, err := s.randomValue()
	if err != nil {
		return "", time.Time{}, AdminPrincipal{}, err
	}
	csrfHash := sha256.Sum256([]byte(csrfToken))
	session.CSRFHash = hex.EncodeToString(csrfHash[:])
	session.LastSeenAt = s.now().UTC()
	if session.IdleExpiresAt.Before(session.LastSeenAt.Add(s.idleTTL)) {
		session.IdleExpiresAt = session.LastSeenAt.Add(s.idleTTL)
	}
	if session.ExpiresAt.Before(session.IdleExpiresAt) {
		session.IdleExpiresAt = session.ExpiresAt
	}
	if err := s.store.Put(ctx, session); err != nil {
		return "", time.Time{}, AdminPrincipal{}, err
	}
	return csrfToken, session.ExpiresAt, principal, nil
}

func (s *AdminSessionService) Logout(ctx context.Context, sessionID string) error {
	if sessionID == "" {
		return ErrUnauthorized
	}
	if err := s.store.Delete(ctx, sessionID); err != nil && !errors.Is(err, redisstate.ErrNotFound) {
		return err
	}
	return nil
}

func (s *AdminSessionService) randomValue() (string, error) {
	value := make([]byte, 32)
	if _, err := io.ReadFull(s.random, value); err != nil {
		return "", errors.New("generate Admin session value")
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}
