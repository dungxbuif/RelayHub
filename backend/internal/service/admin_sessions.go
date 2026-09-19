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
)

const (
	adminSessionIdleTTL     = 30 * time.Minute
	adminSessionAbsoluteTTL = 12 * time.Hour
)

var ErrForbidden = errors.New("forbidden")

type AdminSessionService struct {
	store         redisstate.SessionStore
	bootstrapHash [32]byte
	now           func() time.Time
	random        io.Reader
	idleTTL       time.Duration
	absoluteTTL   time.Duration
}

func NewAdminSessionService(store redisstate.SessionStore, bootstrapToken string, now func() time.Time, random io.Reader) (*AdminSessionService, error) {
	if store == nil || bootstrapToken == "" {
		return nil, ErrInvalidInput
	}
	if now == nil {
		now = time.Now
	}
	if random == nil {
		random = rand.Reader
	}
	return &AdminSessionService{
		store: store, bootstrapHash: sha256.Sum256([]byte(bootstrapToken)), now: now, random: random,
		idleTTL: adminSessionIdleTTL, absoluteTTL: adminSessionAbsoluteTTL,
	}, nil
}

func (s *AdminSessionService) Exchange(ctx context.Context, bootstrapToken string) (string, string, time.Time, error) {
	got := sha256.Sum256([]byte(bootstrapToken))
	if bootstrapToken == "" || subtle.ConstantTimeCompare(got[:], s.bootstrapHash[:]) != 1 {
		return "", "", time.Time{}, ErrUnauthorized
	}
	sessionValue, err := s.randomValue()
	if err != nil {
		return "", "", time.Time{}, err
	}
	csrfToken, err := s.randomValue()
	if err != nil {
		return "", "", time.Time{}, err
	}
	now := s.now().UTC()
	expiresAt := now.Add(s.absoluteTTL)
	csrfHash := sha256.Sum256([]byte(csrfToken))
	session := redisstate.AdminSession{
		ID: "ras_" + sessionValue, CSRFHash: hex.EncodeToString(csrfHash[:]), IssuedAt: now, LastSeenAt: now,
		IdleExpiresAt: now.Add(s.idleTTL), ExpiresAt: expiresAt,
	}
	if err := s.store.Put(ctx, session); err != nil {
		return "", "", time.Time{}, err
	}
	return session.ID, csrfToken, expiresAt, nil
}

func (s *AdminSessionService) Authenticate(ctx context.Context, sessionID, csrfToken string, mutate bool) error {
	if sessionID == "" {
		return ErrUnauthorized
	}
	session, err := s.store.ValidateAndTouch(ctx, sessionID, s.now().UTC(), s.idleTTL)
	if errors.Is(err, redisstate.ErrNotFound) || errors.Is(err, redisstate.ErrCorruptRecord) || errors.Is(err, redisstate.ErrInvalidKeyPart) {
		return ErrUnauthorized
	}
	if err != nil {
		return err
	}
	if !mutate {
		return nil
	}
	got := sha256.Sum256([]byte(csrfToken))
	want, err := hex.DecodeString(session.CSRFHash)
	if err != nil || len(want) != sha256.Size || csrfToken == "" || subtle.ConstantTimeCompare(got[:], want) != 1 {
		return ErrForbidden
	}
	return nil
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
