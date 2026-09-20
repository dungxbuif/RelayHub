package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/dungxbuif/RelayHub/internal/adminread"
	"github.com/dungxbuif/RelayHub/internal/store"
)

type AdminLifecycleOptions struct {
	Now     func() time.Time
	Timeout time.Duration
}

type AdminLifecycleService struct {
	repository store.AdminLifecycleStore
	now        func() time.Time
	timeout    time.Duration
}

func NewAdminLifecycleService(repository store.AdminLifecycleStore, options AdminLifecycleOptions) (*AdminLifecycleService, error) {
	if nilDependency(repository) {
		return nil, ErrInvalidDependency
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Timeout <= 0 {
		options.Timeout = 3 * time.Second
	}
	if options.Timeout > 30*time.Second {
		return nil, ErrInvalidInput
	}
	return &AdminLifecycleService{repository: repository, now: options.Now, timeout: options.Timeout}, nil
}

func (service *AdminLifecycleService) Timeline(parent context.Context, eventID string) (adminread.EventTimeline, error) {
	if service == nil || service.repository == nil || eventID == "" || len(eventID) > 256 || strings.TrimSpace(eventID) != eventID {
		return adminread.EventTimeline{}, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(parent, service.timeout)
	defer cancel()
	result, err := service.repository.GetAdminEventTimeline(ctx, eventID)
	return result, adminLifecycleError(err)
}

func (service *AdminLifecycleService) Replay(parent context.Context, deliveryIDs []string, idempotencyKey, actorID string) (adminread.ReplayResult, bool, error) {
	if service == nil || service.repository == nil || idempotencyKey == "" || len(idempotencyKey) > 256 || strings.TrimSpace(idempotencyKey) != idempotencyKey || actorID == "" || len(actorID) > 256 || strings.TrimSpace(actorID) != actorID {
		return adminread.ReplayResult{}, false, ErrInvalidInput
	}
	canonicalIDs := append([]string(nil), deliveryIDs...)
	sort.Strings(canonicalIDs)
	selection, err := json.Marshal(canonicalIDs)
	if err != nil {
		return adminread.ReplayResult{}, false, ErrInvalidInput
	}
	keyHash := sha256.Sum256([]byte(idempotencyKey))
	fingerprint := sha256.Sum256(append([]byte("relayhub:admin-replay:v1:"), selection...))
	command := adminread.ReplayCommand{
		DeliveryIDs: canonicalIDs, IdempotencyKeyHash: hex.EncodeToString(keyHash[:]),
		RequestFingerprint: hex.EncodeToString(fingerprint[:]), ActorID: actorID, Now: service.now().UTC(),
	}
	if err := command.Validate(); err != nil {
		return adminread.ReplayResult{}, false, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(parent, service.timeout)
	defer cancel()
	result, replayed, err := service.repository.ReplayAdminDeadLetters(ctx, command)
	return result, replayed, adminLifecycleError(err)
}

func adminLifecycleError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, adminread.ErrInvalidArgument), errors.Is(err, adminread.ErrInvalidCursor):
		return ErrInvalidInput
	case errors.Is(err, store.ErrNotFound):
		return ErrNotFound
	case errors.Is(err, store.ErrConflict):
		return ErrConflict
	default:
		return err
	}
}
