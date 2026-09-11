package service

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/observability"
	"github.com/dungxbuif/RelayHub/internal/store"
)

type PublishEvent struct {
	Type         string          `json:"type"`
	TargetAppIDs []string        `json:"target_app_ids"`
	Data         json.RawMessage `json:"data"`
}
type LeasedEvent = store.LeasedEvent

// Notifier delivers hints after durable state succeeds. Errors cannot undo state.
type Notifier interface {
	PublishEvent(context.Context, domain.Event) error
	PublishJob(context.Context, domain.Job) error
}
type EventOptions struct {
	Notifier          Notifier
	NotificationError func(error)
	Now               func() time.Time
	NewID             func(string) (string, error)
	Retention         store.EventRetention
	LeaseDuration     time.Duration
}
type EventService struct {
	repository store.EventStore
	apps       store.ApplicationReader
	options    EventOptions
}

func NewEventService(repository store.EventStore, apps store.ApplicationReader, options EventOptions) *EventService {
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.NewID == nil {
		options.NewID = func(prefix string) (string, error) {
			var raw [16]byte
			if _, err := rand.Read(raw[:]); err != nil {
				return "", err
			}
			return prefix + hex.EncodeToString(raw[:]), nil
		}
	}
	if options.Retention.Event <= 0 {
		options.Retention.Event = 7 * 24 * time.Hour
	}
	if options.Retention.Job <= 0 {
		options.Retention.Job = 7 * 24 * time.Hour
	}
	if options.Retention.Idempotency <= 0 {
		options.Retention.Idempotency = 24 * time.Hour
	}
	if options.LeaseDuration <= 0 {
		options.LeaseDuration = 60 * time.Second
	}
	return &EventService{repository: repository, apps: apps, options: options}
}
func (s *EventService) Publish(ctx context.Context, source string, input PublishEvent, key string) (domain.Event, []domain.Job, bool, error) {
	event, jobs, replay, err := s.publish(ctx, source, input, key)
	switch {
	case errors.Is(err, ErrInvalidInput):
		observability.EventOutcome("rejected")
	case err != nil:
		observability.EventOutcome("store_error")
	case replay:
		observability.EventOutcome("replayed")
	default:
		observability.EventOutcome("published")
	}
	return event, jobs, replay, err
}

func (s *EventService) publish(ctx context.Context, source string, input PublishEvent, key string) (domain.Event, []domain.Job, bool, error) {
	input.Type = strings.TrimSpace(input.Type)
	raw := bytes.TrimSpace(input.Data)
	if source == "" || strings.TrimSpace(key) == "" || input.Type == "" || len(input.TargetAppIDs) < 1 || len(input.TargetAppIDs) > 100 || !domain.JSONObject(raw) {
		return domain.Event{}, nil, false, ErrInvalidInput
	}
	targets := append([]string(nil), input.TargetAppIDs...)
	seen := map[string]bool{}
	for i, id := range targets {
		id = strings.TrimSpace(id)
		if id == "" || seen[id] {
			return domain.Event{}, nil, false, ErrInvalidInput
		}
		seen[id] = true
		targets[i] = id
	}
	previous, err := s.repository.FindPublication(ctx, source, key)
	if err == nil {
		return previous.Event, previous.Jobs, true, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return domain.Event{}, nil, false, mapStoreError(err)
	}
	callbackTargets := map[string]bool{}
	for _, id := range targets {
		app, err := s.apps.GetApplication(ctx, id)
		if errors.Is(err, store.ErrNotFound) || (err == nil && !app.Enabled) {
			return domain.Event{}, nil, false, ErrInvalidInput
		}
		if err != nil {
			return domain.Event{}, nil, false, mapStoreError(err)
		}
		callbackTargets[id] = app.CallbackURL != nil && strings.TrimSpace(*app.CallbackURL) != "" && (app.DeliveryMode == domain.DeliveryCallback || app.DeliveryMode == domain.DeliveryAll)
	}
	sort.Strings(targets)
	id, err := s.options.NewID("evt_")
	if err != nil {
		return domain.Event{}, nil, false, err
	}
	now := s.options.Now().UTC()
	e := domain.Event{ID: id, Type: input.Type, SourceAppID: source, TargetAppIDs: targets, Data: append(json.RawMessage(nil), raw...), CreatedAt: now}
	jobs := make([]domain.Job, 0, len(targets))
	for _, target := range targets {
		id, err := s.options.NewID("job_")
		if err != nil {
			return domain.Event{}, nil, false, err
		}
		jobs = append(jobs, domain.Job{Callback: callbackTargets[target], ID: id, EventID: e.ID, SourceAppID: source, TargetAppID: target, Status: domain.JobPending, CreatedAt: now, UpdatedAt: now})
	}
	p, replay, err := s.repository.PublishEvent(ctx, store.Publication{Event: e, Jobs: jobs}, key, s.options.Retention)
	if errors.Is(err, store.ErrInvalidTarget) {
		err = ErrInvalidInput
	}
	if err == nil && !replay && s.options.Notifier != nil {
		s.notificationError(s.options.Notifier.PublishEvent(ctx, p.Event))
		for _, j := range p.Jobs {
			s.notifyJob(ctx, j)
		}
	}
	return p.Event, p.Jobs, replay, mapStoreError(err)
}
func (s *EventService) Lease(ctx context.Context, target string, limit int, wait time.Duration) ([]LeasedEvent, error) {
	if target == "" || limit < 1 || limit > 100 || wait < 0 || wait > 30*time.Second {
		return nil, ErrInvalidInput
	}
	// Use a real deadline even with an injected, fixed business clock.
	pollCtx := ctx
	cancel := func() {}
	if wait > 0 {
		pollCtx, cancel = context.WithTimeout(ctx, wait)
	}
	defer cancel()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// go-redis applies ctx.Deadline to the socket but cannot interrupt an
		// already-started read when ctx is explicitly cancelled. Short attempt
		// deadlines bound that delay without leaving a detached Redis call.
		attemptCtx := pollCtx
		attemptCancel := func() {}
		if wait > 0 {
			attemptCtx, attemptCancel = context.WithTimeout(pollCtx, 250*time.Millisecond)
		}
		items, err := s.repository.LeaseJobs(attemptCtx, target, limit, s.options.Now().UTC(), s.options.LeaseDuration)
		attemptErr := attemptCtx.Err()
		attemptCancel()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil {
			if wait > 0 && pollCtx.Err() != nil {
				return []LeasedEvent{}, nil
			}
			// A bounded attempt can time out while the overall poll still has
			// time left. Retry at the normal polling cadence until its deadline.
			if wait == 0 || !errors.Is(attemptErr, context.DeadlineExceeded) {
				return nil, mapStoreError(err)
			}
		}
		if err == nil && len(items) > 0 {
			for _, item := range items {
				s.notifyJob(ctx, item.Job)
			}
			return items, nil
		}
		if wait == 0 {
			return []LeasedEvent{}, nil
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-pollCtx.Done():
			timer.Stop()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return []LeasedEvent{}, nil
		case <-timer.C:
		}
	}
}
func (s *EventService) Ack(ctx context.Context, target, event string) error {
	err := s.repository.AckEvent(ctx, target, event, s.options.Now().UTC(), s.options.Retention.Job)
	if err == nil && s.options.Notifier != nil {
		if reader, ok := s.repository.(store.EventJobReader); ok {
			j, readErr := reader.GetEventJob(ctx, target, event)
			if readErr != nil {
				s.notificationError(readErr)
			} else {
				s.notifyJob(ctx, j)
			}
		} else {
			s.notificationError(errors.New("event store does not support acknowledgement notifications"))
		}
	}
	return mapStoreError(err)
}
func (s *EventService) GetEvent(ctx context.Context, actor, id string) (domain.Event, error) {
	e, err := s.repository.GetEvent(ctx, id)
	if err != nil {
		return domain.Event{}, mapStoreError(err)
	}
	if !e.CanRead(actor) {
		return domain.Event{}, ErrNotFound
	}
	return e, nil
}
func (s *EventService) GetJob(ctx context.Context, actor, id string) (domain.Job, error) {
	j, err := s.repository.GetJob(ctx, id)
	if err != nil {
		return domain.Job{}, mapStoreError(err)
	}
	if actor != j.SourceAppID && actor != j.TargetAppID {
		return domain.Job{}, ErrNotFound
	}
	return j, nil
}
func (s *EventService) RequeueJob(ctx context.Context, id string) (domain.Job, error) {
	j, err := s.repository.TransitionJob(ctx, id, domain.JobPending, s.options.Now().UTC(), s.options.Retention.Job)
	if err == nil {
		s.notifyJob(ctx, j)
	}
	return j, mapStoreError(err)
}
func (s *EventService) DeadLetterJob(ctx context.Context, id string) (domain.Job, error) {
	j, err := s.repository.TransitionJob(ctx, id, domain.JobDeadLetter, s.options.Now().UTC(), s.options.Retention.Job)
	if err == nil {
		s.notifyJob(ctx, j)
	}
	return j, mapStoreError(err)
}

func (s *EventService) notifyJob(ctx context.Context, j domain.Job) {
	if s.options.Notifier != nil {
		s.notificationError(s.options.Notifier.PublishJob(ctx, j))
	}
}
func (s *EventService) notificationError(err error) {
	if err != nil && s.options.NotificationError != nil {
		s.options.NotificationError(err)
	}
}
