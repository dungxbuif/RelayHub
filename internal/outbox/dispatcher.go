package outbox

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/dungxbuif/RelayHub/internal/broker"
	"github.com/dungxbuif/RelayHub/internal/store"
)

var (
	ErrStoreUnavailable   = errors.New("outbox store unavailable")
	ErrPublishUnavailable = errors.New("outbox broker publish unavailable")
	ErrOutboxLag          = errors.New("outbox pending lag exceeds readiness limit")
	ErrOutboxTerminal     = errors.New("outbox contains terminal failed rows")
)

type Options struct {
	Now           func() time.Time
	NewToken      func() (string, error)
	BatchSize     int
	ClaimTTL      time.Duration
	BaseRetry     time.Duration
	MaxRetry      time.Duration
	MaxAttempts   int64
	MaxPendingAge time.Duration
}

type Dispatcher struct {
	store     store.OutboxStore
	publisher broker.Publisher
	options   Options
}

func NewDispatcher(repository store.OutboxStore, publisher broker.Publisher, options Options) (*Dispatcher, error) {
	if repository == nil || publisher == nil || options.BatchSize < 1 || options.BatchSize > 1000 || options.ClaimTTL <= 0 || options.BaseRetry <= 0 || options.MaxRetry < options.BaseRetry || options.MaxAttempts < 1 || options.MaxPendingAge <= 0 {
		return nil, errors.New("invalid outbox dispatcher configuration")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.NewToken == nil {
		options.NewToken = randomToken
	}
	return &Dispatcher{store: repository, publisher: publisher, options: options}, nil
}

func (dispatcher *Dispatcher) RunOnce(ctx context.Context) (int, error) {
	now := dispatcher.options.Now().UTC()
	token, err := dispatcher.options.NewToken()
	if err != nil || token == "" {
		recordOutcome("store_error")
		return 0, ErrStoreUnavailable
	}
	messages, err := dispatcher.store.ClaimOutbox(ctx, now, now.Add(-dispatcher.options.ClaimTTL), token, dispatcher.options.BatchSize)
	if err != nil {
		recordOutcome("store_error")
		return 0, ErrStoreUnavailable
	}
	completed := 0
	var publishFailed bool
	for _, message := range messages {
		if message.Reclaimed {
			recordOutcome("reclaimed")
		}
		started, beginErr := dispatcher.store.BeginOutboxPublish(ctx, message.ID, message.ClaimToken, now, dispatcher.options.MaxAttempts)
		if beginErr != nil {
			recordOutcome("store_error")
			return completed, ErrStoreUnavailable
		}
		if started.Exhausted {
			recordOutcome("terminal")
			publishFailed = true
			continue
		}
		ack, publishErr := dispatcher.publisher.Publish(ctx, broker.Publication{Subject: message.Subject, Data: message.Payload, MessageID: message.MessageID})
		if publishErr != nil {
			var persistenceErr error
			if started.Attempt >= dispatcher.options.MaxAttempts {
				persistenceErr = dispatcher.store.FailOutbox(ctx, message.ID, message.ClaimToken, now, "broker_unavailable")
				recordOutcome("terminal")
			} else {
				delay := dispatcher.retryDelay(started.Attempt)
				persistenceErr = dispatcher.store.RetryOutbox(ctx, message.ID, message.ClaimToken, now.Add(delay), "broker_unavailable")
			}
			if persistenceErr != nil {
				recordOutcome("store_error")
				return completed, ErrStoreUnavailable
			}
			recordOutcome("publish_error")
			publishFailed = true
			continue
		}
		if err := dispatcher.store.MarkOutboxDispatched(ctx, message.ID, message.ClaimToken, now); err != nil {
			recordOutcome("store_error")
			return completed, ErrStoreUnavailable
		}
		if ack.Duplicate {
			recordOutcome("duplicate")
		} else {
			recordOutcome("published")
		}
		completed++
	}
	if publishFailed {
		return completed, ErrPublishUnavailable
	}
	return completed, nil
}

func (dispatcher *Dispatcher) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return errors.New("outbox interval must be positive")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if _, err := dispatcher.RunOnce(ctx); err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (dispatcher *Dispatcher) Ping(ctx context.Context) error {
	stats, err := dispatcher.store.OutboxStats(ctx)
	if err != nil {
		return ErrStoreUnavailable
	}
	now := dispatcher.options.Now().UTC()
	recordStats(stats, now)
	if stats.Failed > 0 {
		return ErrOutboxTerminal
	}
	if stats.OldestPendingAt != nil && now.Sub(*stats.OldestPendingAt) > dispatcher.options.MaxPendingAge {
		return ErrOutboxLag
	}
	return nil
}

func (dispatcher *Dispatcher) retryDelay(attempt int64) time.Duration {
	delay := dispatcher.options.BaseRetry
	for step := int64(1); step < attempt && delay < dispatcher.options.MaxRetry; step++ {
		if delay > dispatcher.options.MaxRetry/2 {
			return dispatcher.options.MaxRetry
		}
		delay *= 2
	}
	if delay > dispatcher.options.MaxRetry {
		return dispatcher.options.MaxRetry
	}
	return delay
}

func randomToken() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}
