package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/broker"
	"github.com/dungxbuif/RelayHub/internal/store"
)

func TestDispatcherPublishesAndCompletesWithDeterministicIdentity(t *testing.T) {
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	repository := &memoryOutbox{messages: []store.OutboxMessage{{ID: "obx_1", DeliveryID: "dlv_1", Subject: "rh.v1.delivery.safe", Payload: []byte(`{"event":{}}`), MessageID: "rh-v1-fixed", CreatedAt: now}}}
	publisher := &recordingPublisher{}
	dispatcher, err := NewDispatcher(repository, publisher, Options{Now: func() time.Time { return now }, NewToken: func() (string, error) { return "claim-1", nil }, BatchSize: 10, ClaimTTL: time.Minute, BaseRetry: time.Second, MaxRetry: time.Minute, MaxAttempts: 10, MaxPendingAge: 5 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	count, err := dispatcher.RunOnce(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("RunOnce() count=%d error=%v", count, err)
	}
	if len(publisher.publications) != 1 || publisher.publications[0].MessageID != "rh-v1-fixed" || publisher.publications[0].Subject != "rh.v1.delivery.safe" || string(publisher.publications[0].Data) != `{"event":{}}` {
		t.Fatalf("publications=%#v", publisher.publications)
	}
	if repository.dispatched["obx_1"] != "claim-1" {
		t.Fatalf("dispatch completion=%v", repository.dispatched)
	}
}

func TestDispatcherCrashAfterPublishReusesMessageIDAfterClaimExpiry(t *testing.T) {
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	repository := &memoryOutbox{messages: []store.OutboxMessage{{ID: "obx_1", DeliveryID: "dlv_1", Subject: "rh.v1.delivery.safe", Payload: []byte(`{}`), MessageID: "rh-v1-fixed", CreatedAt: now}}, failCompletionOnce: true}
	publisher := &recordingPublisher{}
	token := 0
	dispatcher, err := NewDispatcher(repository, publisher, Options{Now: func() time.Time { return now }, NewToken: func() (string, error) { token++; return "claim-" + string(rune('0'+token)), nil }, BatchSize: 1, ClaimTTL: time.Minute, BaseRetry: time.Second, MaxRetry: time.Minute, MaxAttempts: 10, MaxPendingAge: 5 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.RunOnce(context.Background()); !errors.Is(err, ErrStoreUnavailable) {
		t.Fatalf("first RunOnce() error=%v", err)
	}
	now = now.Add(2 * time.Minute)
	if _, err := dispatcher.RunOnce(context.Background()); err != nil {
		t.Fatalf("recovery RunOnce() error=%v", err)
	}
	if len(publisher.publications) != 2 || publisher.publications[0].MessageID != publisher.publications[1].MessageID {
		t.Fatalf("publish identities=%#v", publisher.publications)
	}
}

func TestDispatcherPublishFailureSchedulesBoundedRetry(t *testing.T) {
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	repository := &memoryOutbox{messages: []store.OutboxMessage{
		{ID: "obx_1", DeliveryID: "dlv_1", Subject: "rh.v1.delivery.safe", MessageID: "rh-v1-fixed-1", Attempts: 20, CreatedAt: now},
		{ID: "obx_2", DeliveryID: "dlv_2", Subject: "rh.v1.delivery.safe", MessageID: "rh-v1-fixed-2", Attempts: 2, CreatedAt: now},
	}}
	publisher := &recordingPublisher{err: errors.New("secret payload and subject must not reach logs")}
	dispatcher, err := NewDispatcher(repository, publisher, Options{Now: func() time.Time { return now }, NewToken: func() (string, error) { return "claim", nil }, BatchSize: 2, ClaimTTL: time.Minute, BaseRetry: time.Second, MaxRetry: 30 * time.Second, MaxAttempts: 100, MaxPendingAge: 5 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.RunOnce(context.Background()); !errors.Is(err, ErrPublishUnavailable) || err.Error() != ErrPublishUnavailable.Error() {
		t.Fatalf("RunOnce() error=%q", err)
	}
	retry := repository.retried["obx_1"]
	if retry.token != "claim" || !retry.at.Equal(now.Add(30*time.Second)) || retry.reason != "broker_unavailable" {
		t.Fatalf("retry=%#v", retry)
	}
	if second := repository.retried["obx_2"]; !second.at.Equal(now.Add(4 * time.Second)) {
		t.Fatalf("second claimed row was not released with retry: %#v", second)
	}
}

func TestDispatcherExhaustedPublishBecomesTerminal(t *testing.T) {
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	repository := &memoryOutbox{messages: []store.OutboxMessage{{ID: "obx_terminal", Subject: "rh.v1.delivery.safe", MessageID: "fixed", Attempts: 2}}}
	dispatcher, err := NewDispatcher(repository, &recordingPublisher{err: errors.New("offline")}, Options{Now: func() time.Time { return now }, NewToken: func() (string, error) { return "claim", nil }, BatchSize: 1, ClaimTTL: time.Minute, BaseRetry: time.Second, MaxRetry: time.Minute, MaxAttempts: 3, MaxPendingAge: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.RunOnce(context.Background()); !errors.Is(err, ErrPublishUnavailable) {
		t.Fatalf("RunOnce() error=%v", err)
	}
	if terminal := repository.failed["obx_terminal"]; terminal.token != "claim" || terminal.reason != "broker_unavailable" {
		t.Fatalf("terminal=%#v", terminal)
	}
	if len(repository.retried) != 0 {
		t.Fatalf("exhausted row was retried: %#v", repository.retried)
	}
}

func TestDispatcherReadinessUsesOnlyPendingAge(t *testing.T) {
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	oldest := now.Add(-6 * time.Minute)
	repository := &memoryOutbox{stats: store.OutboxStats{Pending: 3, Claimed: 1, OldestPendingAt: &oldest}}
	dispatcher, err := NewDispatcher(repository, &recordingPublisher{}, Options{Now: func() time.Time { return now }, NewToken: func() (string, error) { return "claim", nil }, BatchSize: 1, ClaimTTL: time.Minute, BaseRetry: time.Second, MaxRetry: time.Minute, MaxAttempts: 10, MaxPendingAge: 5 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Ping(context.Background()); !errors.Is(err, ErrOutboxLag) || err.Error() != ErrOutboxLag.Error() {
		t.Fatalf("Ping() error=%q", err)
	}
	repository.stats.OldestPendingAt = nil
	repository.stats.Pending = 0
	if err := dispatcher.Ping(context.Background()); err != nil {
		t.Fatalf("empty Ping() error=%v", err)
	}
	repository.stats.Failed = 1
	if err := dispatcher.Ping(context.Background()); !errors.Is(err, ErrOutboxTerminal) {
		t.Fatalf("terminal Ping() error=%v", err)
	}
}

type retryRecord struct {
	token  string
	at     time.Time
	reason string
}
type memoryOutbox struct {
	messages           []store.OutboxMessage
	claimedAt          time.Time
	claimToken         string
	dispatched         map[string]string
	retried            map[string]retryRecord
	failed             map[string]retryRecord
	stats              store.OutboxStats
	failCompletionOnce bool
}

func (memory *memoryOutbox) ClaimOutbox(_ context.Context, now, staleBefore time.Time, token string, _ int) ([]store.OutboxMessage, error) {
	if !memory.claimedAt.IsZero() && memory.claimedAt.After(staleBefore) {
		return []store.OutboxMessage{}, nil
	}
	memory.claimedAt, memory.claimToken = now, token
	result := append([]store.OutboxMessage(nil), memory.messages...)
	for index := range result {
		result[index].ClaimToken = token
	}
	return result, nil
}
func (memory *memoryOutbox) MarkOutboxDispatched(_ context.Context, id, token string, _ time.Time) error {
	if memory.failCompletionOnce {
		memory.failCompletionOnce = false
		return errors.New("database stopped after publish")
	}
	if memory.dispatched == nil {
		memory.dispatched = map[string]string{}
	}
	memory.dispatched[id] = token
	memory.messages = nil
	return nil
}
func (memory *memoryOutbox) RetryOutbox(_ context.Context, id, token string, at time.Time, reason string) error {
	if memory.retried == nil {
		memory.retried = map[string]retryRecord{}
	}
	memory.retried[id] = retryRecord{token, at, reason}
	return nil
}
func (memory *memoryOutbox) FailOutbox(_ context.Context, id, token string, at time.Time, reason string) error {
	if memory.failed == nil {
		memory.failed = map[string]retryRecord{}
	}
	memory.failed[id] = retryRecord{token, at, reason}
	return nil
}
func (memory *memoryOutbox) OutboxStats(context.Context) (store.OutboxStats, error) {
	return memory.stats, nil
}

type recordingPublisher struct {
	publications []broker.Publication
	err          error
}

func (publisher *recordingPublisher) Publish(_ context.Context, publication broker.Publication) (broker.PublishAck, error) {
	publisher.publications = append(publisher.publications, publication)
	return broker.PublishAck{Duplicate: len(publisher.publications) > 1}, publisher.err
}
