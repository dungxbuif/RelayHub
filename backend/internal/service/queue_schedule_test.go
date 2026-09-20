package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
)

type queueAdvancedRepository struct {
	store.QueueRepository
	created      domain.QueueSchedule
	claims       []domain.QueueSchedule
	completions  []store.QueueScheduleCompletion
	drain        domain.QueueDrain
	subscription domain.QueueSubscription
	audits       []store.AuditRecord
}

func (repository *queueAdvancedRepository) AppendAuditRecord(_ context.Context, record store.AuditRecord) error {
	repository.audits = append(repository.audits, record)
	return nil
}

func (repository *queueAdvancedRepository) CreateQueueSubscription(_ context.Context, item domain.QueueSubscription) error {
	repository.subscription = item
	return nil
}

func (repository *queueAdvancedRepository) CreateQueueSchedule(_ context.Context, item domain.QueueSchedule) error {
	repository.created = item
	return nil
}
func (repository *queueAdvancedRepository) ClaimDueQueueSchedules(context.Context, time.Time, time.Time, string, int) ([]domain.QueueSchedule, error) {
	items := repository.claims
	repository.claims = nil
	return items, nil
}
func (repository *queueAdvancedRepository) CompleteQueueSchedule(_ context.Context, completion store.QueueScheduleCompletion) error {
	repository.completions = append(repository.completions, completion)
	return nil
}
func (repository *queueAdvancedRepository) BeginQueueDrain(_ context.Context, _, subscriptionID string, startedAt, deadlineAt time.Time) (domain.QueueDrain, error) {
	repository.drain = domain.QueueDrain{SubscriptionID: subscriptionID, Status: "draining", InFlight: 2, StartedAt: &startedAt, DeadlineAt: &deadlineAt}
	return repository.drain, nil
}
func (repository *queueAdvancedRepository) GetQueueDrain(context.Context, string, string, time.Time) (domain.QueueDrain, error) {
	return repository.drain, nil
}

func TestQueueScheduleUsesIANAZoneAndSkipsNonexistentDSTWallTime(t *testing.T) {
	from := time.Date(2026, 3, 8, 6, 55, 0, 0, time.UTC)
	repository := &queueAdvancedRepository{}
	service := NewQueueService(repository, QueueOptions{Now: func() time.Time { return from }, NewScheduleID: func() string { return "qsch_dst" }})
	item, err := service.CreateSchedule(context.Background(), "app_1", "sub_1", QueueScheduleInput{Name: "daily", CronExpression: "30 2 * * *", Timezone: "America/New_York", EventType: "report.daily", Data: json.RawMessage(`{"kind":"daily"}`)})
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 3, 9, 6, 30, 0, 0, time.UTC)
	if item.NextRunAt != want || repository.created.NextRunAt != want {
		t.Fatalf("next run=%s want=%s", item.NextRunAt, want)
	}
	for _, input := range []QueueScheduleInput{
		{Name: "bad", CronExpression: "@every 10s", Timezone: "UTC", EventType: "x", Data: json.RawMessage(`{}`)},
		{Name: "bad", CronExpression: "* * * * *", Timezone: "Local", EventType: "x", Data: json.RawMessage(`{}`)},
		{Name: "bad", CronExpression: "* * * * *", Timezone: "UTC", EventType: "x", Data: json.RawMessage(`[]`)},
	} {
		if _, err := service.CreateSchedule(context.Background(), "app_1", "sub_1", input); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("input=%#v error=%v", input, err)
		}
	}
}

func TestQueueSchedulerCompletesClaimWithDeterministicOccurrenceIDs(t *testing.T) {
	due := time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC)
	repository := &queueAdvancedRepository{claims: []domain.QueueSchedule{{ID: "qsch_1", AppID: "app_1", SubscriptionID: "sub_1", CronExpression: "*/5 * * * *", Timezone: "UTC", NextRunAt: due, ClaimToken: "claim", ClaimGeneration: 4}}}
	scheduler := NewQueueScheduler(repository, func() time.Time { return due })
	if err := scheduler.runOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(repository.completions) != 1 {
		t.Fatalf("completions=%d", len(repository.completions))
	}
	completion := repository.completions[0]
	eventID, deliveryID := scheduleOccurrenceIDs("qsch_1", due)
	if completion.EventID != eventID || completion.DeliveryID != deliveryID || completion.ClaimGeneration != 4 || completion.NextRunAt != due.Add(5*time.Minute) {
		t.Fatalf("completion=%#v", completion)
	}
}

func TestQueueDrainStopsAtBoundedDeadline(t *testing.T) {
	now := time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC)
	repository := &queueAdvancedRepository{}
	service := NewQueueService(repository, QueueOptions{Now: func() time.Time { return now }})
	drain, err := service.BeginDrain(context.Background(), "app_1", "sub_1", 45)
	if err != nil || drain.Status != "draining" || drain.DeadlineAt == nil || !drain.DeadlineAt.Equal(now.Add(45*time.Second)) {
		t.Fatalf("drain=%#v error=%v", drain, err)
	}
	if _, err := service.BeginDrain(context.Background(), "app_1", "sub_1", 3601); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("unbounded drain error=%v", err)
	}
}

func TestQueueResultCallbacksRequireSafeHTTPSConfiguration(t *testing.T) {
	repository := &queueAdvancedRepository{}
	service := NewQueueService(repository, QueueOptions{NewID: func() string { return "sub_callbacks" }, Audit: repository})
	insecure := "http://public.example/result"
	if _, err := service.Create(context.Background(), "app_1", QueueSubscriptionInput{Name: "callbacks", SuccessCallbackURL: &insecure}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("insecure callback error=%v", err)
	}
	secure := "https://callbacks.example/result"
	if _, err := service.Create(context.Background(), "app_1", QueueSubscriptionInput{Name: "callbacks", SuccessCallbackURL: &secure, ResultCallbackMetadata: []byte(`{"nested":{"api_token":"secret"}}`)}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("secret metadata error=%v", err)
	}
	created, err := service.Create(context.Background(), "app_1", QueueSubscriptionInput{Name: "callbacks", SuccessCallbackURL: &secure, FailureCallbackURL: &secure, ResultCallbackMetadata: []byte(`{"integration":"orders"}`)})
	if err != nil || created.SuccessCallbackURL == nil || repository.subscription.FailureCallbackURL == nil {
		t.Fatalf("created=%#v error=%v", created, err)
	}
	if len(repository.audits) != 1 || repository.audits[0].Action != "queue.subscription.create" || repository.audits[0].ActorID != "app_1" {
		t.Fatalf("audits=%#v", repository.audits)
	}
}
