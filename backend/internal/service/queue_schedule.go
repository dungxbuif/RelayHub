package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

var queueCronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

type QueueScheduleInput struct {
	Name           string          `json:"name"`
	Enabled        *bool           `json:"enabled,omitempty"`
	CronExpression string          `json:"cron_expression"`
	Timezone       string          `json:"timezone"`
	EventType      string          `json:"event_type"`
	Data           json.RawMessage `json:"data"`
	OrderingKey    string          `json:"ordering_key,omitempty"`
	Priority       int             `json:"priority,omitempty"`
	Metadata       json.RawMessage `json:"metadata,omitempty"`
}

func (service *QueueService) CreateSchedule(ctx context.Context, appID, subscriptionID string, input QueueScheduleInput) (domain.QueueSchedule, error) {
	if service == nil || nilDependency(service.repository) {
		return domain.QueueSchedule{}, ErrInvalidDependency
	}
	now := service.options.Now().UTC()
	item, err := service.scheduleFromInput(appID, subscriptionID, input, now)
	if err != nil {
		return domain.QueueSchedule{}, err
	}
	item.ID, item.PolicyVersion, item.CreatedAt, item.UpdatedAt = service.options.NewScheduleID(), 1, now, now
	if err := service.repository.CreateQueueSchedule(ctx, item); err != nil {
		return domain.QueueSchedule{}, mapStoreError(err)
	}
	service.audit(ctx, appID, "queue.schedule.create", "queue_schedule", item.ID, []string{"created"})
	return item, nil
}

func (service *QueueService) ListSchedules(ctx context.Context, appID, subscriptionID string) ([]domain.QueueSchedule, error) {
	if service == nil || nilDependency(service.repository) {
		return nil, ErrInvalidDependency
	}
	if appID == "" || subscriptionID == "" {
		return nil, ErrInvalidInput
	}
	items, err := service.repository.ListQueueSchedules(ctx, appID, subscriptionID)
	return items, mapStoreError(err)
}

func (service *QueueService) GetSchedule(ctx context.Context, appID, subscriptionID, scheduleID string) (domain.QueueSchedule, error) {
	if service == nil || nilDependency(service.repository) {
		return domain.QueueSchedule{}, ErrInvalidDependency
	}
	if appID == "" || subscriptionID == "" || scheduleID == "" {
		return domain.QueueSchedule{}, ErrInvalidInput
	}
	item, err := service.repository.GetQueueSchedule(ctx, appID, subscriptionID, scheduleID)
	return item, mapStoreError(err)
}

func (service *QueueService) UpdateSchedule(ctx context.Context, appID, subscriptionID, scheduleID string, expectedVersion int64, input QueueScheduleInput) (domain.QueueSchedule, error) {
	if expectedVersion < 1 || scheduleID == "" {
		return domain.QueueSchedule{}, ErrInvalidInput
	}
	current, err := service.GetSchedule(ctx, appID, subscriptionID, scheduleID)
	if err != nil {
		return domain.QueueSchedule{}, err
	}
	now := service.options.Now().UTC()
	item, err := service.scheduleFromInput(appID, subscriptionID, input, now)
	if err != nil {
		return domain.QueueSchedule{}, err
	}
	item.ID, item.CreatedAt, item.UpdatedAt, item.LastRunAt = current.ID, current.CreatedAt, now, current.LastRunAt
	item, err = service.repository.UpdateQueueSchedule(ctx, item, expectedVersion)
	if err == nil {
		service.audit(ctx, appID, "queue.schedule.update", "queue_schedule", scheduleID, []string{"policy"})
	}
	return item, mapStoreError(err)
}

func (service *QueueService) DeleteSchedule(ctx context.Context, appID, subscriptionID, scheduleID string) error {
	if service == nil || nilDependency(service.repository) {
		return ErrInvalidDependency
	}
	if appID == "" || subscriptionID == "" || scheduleID == "" {
		return ErrInvalidInput
	}
	err := service.repository.DeleteQueueSchedule(ctx, appID, subscriptionID, scheduleID)
	if err == nil {
		service.audit(ctx, appID, "queue.schedule.delete", "queue_schedule", scheduleID, []string{"deleted"})
	}
	return mapStoreError(err)
}

func (service *QueueService) scheduleFromInput(appID, subscriptionID string, input QueueScheduleInput, from time.Time) (domain.QueueSchedule, error) {
	enabled := true
	if input.Enabled != nil {
		enabled = *input.Enabled
	}
	item := domain.QueueSchedule{AppID: appID, SubscriptionID: subscriptionID, Name: strings.TrimSpace(input.Name), Enabled: enabled, CronExpression: strings.TrimSpace(input.CronExpression), Timezone: strings.TrimSpace(input.Timezone), EventType: strings.TrimSpace(input.EventType), Data: append(json.RawMessage(nil), input.Data...), OrderingKey: strings.TrimSpace(input.OrderingKey), Priority: input.Priority, Metadata: append(json.RawMessage(nil), input.Metadata...)}
	if len(item.Metadata) == 0 {
		item.Metadata = json.RawMessage(`{}`)
	}
	if appID == "" || subscriptionID == "" || !queueName.MatchString(item.Name) || item.EventType == "" || len(item.EventType) > 128 || len(item.OrderingKey) > 256 || item.Priority < -10 || item.Priority > 10 || !domain.JSONObject(item.Data) || !domain.JSONObject(item.Metadata) || len(item.Data) > 1<<20 || len(item.Metadata) > 4096 {
		return item, ErrInvalidInput
	}
	location, err := time.LoadLocation(item.Timezone)
	if err != nil || item.Timezone == "Local" {
		return item, ErrInvalidInput
	}
	schedule, err := queueCronParser.Parse(item.CronExpression)
	if err != nil {
		return item, ErrInvalidInput
	}
	first := schedule.Next(from.In(location))
	second := schedule.Next(first)
	if first.IsZero() || second.Sub(first) < time.Minute {
		return item, ErrInvalidInput
	}
	item.NextRunAt = first.UTC()
	return item, nil
}

type QueueScheduler struct {
	repository store.QueueRepository
	now        func() time.Time
	interval   time.Duration
}

func NewQueueScheduler(repository store.QueueRepository, now func() time.Time) *QueueScheduler {
	if now == nil {
		now = time.Now
	}
	return &QueueScheduler{repository: repository, now: now, interval: 500 * time.Millisecond}
}

func (scheduler *QueueScheduler) Run(ctx context.Context) error {
	if scheduler == nil || nilDependency(scheduler.repository) {
		return ErrInvalidDependency
	}
	ticker := time.NewTicker(scheduler.interval)
	defer ticker.Stop()
	for {
		if err := scheduler.runOnce(ctx); err != nil && ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(250 * time.Millisecond):
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (scheduler *QueueScheduler) runOnce(ctx context.Context) error {
	now := scheduler.now().UTC()
	token := "qclaim_" + uuid.NewString()
	items, err := scheduler.repository.ClaimDueQueueSchedules(ctx, now, now.Add(15*time.Second), token, 32)
	if err != nil {
		return err
	}
	for _, item := range items {
		location, err := time.LoadLocation(item.Timezone)
		if err != nil {
			return err
		}
		spec, err := queueCronParser.Parse(item.CronExpression)
		if err != nil {
			return err
		}
		next := spec.Next(item.NextRunAt.In(location)).UTC()
		eventID, deliveryID := scheduleOccurrenceIDs(item.ID, item.NextRunAt)
		err = scheduler.repository.CompleteQueueSchedule(ctx, store.QueueScheduleCompletion{ScheduleID: item.ID, ClaimToken: item.ClaimToken, ClaimGeneration: item.ClaimGeneration, EventID: eventID, DeliveryID: deliveryID, OccurrenceAt: item.NextRunAt, NextRunAt: next, Now: scheduler.now().UTC()})
		if err != nil {
			return err
		}
	}
	return nil
}

func scheduleOccurrenceIDs(scheduleID string, occurrence time.Time) (string, string) {
	digest := sha256.Sum256([]byte(scheduleID + "\x00" + occurrence.UTC().Format(time.RFC3339Nano)))
	suffix := hex.EncodeToString(digest[:16])
	return "evt_sched_" + suffix, "qdl_sched_" + suffix
}
