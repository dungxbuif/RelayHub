package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/jackc/pgx/v5"
)

const queueScheduleColumns = `id,app_id,subscription_id,name,enabled,cron_expression,timezone,event_type,data::text,COALESCE(ordering_key,''),priority,metadata::text,next_run_at,last_run_at,policy_version,created_at,updated_at,COALESCE(claim_token,''),claim_generation`
const queueScheduleQualifiedColumns = `q.id,q.app_id,q.subscription_id,q.name,q.enabled,q.cron_expression,q.timezone,q.event_type,q.data::text,COALESCE(q.ordering_key,''),q.priority,q.metadata::text,q.next_run_at,q.last_run_at,q.policy_version,q.created_at,q.updated_at,COALESCE(q.claim_token,''),q.claim_generation`

func scanQueueSchedule(row queueScanner) (domain.QueueSchedule, error) {
	var item domain.QueueSchedule
	var data, metadata string
	err := row.Scan(&item.ID, &item.AppID, &item.SubscriptionID, &item.Name, &item.Enabled, &item.CronExpression, &item.Timezone, &item.EventType, &data, &item.OrderingKey, &item.Priority, &metadata, &item.NextRunAt, &item.LastRunAt, &item.PolicyVersion, &item.CreatedAt, &item.UpdatedAt, &item.ClaimToken, &item.ClaimGeneration)
	if errors.Is(err, pgx.ErrNoRows) {
		return item, store.ErrNotFound
	}
	item.Data, item.Metadata = json.RawMessage(data), json.RawMessage(metadata)
	return item, err
}

func (client *Client) CreateQueueSchedule(ctx context.Context, item domain.QueueSchedule) error {
	result, err := client.pool.Exec(ctx, `INSERT INTO queue_schedules(id,app_id,subscription_id,name,enabled,cron_expression,timezone,event_type,data,ordering_key,priority,metadata,next_run_at,policy_version,created_at,updated_at) SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,NULLIF($10,''),$11,$12::jsonb,$13,$14,$15,$16 FROM queue_subscriptions WHERE app_id=$2 AND id=$3`, item.ID, item.AppID, item.SubscriptionID, item.Name, item.Enabled, item.CronExpression, item.Timezone, item.EventType, string(item.Data), item.OrderingKey, item.Priority, string(item.Metadata), item.NextRunAt, item.PolicyVersion, item.CreatedAt, item.UpdatedAt)
	if uniqueViolation(err) {
		return store.ErrConflict
	}
	if err == nil && result.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return err
}

func (client *Client) ListQueueSchedules(ctx context.Context, appID, subscriptionID string) ([]domain.QueueSchedule, error) {
	rows, err := client.pool.Query(ctx, `SELECT `+queueScheduleColumns+` FROM queue_schedules WHERE app_id=$1 AND subscription_id=$2 ORDER BY created_at,id`, appID, subscriptionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []domain.QueueSchedule{}
	for rows.Next() {
		item, err := scanQueueSchedule(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (client *Client) GetQueueSchedule(ctx context.Context, appID, subscriptionID, scheduleID string) (domain.QueueSchedule, error) {
	return scanQueueSchedule(client.pool.QueryRow(ctx, `SELECT `+queueScheduleColumns+` FROM queue_schedules WHERE app_id=$1 AND subscription_id=$2 AND id=$3`, appID, subscriptionID, scheduleID))
}

func (client *Client) UpdateQueueSchedule(ctx context.Context, item domain.QueueSchedule, expectedVersion int64) (domain.QueueSchedule, error) {
	row := client.pool.QueryRow(ctx, `UPDATE queue_schedules SET name=$4,enabled=$5,cron_expression=$6,timezone=$7,event_type=$8,data=$9::jsonb,ordering_key=NULLIF($10,''),priority=$11,metadata=$12::jsonb,next_run_at=$13,claim_token=NULL,claim_expires_at=NULL,policy_version=policy_version+1,updated_at=$14 WHERE app_id=$1 AND subscription_id=$2 AND id=$3 AND policy_version=$15 RETURNING `+queueScheduleColumns, item.AppID, item.SubscriptionID, item.ID, item.Name, item.Enabled, item.CronExpression, item.Timezone, item.EventType, string(item.Data), item.OrderingKey, item.Priority, string(item.Metadata), item.NextRunAt, item.UpdatedAt, expectedVersion)
	updated, err := scanQueueSchedule(row)
	if errors.Is(err, store.ErrNotFound) {
		if _, findErr := client.GetQueueSchedule(ctx, item.AppID, item.SubscriptionID, item.ID); findErr == nil {
			return domain.QueueSchedule{}, store.ErrConflict
		}
	}
	return updated, err
}

func (client *Client) DeleteQueueSchedule(ctx context.Context, appID, subscriptionID, scheduleID string) error {
	result, err := client.pool.Exec(ctx, `DELETE FROM queue_schedules WHERE app_id=$1 AND subscription_id=$2 AND id=$3`, appID, subscriptionID, scheduleID)
	if err == nil && result.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return err
}

func (client *Client) ClaimDueQueueSchedules(ctx context.Context, now, claimUntil time.Time, token string, limit int) ([]domain.QueueSchedule, error) {
	rows, err := client.pool.Query(ctx, `WITH due AS (SELECT q.id FROM queue_schedules q JOIN queue_subscriptions s ON s.id=q.subscription_id AND s.app_id=q.app_id WHERE q.enabled AND q.next_run_at<=$1 AND (q.claim_expires_at IS NULL OR q.claim_expires_at<=$1) AND s.enabled AND s.paused_at IS NULL AND s.draining_at IS NULL ORDER BY q.next_run_at,q.id FOR UPDATE OF q SKIP LOCKED LIMIT $4) UPDATE queue_schedules q SET claim_token=$3,claim_expires_at=$2,claim_generation=q.claim_generation+1,updated_at=$1 FROM due WHERE q.id=due.id RETURNING `+queueScheduleQualifiedColumns, now, claimUntil, token, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []domain.QueueSchedule{}
	for rows.Next() {
		item, err := scanQueueSchedule(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (client *Client) CompleteQueueSchedule(ctx context.Context, completion store.QueueScheduleCompletion) error {
	tx, err := client.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var schedule domain.QueueSchedule
	var data, metadata string
	var retentionSeconds int
	err = tx.QueryRow(ctx, `SELECT q.id,q.app_id,q.subscription_id,q.event_type,q.data::text,COALESCE(q.ordering_key,''),q.priority,q.metadata::text,q.next_run_at,s.retention_seconds FROM queue_schedules q JOIN queue_subscriptions s ON s.id=q.subscription_id AND s.app_id=q.app_id WHERE q.id=$1 AND q.claim_token=$2 AND q.claim_generation=$3 FOR UPDATE`, completion.ScheduleID, completion.ClaimToken, completion.ClaimGeneration).Scan(&schedule.ID, &schedule.AppID, &schedule.SubscriptionID, &schedule.EventType, &data, &schedule.OrderingKey, &schedule.Priority, &metadata, &schedule.NextRunAt, &retentionSeconds)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.ErrConflict
	}
	if err != nil {
		return err
	}
	if !schedule.NextRunAt.Equal(completion.OccurrenceAt) {
		return store.ErrConflict
	}
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM queue_schedule_occurrences WHERE schedule_id=$1 AND scheduled_at=$2)`, schedule.ID, completion.OccurrenceAt).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		expiresAt := completion.OccurrenceAt.Add(time.Duration(retentionSeconds) * time.Second)
		if _, err := tx.Exec(ctx, `INSERT INTO events(id,type,source_app_id,target_app_ids,data,created_at,expires_at,queue_available_at,queue_ordering_key,queue_priority,queue_metadata) VALUES($1,$2,$3,$4,$5::json,$6,$7,$6,NULLIF($8,''),$9,$10::jsonb) ON CONFLICT(id) DO NOTHING`, completion.EventID, schedule.EventType, schedule.AppID, []string{schedule.AppID}, data, completion.OccurrenceAt, expiresAt, schedule.OrderingKey, schedule.Priority, metadata); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO queue_deliveries(id,subscription_id,app_id,event_id,status,attempts,available_at,ordering_key,priority,metadata,created_at,updated_at,expires_at) VALUES($1,$2,$3,$4,'available',0,$5,NULLIF($6,''),$7,$8::jsonb,$5,$5,$9) ON CONFLICT(subscription_id,event_id) DO NOTHING`, completion.DeliveryID, schedule.SubscriptionID, schedule.AppID, completion.EventID, completion.OccurrenceAt, schedule.OrderingKey, schedule.Priority, metadata, expiresAt); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO queue_schedule_occurrences(schedule_id,scheduled_at,event_id,created_at) VALUES($1,$2,$3,$4)`, schedule.ID, completion.OccurrenceAt, completion.EventID, completion.Now); err != nil {
			return err
		}
	}
	result, err := tx.Exec(ctx, `UPDATE queue_schedules SET last_run_at=$4,next_run_at=$5,claim_token=NULL,claim_expires_at=NULL,updated_at=$6 WHERE id=$1 AND claim_token=$2 AND claim_generation=$3`, schedule.ID, completion.ClaimToken, completion.ClaimGeneration, completion.OccurrenceAt, completion.NextRunAt, completion.Now)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return store.ErrConflict
	}
	return tx.Commit(ctx)
}

func (client *Client) BeginQueueDrain(ctx context.Context, appID, subscriptionID string, startedAt, deadlineAt time.Time) (domain.QueueDrain, error) {
	result, err := client.pool.Exec(ctx, `UPDATE queue_subscriptions SET draining_at=COALESCE(draining_at,$3),drain_deadline_at=COALESCE(drain_deadline_at,$4),policy_version=CASE WHEN draining_at IS NULL THEN policy_version+1 ELSE policy_version END,updated_at=$3 WHERE app_id=$1 AND id=$2`, appID, subscriptionID, startedAt, deadlineAt)
	if err != nil {
		return domain.QueueDrain{}, err
	}
	if result.RowsAffected() == 0 {
		return domain.QueueDrain{}, store.ErrNotFound
	}
	return client.GetQueueDrain(ctx, appID, subscriptionID, startedAt)
}

func (client *Client) GetQueueDrain(ctx context.Context, appID, subscriptionID string, now time.Time) (domain.QueueDrain, error) {
	var drain domain.QueueDrain
	drain.SubscriptionID = subscriptionID
	err := client.pool.QueryRow(ctx, `SELECT draining_at,drain_deadline_at,drained_at,(SELECT count(*) FROM queue_deliveries d WHERE d.subscription_id=s.id AND d.status='in_flight') FROM queue_subscriptions s WHERE app_id=$1 AND id=$2`, appID, subscriptionID).Scan(&drain.StartedAt, &drain.DeadlineAt, &drain.CompletedAt, &drain.InFlight)
	if errors.Is(err, pgx.ErrNoRows) {
		return drain, store.ErrNotFound
	}
	if err != nil {
		return drain, err
	}
	drain.Status = "active"
	if drain.StartedAt == nil {
		return drain, nil
	}
	if drain.InFlight == 0 {
		completed := now
		if drain.CompletedAt == nil {
			if _, err := client.pool.Exec(ctx, `UPDATE queue_subscriptions SET drained_at=$3,updated_at=$3 WHERE app_id=$1 AND id=$2 AND draining_at IS NOT NULL AND drained_at IS NULL`, appID, subscriptionID, completed); err != nil {
				return drain, err
			}
			drain.CompletedAt = &completed
		}
		drain.Status = "drained"
	} else if drain.DeadlineAt != nil && !now.Before(*drain.DeadlineAt) {
		drain.Status = "timed_out"
	} else {
		drain.Status = "draining"
	}
	return drain, nil
}
