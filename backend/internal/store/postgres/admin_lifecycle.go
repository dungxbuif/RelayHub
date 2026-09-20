package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/dungxbuif/RelayHub/internal/adminread"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/jackc/pgx/v5"
)

const adminReplayRetention = 24 * time.Hour

type replayDeliveryRow struct {
	id, eventID, jobID, sink, outboxID string
	generation                         int64
	payload                            []byte
}

func (client *Client) GetAdminEventTimeline(ctx context.Context, eventID string) (adminread.EventTimeline, error) {
	if eventID == "" || len(eventID) > 256 {
		return adminread.EventTimeline{}, adminread.ErrInvalidArgument
	}
	tx, err := client.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return adminread.EventTimeline{}, err
	}
	defer tx.Rollback(ctx)

	result := adminread.EventTimeline{
		Deliveries: make([]adminread.DeliveryLifecycleSummary, 0),
		Attempts:   make([]adminread.DeliveryAttemptSummary, 0),
		Items:      make([]adminread.TimelineItem, 0),
	}
	var data []byte
	err = tx.QueryRow(ctx, `SELECT e.id,e.type,e.source_app_id,cardinality(e.target_app_ids),count(d.id),e.created_at,e.target_app_ids,e.data::text
		FROM events e LEFT JOIN deliveries d ON d.event_id=e.id WHERE e.id=$1 GROUP BY e.id`, eventID).Scan(
		&result.Event.ID, &result.Event.Type, &result.Event.SourceAppID, &result.Event.TargetCount,
		&result.Event.DeliveryCount, &result.Event.CreatedAt, &result.Event.TargetAppIDs, &data)
	if errors.Is(err, pgx.ErrNoRows) {
		return adminread.EventTimeline{}, store.ErrNotFound
	}
	if err != nil {
		return adminread.EventTimeline{}, err
	}
	result.Event.Data = json.RawMessage(data)
	result.Items = append(result.Items, adminread.TimelineItem{
		ID: "event:" + eventID, Type: "event.created", OccurredAt: result.Event.CreatedAt,
		EventID: eventID, Outcome: "accepted",
	})

	deliveries, err := tx.Query(ctx, `SELECT d.id,d.public_job_id,d.event_id,d.target_app_id,d.sink,d.status,
		COALESCE(d.callback_reason,o.last_error,a.reason,''),d.generation,d.attempts,d.created_at,d.updated_at
		FROM deliveries d LEFT JOIN outbox o ON o.delivery_id=d.id
		LEFT JOIN LATERAL (SELECT reason FROM delivery_attempts WHERE delivery_id=d.id ORDER BY generation DESC,attempt DESC LIMIT 1) a ON true
		WHERE d.event_id=$1 ORDER BY d.created_at,d.id`, eventID)
	if err != nil {
		return adminread.EventTimeline{}, err
	}
	for deliveries.Next() {
		var item adminread.DeliveryLifecycleSummary
		if err := deliveries.Scan(&item.DeliveryID, &item.JobID, &item.EventID, &item.TargetAppID, &item.Sink,
			&item.Status, &item.Reason, &item.Generation, &item.Attempts, &item.CreatedAt, &item.UpdatedAt); err != nil {
			deliveries.Close()
			return adminread.EventTimeline{}, err
		}
		result.Deliveries = append(result.Deliveries, item)
	}
	if err := deliveries.Err(); err != nil {
		deliveries.Close()
		return adminread.EventTimeline{}, err
	}
	deliveries.Close()

	attempts, err := tx.Query(ctx, `SELECT a.delivery_id,a.generation,a.attempt,a.outcome,COALESCE(a.reason,''),a.created_at,a.updated_at
		FROM delivery_attempts a JOIN deliveries d ON d.id=a.delivery_id
		WHERE d.event_id=$1 ORDER BY a.created_at,a.id`, eventID)
	if err != nil {
		return adminread.EventTimeline{}, err
	}
	for attempts.Next() {
		var item adminread.DeliveryAttemptSummary
		if err := attempts.Scan(&item.DeliveryID, &item.Generation, &item.Attempt, &item.Outcome, &item.Reason, &item.StartedAt, &item.UpdatedAt); err != nil {
			attempts.Close()
			return adminread.EventTimeline{}, err
		}
		result.Attempts = append(result.Attempts, item)
	}
	if err := attempts.Err(); err != nil {
		attempts.Close()
		return adminread.EventTimeline{}, err
	}
	attempts.Close()

	lifecycle, err := tx.Query(ctx, `SELECT l.id,l.delivery_id,d.public_job_id,l.generation,COALESCE(l.attempt,0),l.type,
		COALESCE(l.outcome,''),COALESCE(l.reason,''),COALESCE(l.actor_type,''),COALESCE(l.actor_id,''),l.occurred_at
		FROM delivery_lifecycle l JOIN deliveries d ON d.id=l.delivery_id
		WHERE d.event_id=$1 ORDER BY l.occurred_at,l.id`, eventID)
	if err != nil {
		return adminread.EventTimeline{}, err
	}
	for lifecycle.Next() {
		var sequence int64
		var item adminread.TimelineItem
		if err := lifecycle.Scan(&sequence, &item.DeliveryID, &item.JobID, &item.Generation, &item.Attempt, &item.Type,
			&item.Outcome, &item.Reason, &item.ActorType, &item.ActorID, &item.OccurredAt); err != nil {
			lifecycle.Close()
			return adminread.EventTimeline{}, err
		}
		item.ID = "lifecycle:" + strconv.FormatInt(sequence, 10)
		item.EventID = eventID
		result.Items = append(result.Items, item)
	}
	if err := lifecycle.Err(); err != nil {
		lifecycle.Close()
		return adminread.EventTimeline{}, err
	}
	lifecycle.Close()
	sort.SliceStable(result.Items, func(i, j int) bool {
		if result.Items[i].OccurredAt.Equal(result.Items[j].OccurredAt) {
			return result.Items[i].ID < result.Items[j].ID
		}
		return result.Items[i].OccurredAt.Before(result.Items[j].OccurredAt)
	})
	if err := tx.Commit(ctx); err != nil {
		return adminread.EventTimeline{}, err
	}
	return result, nil
}

func (client *Client) ReplayAdminDeadLetters(ctx context.Context, command adminread.ReplayCommand) (adminread.ReplayResult, bool, error) {
	if err := command.Validate(); err != nil {
		return adminread.ReplayResult{}, false, err
	}
	ids := append([]string(nil), command.DeliveryIDs...)
	sort.Strings(ids)
	tx, err := client.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return adminread.ReplayResult{}, false, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, advisoryKey("admin-replay", command.IdempotencyKeyHash)); err != nil {
		return adminread.ReplayResult{}, false, err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM admin_replay_requests WHERE expires_at<=clock_timestamp()`); err != nil {
		return adminread.ReplayResult{}, false, err
	}
	var fingerprint string
	var storedRaw []byte
	err = tx.QueryRow(ctx, `SELECT request_fingerprint,result FROM admin_replay_requests WHERE idempotency_key_hash=$1`, command.IdempotencyKeyHash).Scan(&fingerprint, &storedRaw)
	if err == nil {
		if fingerprint != command.RequestFingerprint {
			return adminread.ReplayResult{}, false, store.ErrConflict
		}
		var stored adminread.ReplayResult
		if json.Unmarshal(storedRaw, &stored) != nil {
			return adminread.ReplayResult{}, false, store.ErrConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return adminread.ReplayResult{}, false, err
		}
		return stored, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return adminread.ReplayResult{}, false, err
	}

	rows, err := tx.Query(ctx, `SELECT d.id,d.event_id,d.public_job_id,d.sink,d.generation,o.id,o.payload
		FROM deliveries d JOIN outbox o ON o.delivery_id=d.id
		WHERE d.id=ANY($1) AND d.status='dead_letter'
		ORDER BY d.id FOR UPDATE OF d,o`, ids)
	if err != nil {
		return adminread.ReplayResult{}, false, err
	}
	locked := make([]replayDeliveryRow, 0, len(ids))
	for rows.Next() {
		var row replayDeliveryRow
		if err := rows.Scan(&row.id, &row.eventID, &row.jobID, &row.sink, &row.generation, &row.outboxID, &row.payload); err != nil {
			rows.Close()
			return adminread.ReplayResult{}, false, err
		}
		locked = append(locked, row)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return adminread.ReplayResult{}, false, err
	}
	if len(locked) != len(ids) {
		return adminread.ReplayResult{}, false, store.ErrConflict
	}

	result := adminread.ReplayResult{Items: make([]adminread.ReplayItem, 0, len(locked))}
	for _, row := range locked {
		nextGeneration := row.generation + 1
		var envelope outboxEnvelope
		if err := json.Unmarshal(row.payload, &envelope); err != nil || envelope.DeliveryID != row.id || envelope.Event.ID != row.eventID {
			return adminread.ReplayResult{}, false, store.ErrConflict
		}
		envelope.Generation = nextGeneration
		payload, err := json.Marshal(envelope)
		if err != nil {
			return adminread.ReplayResult{}, false, err
		}
		updated, err := tx.Exec(ctx, `UPDATE deliveries SET generation=$2,status='pending',attempts=0,
			assigned_connection_id=NULL,assignment_token=NULL,assignment_started_at=NULL,assignment_expires_at=NULL,assignment_max_expires_at=NULL,
			callback_token=NULL,callback_expires_at=NULL,callback_retry_at=NULL,callback_url=NULL,callback_credential_version=NULL,callback_reason=NULL,callback_dlq_published_at=NULL,
			updated_at=$3 WHERE id=$1 AND generation=$4 AND status='dead_letter'`, row.id, nextGeneration, command.Now, row.generation)
		if err != nil {
			return adminread.ReplayResult{}, false, err
		}
		if updated.RowsAffected() != 1 {
			return adminread.ReplayResult{}, false, store.ErrConflict
		}
		_, baseMessageID := outboxIdentities(row.id)
		messageID := baseMessageID + "-g" + strconv.FormatInt(nextGeneration, 10)
		updated, err = tx.Exec(ctx, `UPDATE outbox SET generation=$2,payload=$3,message_id=$4,attempts=0,available_at=$5,claimed_at=NULL,claim_token=NULL,dispatched_at=NULL,failed_at=NULL,last_error=NULL,updated_at=$5 WHERE id=$1 AND generation=$6`, row.outboxID, nextGeneration, payload, messageID, command.Now, row.generation)
		if err != nil {
			return adminread.ReplayResult{}, false, err
		}
		if updated.RowsAffected() != 1 {
			return adminread.ReplayResult{}, false, store.ErrConflict
		}
		result.Items = append(result.Items, adminread.ReplayItem{DeliveryID: row.id, EventID: row.eventID, JobID: row.jobID, FromGeneration: row.generation, Generation: nextGeneration, Status: "pending"})
	}
	resultRaw, err := json.Marshal(result)
	if err != nil || len(resultRaw) > 64*1024 {
		return adminread.ReplayResult{}, false, fmt.Errorf("encode bounded replay result")
	}
	if _, err := tx.Exec(ctx, `INSERT INTO admin_replay_requests(idempotency_key_hash,request_fingerprint,result,actor_id,created_at,expires_at) VALUES($1,$2,$3,$4,$5,$6)`, command.IdempotencyKeyHash, command.RequestFingerprint, resultRaw, command.ActorID, command.Now, command.Now.Add(adminReplayRetention)); err != nil {
		return adminread.ReplayResult{}, false, err
	}
	for _, item := range result.Items {
		if _, err := tx.Exec(ctx, `INSERT INTO delivery_replays(delivery_id,from_generation,generation,idempotency_key_hash,actor_id,occurred_at) VALUES($1,$2,$3,$4,$5,$6)`, item.DeliveryID, item.FromGeneration, item.Generation, command.IdempotencyKeyHash, command.ActorID, command.Now); err != nil {
			return adminread.ReplayResult{}, false, err
		}
		metadata, _ := json.Marshal(map[string]any{"previous_state": "dead_letter", "new_state": "pending"})
		if _, err := tx.Exec(ctx, `INSERT INTO audit_log(occurred_at,actor_type,actor_id,action,resource_type,resource_id,outcome,metadata) VALUES($1,'admin',$2,'delivery.replay','delivery',$3,'success',$4)`, command.Now, command.ActorID, item.DeliveryID, metadata); err != nil {
			return adminread.ReplayResult{}, false, err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO delivery_lifecycle(delivery_id,generation,type,outcome,actor_type,actor_id,occurred_at) VALUES($1,$2,'operator.replayed','pending','admin',$3,$4)`, item.DeliveryID, item.Generation, command.ActorID, command.Now); err != nil {
			return adminread.ReplayResult{}, false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return adminread.ReplayResult{}, false, err
	}
	return result, false, nil
}

var _ interface {
	GetAdminEventTimeline(context.Context, string) (adminread.EventTimeline, error)
	ReplayAdminDeadLetters(context.Context, adminread.ReplayCommand) (adminread.ReplayResult, bool, error)
} = (*Client)(nil)
