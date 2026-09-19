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
	}
	if err := tx.Commit(ctx); err != nil {
		return adminread.ReplayResult{}, false, err
	}
	return result, false, nil
}

var _ interface {
	ReplayAdminDeadLetters(context.Context, adminread.ReplayCommand) (adminread.ReplayResult, bool, error)
} = (*Client)(nil)
