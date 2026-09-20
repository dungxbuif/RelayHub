package postgres

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/jackc/pgx/v5"
)

var _ store.OutboxStore = (*Client)(nil)

func (client *Client) ClaimOutbox(ctx context.Context, now, staleBefore time.Time, claimToken string, limit int) ([]store.OutboxMessage, error) {
	if claimToken == "" || limit < 1 || limit > 1000 || staleBefore.After(now) {
		return nil, errors.New("invalid outbox claim")
	}
	rows, err := client.pool.Query(ctx, `
		WITH candidates AS (
			SELECT id,(claimed_at IS NOT NULL) AS reclaimed FROM outbox
			WHERE dispatched_at IS NULL AND failed_at IS NULL AND available_at <= $1
			  AND (claimed_at IS NULL OR claimed_at <= $2)
			ORDER BY available_at,created_at,id
			FOR UPDATE SKIP LOCKED LIMIT $3
		)
		UPDATE outbox o SET claimed_at=$1,claim_token=$4,updated_at=$1
		FROM candidates c WHERE o.id=c.id
		RETURNING o.id,o.event_id,o.delivery_id,o.subject,o.payload,o.message_id,o.claim_token,o.attempts,c.reclaimed,o.created_at`, now, staleBefore, limit, claimToken)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	messages := make([]store.OutboxMessage, 0)
	for rows.Next() {
		var message store.OutboxMessage
		if err := rows.Scan(&message.ID, &message.EventID, &message.DeliveryID, &message.Subject, &message.Payload, &message.MessageID, &message.ClaimToken, &message.Attempts, &message.Reclaimed, &message.CreatedAt); err != nil {
			return nil, err
		}
		message.Payload = append([]byte(nil), message.Payload...)
		messages = append(messages, message)
	}
	return messages, rows.Err()
}

func (client *Client) BeginOutboxPublish(ctx context.Context, outboxID, claimToken string, now time.Time, maxAttempts int64) (store.OutboxPublishStart, error) {
	if outboxID == "" || claimToken == "" || maxAttempts < 1 {
		return store.OutboxPublishStart{}, store.ErrConflict
	}
	tx, err := client.pool.Begin(ctx)
	if err != nil {
		return store.OutboxPublishStart{}, err
	}
	defer tx.Rollback(ctx)
	var deliveryID string
	var attempts int64
	err = tx.QueryRow(ctx, `SELECT delivery_id,attempts FROM outbox WHERE id=$1 AND claim_token=$2 AND dispatched_at IS NULL AND failed_at IS NULL FOR UPDATE`, outboxID, claimToken).Scan(&deliveryID, &attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.OutboxPublishStart{}, store.ErrConflict
	}
	if err != nil {
		return store.OutboxPublishStart{}, err
	}
	if attempts >= maxAttempts {
		if _, err := tx.Exec(ctx, `UPDATE outbox SET failed_at=$2,claimed_at=NULL,claim_token=NULL,last_error='max_attempts',updated_at=$2 WHERE id=$1`, outboxID, now); err != nil {
			return store.OutboxPublishStart{}, err
		}
		if _, err := tx.Exec(ctx, `UPDATE deliveries SET status='dead_letter',assigned_connection_id=NULL,assignment_token=NULL,assignment_expires_at=NULL,updated_at=GREATEST(updated_at,$2) WHERE id=$1 AND status!='acked'`, deliveryID, now); err != nil {
			return store.OutboxPublishStart{}, err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO delivery_lifecycle(delivery_id,generation,type,outcome,reason,occurred_at) SELECT id,generation,'delivery.dead_lettered','dead_letter','max_attempts',$2 FROM deliveries WHERE id=$1`, deliveryID, now); err != nil {
			return store.OutboxPublishStart{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return store.OutboxPublishStart{}, err
		}
		return store.OutboxPublishStart{Attempt: attempts, Exhausted: true}, nil
	}
	attempts++
	if _, err := tx.Exec(ctx, `UPDATE outbox SET attempts=$2,updated_at=GREATEST(updated_at,$3) WHERE id=$1`, outboxID, attempts, now); err != nil {
		return store.OutboxPublishStart{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.OutboxPublishStart{}, err
	}
	return store.OutboxPublishStart{Attempt: attempts}, nil
}

func (client *Client) MarkOutboxDispatched(ctx context.Context, outboxID, claimToken string, now time.Time) error {
	if outboxID == "" || claimToken == "" {
		return store.ErrConflict
	}
	tx, err := client.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var deliveryID string
	err = tx.QueryRow(ctx, `UPDATE outbox SET dispatched_at=$3,claimed_at=NULL,claim_token=NULL,last_error=NULL,updated_at=$3 WHERE id=$1 AND claim_token=$2 AND dispatched_at IS NULL AND failed_at IS NULL RETURNING delivery_id`, outboxID, claimToken, now).Scan(&deliveryID)
	if errors.Is(err, pgx.ErrNoRows) {
		var dispatched bool
		if scanErr := tx.QueryRow(ctx, `SELECT dispatched_at IS NOT NULL FROM outbox WHERE id=$1`, outboxID).Scan(&dispatched); errors.Is(scanErr, pgx.ErrNoRows) {
			return store.ErrNotFound
		} else if scanErr != nil {
			return scanErr
		}
		if dispatched {
			return tx.Commit(ctx)
		}
		return store.ErrConflict
	}
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE deliveries SET status='dispatched',updated_at=GREATEST(updated_at,$2) WHERE id=$1 AND status NOT IN ('acked','dead_letter')`, deliveryID, now); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO delivery_lifecycle(delivery_id,generation,type,outcome,occurred_at) SELECT id,generation,'outbox.dispatched','dispatched',$2 FROM deliveries WHERE id=$1`, deliveryID, now); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (client *Client) RetryOutbox(ctx context.Context, outboxID, claimToken string, availableAt time.Time, reason string) error {
	if outboxID == "" || claimToken == "" {
		return store.ErrConflict
	}
	reason = safeOutboxReason(reason)
	tx, err := client.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var deliveryID string
	err = tx.QueryRow(ctx, `UPDATE outbox SET available_at=$3,claimed_at=NULL,claim_token=NULL,last_error=$4,updated_at=clock_timestamp() WHERE id=$1 AND claim_token=$2 AND dispatched_at IS NULL AND failed_at IS NULL RETURNING delivery_id`, outboxID, claimToken, availableAt, reason).Scan(&deliveryID)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.ErrConflict
	}
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE deliveries SET status='retrying',updated_at=clock_timestamp() WHERE id=$1 AND status NOT IN ('acked','dead_letter')`, deliveryID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (client *Client) FailOutbox(ctx context.Context, outboxID, claimToken string, now time.Time, reason string) error {
	if outboxID == "" || claimToken == "" {
		return store.ErrConflict
	}
	reason = safeOutboxReason(reason)
	tx, err := client.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var deliveryID string
	err = tx.QueryRow(ctx, `UPDATE outbox SET failed_at=$3,claimed_at=NULL,claim_token=NULL,last_error=$4,updated_at=$3 WHERE id=$1 AND claim_token=$2 AND dispatched_at IS NULL AND failed_at IS NULL RETURNING delivery_id`, outboxID, claimToken, now, reason).Scan(&deliveryID)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.ErrConflict
	}
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE deliveries SET status='dead_letter',assigned_connection_id=NULL,assignment_token=NULL,assignment_expires_at=NULL,updated_at=GREATEST(updated_at,$2) WHERE id=$1 AND status!='acked'`, deliveryID, now); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO delivery_lifecycle(delivery_id,generation,type,outcome,reason,occurred_at) SELECT id,generation,'delivery.dead_lettered','dead_letter',$2,$3 FROM deliveries WHERE id=$1`, deliveryID, reason, now); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (client *Client) OutboxStats(ctx context.Context) (store.OutboxStats, error) {
	var stats store.OutboxStats
	err := client.pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE failed_at IS NULL),count(*) FILTER (WHERE failed_at IS NULL AND claimed_at IS NOT NULL),count(*) FILTER (WHERE failed_at IS NOT NULL),min(created_at) FILTER (WHERE failed_at IS NULL) FROM outbox WHERE dispatched_at IS NULL`).Scan(&stats.Pending, &stats.Claimed, &stats.Failed, &stats.OldestPendingAt)
	return stats, err
}

func safeOutboxReason(reason string) string {
	switch strings.ToLower(strings.TrimSpace(reason)) {
	case "broker_unavailable", "publish_timeout", "publish_rejected", "store_error":
		return strings.ToLower(strings.TrimSpace(reason))
	default:
		return "publish_error"
	}
}
