package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/jackc/pgx/v5"
)

var _ store.DeliveryAssignmentStore = (*Client)(nil)

func (client *Client) AssignStreamDelivery(ctx context.Context, deliveryID, targetAppID, connectionID, token string, generation int64, now time.Time, lease, maxProcessing time.Duration) (store.DeliveryAssignment, store.DeliveryAssignmentDisposition, error) {
	if deliveryID == "" || targetAppID == "" || connectionID == "" || token == "" || generation < 1 || lease <= 0 || maxProcessing < lease {
		return store.DeliveryAssignment{}, "", store.ErrConflict
	}
	tx, err := client.pool.Begin(ctx)
	if err != nil {
		return store.DeliveryAssignment{}, "", err
	}
	defer tx.Rollback(ctx)
	var storedTarget, sink, status string
	var assignedConnection, assignedToken *string
	var assignedUntil *time.Time
	var attempts int
	var storedGeneration int64
	err = tx.QueryRow(ctx, `SELECT target_app_id,sink,status,assigned_connection_id,assignment_token,assignment_expires_at,attempts,generation FROM deliveries WHERE id=$1 FOR UPDATE`, deliveryID).Scan(&storedTarget, &sink, &status, &assignedConnection, &assignedToken, &assignedUntil, &attempts, &storedGeneration)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && (storedTarget != targetAppID || sink != "stream") {
		return store.DeliveryAssignment{}, "", store.ErrNotFound
	}
	if err != nil {
		return store.DeliveryAssignment{}, "", err
	}
	if storedGeneration != generation {
		return store.DeliveryAssignment{}, "", store.ErrConflict
	}
	if status == "acked" || status == "dead_letter" {
		if err := tx.Commit(ctx); err != nil {
			return store.DeliveryAssignment{}, "", err
		}
		return store.DeliveryAssignment{}, store.DeliveryAlreadyComplete, nil
	}
	if assignedUntil != nil && assignedUntil.After(now) {
		if err := tx.Commit(ctx); err != nil {
			return store.DeliveryAssignment{}, "", err
		}
		return store.DeliveryAssignment{}, store.DeliveryAlreadyAssigned, nil
	}
	expiresAt := now.Add(lease)
	maxExpiresAt := now.Add(maxProcessing)
	attempts++
	_, err = tx.Exec(ctx, `UPDATE deliveries SET assigned_connection_id=$2,assignment_token=$3,assignment_started_at=$4,assignment_expires_at=$5,assignment_max_expires_at=$6,attempts=$7,updated_at=GREATEST(updated_at,$8) WHERE id=$1 AND generation=$9`, deliveryID, connectionID, token, now, expiresAt, maxExpiresAt, attempts, now, generation)
	if err != nil {
		return store.DeliveryAssignment{}, "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.DeliveryAssignment{}, "", err
	}
	return store.DeliveryAssignment{DeliveryID: deliveryID, TargetAppID: targetAppID, ConnectionID: connectionID, Token: token, Attempt: attempts, Generation: generation, ExpiresAt: expiresAt}, store.DeliveryAssigned, nil
}

func (client *Client) AcknowledgeStreamDelivery(ctx context.Context, deliveryID, targetAppID, connectionID, token string, generation int64, now time.Time) error {
	result, err := client.pool.Exec(ctx, `UPDATE deliveries SET status='acked',updated_at=GREATEST(updated_at,$6) WHERE id=$1 AND target_app_id=$2 AND sink='stream' AND assigned_connection_id=$3 AND assignment_token=$4 AND generation=$5 AND assignment_expires_at>$6 AND status NOT IN ('acked','dead_letter')`, deliveryID, targetAppID, connectionID, token, generation, now)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 1 {
		return nil
	}
	var storedTarget, sink, status string
	var storedConnection, storedToken *string
	var storedGeneration int64
	err = client.pool.QueryRow(ctx, `SELECT target_app_id,sink,status,assigned_connection_id,assignment_token,generation FROM deliveries WHERE id=$1`, deliveryID).Scan(&storedTarget, &sink, &status, &storedConnection, &storedToken, &storedGeneration)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && (storedTarget != targetAppID || sink != "stream") {
		return store.ErrNotFound
	}
	if err != nil {
		return err
	}
	if status == "acked" && storedGeneration == generation && storedConnection != nil && storedToken != nil && *storedConnection == connectionID && *storedToken == token {
		return nil
	}
	return store.ErrConflict
}

func (client *Client) ReleaseStreamDelivery(ctx context.Context, deliveryID, targetAppID, connectionID, token string, generation int64, now time.Time) error {
	result, err := client.pool.Exec(ctx, `UPDATE deliveries SET status='retrying',assigned_connection_id=NULL,assignment_token=NULL,assignment_started_at=NULL,assignment_expires_at=NULL,assignment_max_expires_at=NULL,updated_at=GREATEST(updated_at,$6) WHERE id=$1 AND target_app_id=$2 AND sink='stream' AND assigned_connection_id=$3 AND assignment_token=$4 AND generation=$5 AND assignment_expires_at>$6 AND status NOT IN ('acked','dead_letter')`, deliveryID, targetAppID, connectionID, token, generation, now)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 1 {
		return nil
	}
	return client.streamAssignmentError(ctx, deliveryID, targetAppID)
}

func (client *Client) ProgressStreamDelivery(ctx context.Context, deliveryID, targetAppID, connectionID, token string, generation int64, now time.Time, lease time.Duration) error {
	if lease <= 0 {
		return store.ErrConflict
	}
	result, err := client.pool.Exec(ctx, `UPDATE deliveries SET assignment_expires_at=LEAST($7,assignment_max_expires_at),updated_at=GREATEST(updated_at,$6) WHERE id=$1 AND target_app_id=$2 AND sink='stream' AND assigned_connection_id=$3 AND assignment_token=$4 AND generation=$5 AND assignment_expires_at>$6 AND assignment_max_expires_at>$6 AND status NOT IN ('acked','dead_letter')`, deliveryID, targetAppID, connectionID, token, generation, now, now.Add(lease))
	if err != nil {
		return err
	}
	if result.RowsAffected() == 1 {
		return nil
	}
	return client.streamAssignmentError(ctx, deliveryID, targetAppID)
}

func (client *Client) streamAssignmentError(ctx context.Context, deliveryID, targetAppID string) error {
	var storedTarget, sink string
	err := client.pool.QueryRow(ctx, `SELECT target_app_id,sink FROM deliveries WHERE id=$1`, deliveryID).Scan(&storedTarget, &sink)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && (storedTarget != targetAppID || sink != "stream") {
		return store.ErrNotFound
	}
	if err != nil {
		return err
	}
	return store.ErrConflict
}
