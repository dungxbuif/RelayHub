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

var _ store.CallbackAttemptStore = (*Client)(nil)

func (client *Client) BeginCallbackAttempt(ctx context.Context, deliveryID, token string, now time.Time, lease time.Duration) (store.CallbackDispatch, store.CallbackDispatchDisposition, error) {
	if deliveryID == "" || token == "" || lease <= store.CallbackFinishMargin {
		return store.CallbackDispatch{}, "", store.ErrConflict
	}
	now = postgresTime(now)
	tx, err := client.pool.Begin(ctx)
	if err != nil {
		return store.CallbackDispatch{}, "", err
	}
	defer tx.Rollback(ctx)

	var result store.CallbackDispatch
	var status string
	var outboxPayload []byte
	var callbackURL *string
	var encryptedSecret *string
	var credentialVersion *int
	var activeToken, reason *string
	var leaseExpires, retryAt, dlqPublishedAt *time.Time
	var eventExpires time.Time
	err = tx.QueryRow(ctx, `
		SELECT d.id,d.public_job_id,d.target_app_id,d.status,d.attempts,d.generation,d.callback_token,
		       d.callback_expires_at,d.callback_retry_at,d.callback_reason,d.callback_dlq_published_at,
		       a.id,a.name,a.callback_url,a.delivery_mode,a.enabled,a.created_at,a.updated_at,
		       o.payload,e.expires_at,
		       c.encrypted_hmac_secret,c.version
		FROM deliveries d
		JOIN applications a ON a.id=d.target_app_id
		JOIN events e ON e.id=d.event_id
		JOIN outbox o ON o.delivery_id=d.id
		LEFT JOIN application_credentials c ON c.app_id=a.id AND c.revoked_at IS NULL
		WHERE d.id=$1 AND d.sink='callback'
		FOR UPDATE OF d`, deliveryID).Scan(
		&result.DeliveryID, &result.PublicJobID, &result.TargetAppID, &status, &result.Attempt, &result.Generation, &activeToken,
		&leaseExpires, &retryAt, &reason, &dlqPublishedAt,
		&result.App.ID, &result.App.Name, &callbackURL, &result.App.DeliveryMode, &result.App.Enabled, &result.App.CreatedAt, &result.App.UpdatedAt,
		&outboxPayload, &eventExpires,
		&encryptedSecret, &credentialVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.CallbackDispatch{}, "", store.ErrNotFound
	}
	if err != nil {
		return store.CallbackDispatch{}, "", err
	}
	result.App.CallbackURL = callbackURL
	var persisted struct {
		Event json.RawMessage `json:"event"`
	}
	if err := json.Unmarshal(outboxPayload, &persisted); err != nil || len(persisted.Event) == 0 {
		return store.CallbackDispatch{}, "", store.ErrConflict
	}
	result.Body = append([]byte(nil), persisted.Event...)
	if err := json.Unmarshal(result.Body, &result.Event); err != nil {
		return store.CallbackDispatch{}, "", store.ErrConflict
	}
	if activeToken != nil {
		result.Token = *activeToken
	}
	if leaseExpires != nil {
		result.LeaseExpiresAt = *leaseExpires
	}
	if retryAt != nil {
		result.RetryAt = *retryAt
	}
	if reason != nil {
		result.Reason = *reason
	}
	result.DLQPublished = dlqPublishedAt != nil

	if status == "acked" || status == "delivered" {
		return result, store.CallbackDispatchComplete, tx.Commit(ctx)
	}
	if status == "dead_letter" {
		return result, store.CallbackDispatchDeadLetter, tx.Commit(ctx)
	}
	if !result.RetryAt.IsZero() && result.RetryAt.After(now) {
		return result, store.CallbackDispatchBusy, tx.Commit(ctx)
	}
	if result.Token != "" && result.LeaseExpiresAt.After(now) {
		result.RetryAt = result.LeaseExpiresAt
		return result, store.CallbackDispatchBusy, tx.Commit(ctx)
	}
	if !eventExpires.After(now) || result.Attempt >= 6 {
		reason := "event_expired"
		if result.Attempt >= 6 {
			reason = "attempts_exhausted"
		}
		_, err = tx.Exec(ctx, `UPDATE deliveries SET status='dead_letter',callback_reason=$2,callback_token=NULL,callback_expires_at=NULL,updated_at=$3 WHERE id=$1`, deliveryID, reason, now)
		result.Reason = reason
		if err == nil {
			err = tx.Commit(ctx)
		}
		return result, store.CallbackDispatchDeadLetter, err
	}
	eligible := result.App.Enabled && callbackURL != nil && *callbackURL != "" && (result.App.DeliveryMode == domain.DeliveryCallback || result.App.DeliveryMode == domain.DeliveryAll) && encryptedSecret != nil && credentialVersion != nil
	if !eligible {
		_, err = tx.Exec(ctx, `UPDATE deliveries SET status='dead_letter',callback_reason='callback_disabled',callback_token=NULL,callback_expires_at=NULL,updated_at=$2 WHERE id=$1`, deliveryID, now)
		result.Reason = "callback_disabled"
		if err == nil {
			err = tx.Commit(ctx)
		}
		return result, store.CallbackDispatchDeadLetter, err
	}
	result.Attempt++
	result.Token = token
	result.LeaseExpiresAt = postgresTime(now.Add(lease))
	result.RetryAt = time.Time{}
	result.CredentialVersion = *credentialVersion
	result.Secret, err = client.cipher.Decrypt(*encryptedSecret)
	if err != nil {
		return store.CallbackDispatch{}, "", err
	}
	command, err := tx.Exec(ctx, `UPDATE deliveries SET status='dispatched',attempts=$2,callback_token=$3,callback_expires_at=$4,callback_retry_at=NULL,callback_url=$5,callback_credential_version=$6,callback_reason=NULL,updated_at=$7 WHERE id=$1`, deliveryID, result.Attempt, token, result.LeaseExpiresAt, *callbackURL, *credentialVersion, now)
	if err != nil || command.RowsAffected() != 1 {
		return store.CallbackDispatch{}, "", err
	}
	_, err = tx.Exec(ctx, `INSERT INTO delivery_attempts(delivery_id,generation,attempt,outcome,reason,created_at,updated_at,callback_token) VALUES($1,$2,$3,'started',NULL,$4,$4,$5) ON CONFLICT(delivery_id,generation,attempt) DO NOTHING`, deliveryID, result.Generation, result.Attempt, now, token)
	if err == nil {
		err = tx.Commit(ctx)
	}
	return result, store.CallbackDispatchReady, err
}

func (client *Client) FinishCallbackAttempt(ctx context.Context, deliveryID, token string, attempt int, transition store.CallbackAttemptTransition) error {
	if transition.Status != domain.JobDelivered && transition.Status != domain.JobPending && transition.Status != domain.JobDeadLetter {
		return store.ErrConflict
	}
	if transition.Status == domain.JobPending && transition.RetryAt.IsZero() {
		return store.ErrConflict
	}
	transition.Now = postgresTime(transition.Now)
	if !transition.RetryAt.IsZero() {
		transition.RetryAt = postgresTime(transition.RetryAt)
	}
	tx, err := client.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	status := "delivered"
	if transition.Status == domain.JobPending {
		status = "retrying"
	}
	if transition.Status == domain.JobDeadLetter {
		status = "dead_letter"
	}
	command, err := tx.Exec(ctx, `UPDATE deliveries SET status=$5,callback_retry_at=$6,callback_reason=$7,callback_token=NULL,callback_expires_at=NULL,updated_at=$8 WHERE id=$1 AND sink='callback' AND callback_token=$2 AND attempts=$3 AND callback_expires_at>$4`, deliveryID, token, attempt, transition.Now, status, nullableTime(transition.RetryAt), transition.Reason, transition.Now)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		var currentStatus, outcome, currentToken string
		var currentRetry *time.Time
		var currentReason *string
		err = tx.QueryRow(ctx, `SELECT d.status,d.callback_retry_at,a.outcome,a.reason,a.callback_token FROM deliveries d JOIN delivery_attempts a ON a.delivery_id=d.id AND a.attempt=$2 WHERE d.id=$1 AND d.attempts=$2`, deliveryID, attempt).Scan(&currentStatus, &currentRetry, &outcome, &currentReason, &currentToken)
		if err != nil || currentStatus != status || outcome != status || currentToken != token || stringValue(currentReason) != transition.Reason || !sameOptionalTime(currentRetry, transition.RetryAt) {
			return store.ErrConflict
		}
		return tx.Commit(ctx)
	}
	command, err = tx.Exec(ctx, `UPDATE delivery_attempts SET outcome=$3,reason=$4,updated_at=$5 WHERE delivery_id=$1 AND attempt=$2`, deliveryID, attempt, status, transition.Reason, transition.Now)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return store.ErrConflict
	}
	return tx.Commit(ctx)
}

func (client *Client) MarkCallbackDLQPublished(ctx context.Context, deliveryID string, now time.Time) error {
	now = postgresTime(now)
	command, err := client.pool.Exec(ctx, `UPDATE deliveries SET callback_dlq_published_at=COALESCE(callback_dlq_published_at,$2),updated_at=GREATEST(updated_at,$2) WHERE id=$1 AND sink='callback' AND status='dead_letter'`, deliveryID, now)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return store.ErrConflict
	}
	return nil
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}
func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
func sameOptionalTime(value *time.Time, expected time.Time) bool {
	if value == nil {
		return expected.IsZero()
	}
	return value.Equal(postgresTime(expected))
}

func postgresTime(value time.Time) time.Time { return value.UTC().Truncate(time.Microsecond) }
