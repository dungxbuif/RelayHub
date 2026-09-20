package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/jackc/pgx/v5"
)

func insertQueueResultCallback(ctx context.Context, tx pgx.Tx, subscription domain.QueueSubscription, deliveryID, eventID string, generation int64, attempts int, status string, now time.Time) error {
	outcome := "success"
	callbackURL := subscription.SuccessCallbackURL
	if status == "dead_letter" {
		outcome, callbackURL = "failure", subscription.FailureCallbackURL
	}
	if callbackURL == nil {
		return nil
	}
	payload, err := json.Marshal(map[string]any{"app_id": subscription.AppID, "subscription_id": subscription.ID, "delivery_id": deliveryID, "event_id": eventID, "generation": generation, "outcome": outcome, "attempt_count": attempts, "metadata": json.RawMessage(subscription.ResultCallbackMetadata)})
	if err != nil {
		return err
	}
	digest := sha256.Sum256([]byte(deliveryID + "\x00" + outcome + "\x00" + time.Unix(generation, 0).UTC().String()))
	id := "qcb_" + hex.EncodeToString(digest[:16])
	_, err = tx.Exec(ctx, `INSERT INTO queue_result_callbacks(id,app_id,subscription_id,delivery_id,event_id,generation,outcome,url,payload,available_at,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$10,$10) ON CONFLICT(delivery_id,generation,outcome) DO NOTHING`, id, subscription.AppID, subscription.ID, deliveryID, eventID, generation, outcome, *callbackURL, string(payload), now)
	return err
}

func (client *Client) ClaimQueueResultCallback(ctx context.Context, now, claimUntil time.Time, token string) (domain.QueueResultCallback, error) {
	tx, err := client.pool.Begin(ctx)
	if err != nil {
		return domain.QueueResultCallback{}, err
	}
	defer tx.Rollback(ctx)
	var result domain.QueueResultCallback
	var payload, encryptedSecret string
	err = tx.QueryRow(ctx, `WITH candidate AS (SELECT id FROM queue_result_callbacks WHERE status='pending' AND attempts<6 AND available_at<=$1 AND (claim_expires_at IS NULL OR claim_expires_at<=$1) ORDER BY available_at,id FOR UPDATE SKIP LOCKED LIMIT 1), claimed AS (UPDATE queue_result_callbacks q SET claim_token=$3,claim_expires_at=$2,claim_generation=q.claim_generation+1,attempts=q.attempts+1,updated_at=$1 FROM candidate WHERE q.id=candidate.id RETURNING q.*) SELECT c.id,c.app_id,c.subscription_id,c.delivery_id,c.event_id,c.generation,c.outcome,c.url,c.payload::text,c.attempts,c.claim_token,c.claim_generation,c.claim_expires_at,k.encrypted_hmac_secret FROM claimed c JOIN application_credentials k ON k.app_id=c.app_id AND k.revoked_at IS NULL`, now, claimUntil, token).Scan(&result.ID, &result.AppID, &result.SubscriptionID, &result.DeliveryID, &result.EventID, &result.Generation, &result.Outcome, &result.URL, &payload, &result.Attempt, &result.ClaimToken, &result.ClaimGeneration, &result.ClaimExpiresAt, &encryptedSecret)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, store.ErrNotFound
	}
	if err != nil {
		return result, err
	}
	result.Body = []byte(payload)
	result.Secret, err = client.cipher.Decrypt(encryptedSecret)
	if err != nil {
		return domain.QueueResultCallback{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO queue_result_callback_attempts(callback_id,attempt,claim_generation,outcome,occurred_at) VALUES($1,$2,$3,'claimed',$4)`, result.ID, result.Attempt, result.ClaimGeneration, now); err != nil {
		return domain.QueueResultCallback{}, err
	}
	return result, tx.Commit(ctx)
}

func (client *Client) FinishQueueResultCallback(ctx context.Context, transition store.QueueResultCallbackTransition) error {
	tx, err := client.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	result, err := tx.Exec(ctx, `UPDATE queue_result_callbacks SET status=$5,available_at=CASE WHEN $5='pending' THEN $6 ELSE available_at END,claim_token=NULL,claim_expires_at=NULL,last_reason=NULLIF($7,''),updated_at=$8 WHERE id=$1 AND attempts=$2 AND claim_token=$3 AND claim_generation=$4 AND status='pending'`, transition.CallbackID, transition.Attempt, transition.ClaimToken, transition.ClaimGeneration, transition.Status, transition.RetryAt, transition.Reason, transition.Now)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return store.ErrConflict
	}
	if _, err = tx.Exec(ctx, `UPDATE queue_result_callback_attempts SET outcome=$3,reason=NULLIF($4,'') WHERE callback_id=$1 AND attempt=$2`, transition.CallbackID, transition.Attempt, transition.Status, transition.Reason); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (client *Client) ListQueueResultCallbacks(ctx context.Context, appID, subscriptionID string, limit int) ([]domain.QueueCallbackOutcome, error) {
	rows, err := client.pool.Query(ctx, `SELECT id,subscription_id,delivery_id,event_id,generation,outcome,status,attempts,COALESCE(last_reason,''),created_at,updated_at FROM queue_result_callbacks WHERE app_id=$1 AND subscription_id=$2 ORDER BY created_at DESC,id DESC LIMIT $3`, appID, subscriptionID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []domain.QueueCallbackOutcome{}
	for rows.Next() {
		var item domain.QueueCallbackOutcome
		if err := rows.Scan(&item.ID, &item.SubscriptionID, &item.DeliveryID, &item.EventID, &item.Generation, &item.Outcome, &item.Status, &item.Attempts, &item.Reason, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}
