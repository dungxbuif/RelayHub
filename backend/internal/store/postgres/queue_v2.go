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

var _ store.QueueRepository = (*Client)(nil)

const queueSubscriptionColumns = `id,app_id,name,enabled,paused_at,draining_at,drain_deadline_at,drained_at,event_types,max_attempts,default_visibility_seconds,max_visibility_seconds,max_total_lease_seconds,retention_seconds,max_in_flight,max_batch_size,retry_delay_seconds,ordering_mode,deduplication_seconds,max_dispatch_rate,success_callback_url,failure_callback_url,result_callback_metadata::text,policy_version,created_at,updated_at`

type queueScanner interface{ Scan(...any) error }

func scanQueueSubscription(row queueScanner) (domain.QueueSubscription, error) {
	var item domain.QueueSubscription
	var resultMetadata string
	err := row.Scan(&item.ID, &item.AppID, &item.Name, &item.Enabled, &item.PausedAt, &item.DrainingAt, &item.DrainDeadlineAt, &item.DrainedAt, &item.EventTypes, &item.MaxAttempts, &item.DefaultVisibilitySeconds, &item.MaxVisibilitySeconds, &item.MaxTotalLeaseSeconds, &item.RetentionSeconds, &item.MaxInFlight, &item.MaxBatchSize, &item.RetryDelaySeconds, &item.OrderingMode, &item.DeduplicationSeconds, &item.MaxDispatchRate, &item.SuccessCallbackURL, &item.FailureCallbackURL, &resultMetadata, &item.PolicyVersion, &item.CreatedAt, &item.UpdatedAt)
	item.ResultCallbackMetadata = json.RawMessage(resultMetadata)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.QueueSubscription{}, store.ErrNotFound
	}
	return item, err
}

func (client *Client) CreateQueueSubscription(ctx context.Context, item domain.QueueSubscription) error {
	if item.EventTypes == nil {
		item.EventTypes = []string{}
	}
	if len(item.ResultCallbackMetadata) == 0 {
		item.ResultCallbackMetadata = json.RawMessage(`{}`)
	}
	_, err := client.pool.Exec(ctx, `INSERT INTO queue_subscriptions(id,app_id,name,enabled,paused_at,draining_at,drain_deadline_at,drained_at,event_types,max_attempts,default_visibility_seconds,max_visibility_seconds,max_total_lease_seconds,retention_seconds,max_in_flight,max_batch_size,retry_delay_seconds,ordering_mode,deduplication_seconds,max_dispatch_rate,success_callback_url,failure_callback_url,result_callback_metadata,policy_version,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23::jsonb,$24,$25,$26)`, item.ID, item.AppID, item.Name, item.Enabled, item.PausedAt, item.DrainingAt, item.DrainDeadlineAt, item.DrainedAt, item.EventTypes, item.MaxAttempts, item.DefaultVisibilitySeconds, item.MaxVisibilitySeconds, item.MaxTotalLeaseSeconds, item.RetentionSeconds, item.MaxInFlight, item.MaxBatchSize, item.RetryDelaySeconds, item.OrderingMode, item.DeduplicationSeconds, item.MaxDispatchRate, item.SuccessCallbackURL, item.FailureCallbackURL, string(item.ResultCallbackMetadata), item.PolicyVersion, item.CreatedAt, item.UpdatedAt)
	if uniqueViolation(err) {
		return store.ErrConflict
	}
	return err
}

func (client *Client) ListQueueSubscriptions(ctx context.Context, appID string) ([]domain.QueueSubscription, error) {
	rows, err := client.pool.Query(ctx, `SELECT `+queueSubscriptionColumns+` FROM queue_subscriptions WHERE app_id=$1 ORDER BY created_at,id`, appID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []domain.QueueSubscription{}
	for rows.Next() {
		item, err := scanQueueSubscription(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (client *Client) GetQueueSubscription(ctx context.Context, appID, id string) (domain.QueueSubscription, error) {
	return scanQueueSubscription(client.pool.QueryRow(ctx, `SELECT `+queueSubscriptionColumns+` FROM queue_subscriptions WHERE app_id=$1 AND id=$2`, appID, id))
}

func (client *Client) UpdateQueueSubscription(ctx context.Context, item domain.QueueSubscription, expectedVersion int64) (domain.QueueSubscription, error) {
	if item.EventTypes == nil {
		item.EventTypes = []string{}
	}
	if len(item.ResultCallbackMetadata) == 0 {
		item.ResultCallbackMetadata = json.RawMessage(`{}`)
	}
	row := client.pool.QueryRow(ctx, `UPDATE queue_subscriptions SET name=$3,enabled=$4,paused_at=$5,event_types=$6,max_attempts=$7,default_visibility_seconds=$8,max_visibility_seconds=$9,max_total_lease_seconds=$10,retention_seconds=$11,max_in_flight=$12,max_batch_size=$13,retry_delay_seconds=$14,ordering_mode=$15,deduplication_seconds=$16,max_dispatch_rate=$17,success_callback_url=$18,failure_callback_url=$19,result_callback_metadata=$20::jsonb,policy_version=policy_version+1,updated_at=$21 WHERE app_id=$1 AND id=$2 AND policy_version=$22 RETURNING `+queueSubscriptionColumns, item.AppID, item.ID, item.Name, item.Enabled, item.PausedAt, item.EventTypes, item.MaxAttempts, item.DefaultVisibilitySeconds, item.MaxVisibilitySeconds, item.MaxTotalLeaseSeconds, item.RetentionSeconds, item.MaxInFlight, item.MaxBatchSize, item.RetryDelaySeconds, item.OrderingMode, item.DeduplicationSeconds, item.MaxDispatchRate, item.SuccessCallbackURL, item.FailureCallbackURL, string(item.ResultCallbackMetadata), item.UpdatedAt, expectedVersion)
	updated, err := scanQueueSubscription(row)
	if errors.Is(err, store.ErrNotFound) {
		if _, getErr := client.GetQueueSubscription(ctx, item.AppID, item.ID); getErr == nil {
			return domain.QueueSubscription{}, store.ErrConflict
		}
	}
	if uniqueViolation(err) {
		return domain.QueueSubscription{}, store.ErrConflict
	}
	return updated, err
}

func (client *Client) DeleteQueueSubscription(ctx context.Context, appID, id string) error {
	result, err := client.pool.Exec(ctx, `DELETE FROM queue_subscriptions WHERE app_id=$1 AND id=$2`, appID, id)
	if err == nil && result.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return err
}

type queuePullRow struct {
	id, eventID, eventType, sourceAppID string
	targetAppIDs                        []string
	data, metadata                      []byte
	createdAt                           time.Time
	orderingKey                         *string
	priority                            int
	generation                          int64
	attempt                             int
}

func (client *Client) PullQueueDeliveries(ctx context.Context, request store.QueuePullRequest) ([]domain.QueueDelivery, error) {
	tx, err := client.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	subscription, err := scanQueueSubscription(tx.QueryRow(ctx, `SELECT `+queueSubscriptionColumns+` FROM queue_subscriptions WHERE app_id=$1 AND id=$2 FOR UPDATE`, request.AppID, request.SubscriptionID))
	if err != nil {
		return nil, err
	}
	if subscription.DrainingAt != nil {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return []domain.QueueDelivery{}, nil
	}
	if !subscription.Enabled || subscription.PausedAt != nil {
		return nil, store.ErrConflict
	}
	if request.Limit > subscription.MaxBatchSize {
		request.Limit = subscription.MaxBatchSize
	}
	visibility := request.Visibility
	if visibility <= 0 {
		visibility = time.Duration(subscription.DefaultVisibilitySeconds) * time.Second
	}
	maxVisibility := time.Duration(subscription.MaxVisibilitySeconds) * time.Second
	if visibility > maxVisibility {
		visibility = maxVisibility
	}
	// Reclaiming happens under the subscription lock, so exactly one puller can
	// transition an expired receipt before new work is selected.
	if _, err := tx.Exec(ctx, `WITH reclaimed AS (UPDATE queue_deliveries SET status=CASE WHEN attempts >= $3 THEN 'dead_letter' ELSE 'available' END,available_at=CASE WHEN attempts >= $3 THEN available_at ELSE lease_expires_at+make_interval(secs=>$4) END,receipt_hash=NULL,lease_started_at=NULL,lease_expires_at=NULL,lease_max_expires_at=NULL,last_reason=CASE WHEN attempts >= $3 THEN 'max_attempts_exhausted' ELSE 'visibility_timeout' END,updated_at=$2 WHERE subscription_id=$1 AND status='in_flight' AND lease_expires_at<=$2 RETURNING id,generation,attempts,status,last_reason) INSERT INTO queue_delivery_attempts(delivery_id,generation,attempt,outcome,reason,occurred_at) SELECT id,generation,attempts,CASE WHEN status='dead_letter' THEN 'dead_letter' ELSE 'visibility_timeout' END,last_reason,$2 FROM reclaimed ON CONFLICT DO NOTHING`, request.SubscriptionID, request.Now, subscription.MaxAttempts, subscription.RetryDelaySeconds); err != nil {
		return nil, err
	}
	if subscription.FailureCallbackURL != nil {
		if _, err := tx.Exec(ctx, `INSERT INTO queue_result_callbacks(id,app_id,subscription_id,delivery_id,event_id,generation,outcome,url,payload,available_at,created_at,updated_at) SELECT 'qcb_'||md5(d.id||':'||d.generation::text||':failure'),d.app_id,d.subscription_id,d.id,d.event_id,d.generation,'failure',$3,jsonb_build_object('app_id',d.app_id,'subscription_id',d.subscription_id,'delivery_id',d.id,'event_id',d.event_id,'generation',d.generation,'outcome','failure','attempt_count',d.attempts,'metadata',$4::jsonb),$2,$2,$2 FROM queue_deliveries d WHERE d.subscription_id=$1 AND d.status='dead_letter' AND d.updated_at=$2 AND d.last_reason='max_attempts_exhausted' ON CONFLICT(delivery_id,generation,outcome) DO NOTHING`, request.SubscriptionID, request.Now, *subscription.FailureCallbackURL, string(subscription.ResultCallbackMetadata)); err != nil {
			return nil, err
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE queue_deliveries SET status='dead_letter',last_reason='retention_expired',updated_at=$2 WHERE subscription_id=$1 AND status='available' AND expires_at<=$2`, request.SubscriptionID, request.Now); err != nil {
		return nil, err
	}
	var inFlight int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM queue_deliveries WHERE subscription_id=$1 AND status='in_flight'`, request.SubscriptionID).Scan(&inFlight); err != nil {
		return nil, err
	}
	capacity := subscription.MaxInFlight - inFlight
	if capacity <= 0 {
		return []domain.QueueDelivery{}, tx.Commit(ctx)
	}
	if request.Limit > capacity {
		request.Limit = capacity
	}
	if subscription.MaxDispatchRate != nil {
		var recentlyDispatched int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM queue_delivery_attempts a JOIN queue_deliveries d ON d.id=a.delivery_id WHERE d.subscription_id=$1 AND a.outcome='leased' AND a.occurred_at>$2::timestamptz-interval '1 second'`, request.SubscriptionID, request.Now).Scan(&recentlyDispatched); err != nil {
			return nil, err
		}
		rateCapacity := *subscription.MaxDispatchRate - recentlyDispatched
		if rateCapacity <= 0 {
			return []domain.QueueDelivery{}, tx.Commit(ctx)
		}
		if request.Limit > rateCapacity {
			request.Limit = rateCapacity
		}
	}
	rows, err := tx.Query(ctx, `SELECT d.id,d.event_id,e.type,e.source_app_id,e.target_app_ids,e.data::text,e.created_at,d.ordering_key,d.priority,d.generation,d.attempts,d.metadata::text FROM queue_deliveries d JOIN events e ON e.id=d.event_id WHERE d.subscription_id=$1 AND d.status='available' AND d.available_at<=$2 AND d.expires_at>$2 AND ($3::text='none' OR d.ordering_key IS NULL OR NOT EXISTS (SELECT 1 FROM queue_deliveries earlier WHERE earlier.subscription_id=d.subscription_id AND earlier.ordering_key=d.ordering_key AND earlier.status IN ('available','in_flight') AND (earlier.created_at,earlier.id)<(d.created_at,d.id))) ORDER BY LEAST(10,d.priority+FLOOR(EXTRACT(EPOCH FROM ($2-d.available_at))/60)::integer) DESC,d.available_at,d.created_at,d.id FOR UPDATE OF d SKIP LOCKED LIMIT $4`, request.SubscriptionID, request.Now, subscription.OrderingMode, request.Limit)
	if err != nil {
		return nil, err
	}
	selected := []queuePullRow{}
	for rows.Next() {
		var item queuePullRow
		if err := rows.Scan(&item.id, &item.eventID, &item.eventType, &item.sourceAppID, &item.targetAppIDs, &item.data, &item.createdAt, &item.orderingKey, &item.priority, &item.generation, &item.attempt, &item.metadata); err != nil {
			rows.Close()
			return nil, err
		}
		selected = append(selected, item)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	result := make([]domain.QueueDelivery, 0, len(selected))
	for index, item := range selected {
		if index >= len(request.Receipts) {
			return nil, store.ErrConflict
		}
		receipt := request.Receipts[index]
		hash := queueReceiptHash(receipt)
		leaseExpires := request.Now.Add(visibility)
		leaseMax := request.Now.Add(time.Duration(subscription.MaxTotalLeaseSeconds) * time.Second)
		attempt := item.attempt + 1
		if _, err := tx.Exec(ctx, `UPDATE queue_deliveries SET status='in_flight',attempts=$2,receipt_hash=$3,lease_started_at=$4,lease_expires_at=$5,lease_max_expires_at=$6,updated_at=$4 WHERE id=$1`, item.id, attempt, hash, request.Now, leaseExpires, leaseMax); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO queue_delivery_attempts(delivery_id,generation,attempt,outcome,occurred_at) VALUES($1,$2,$3,'leased',$4)`, item.id, item.generation, attempt, request.Now); err != nil {
			return nil, err
		}
		result = append(result, domain.QueueDelivery{ID: item.id, SubscriptionID: request.SubscriptionID, Event: domain.Event{ID: item.eventID, Type: item.eventType, SourceAppID: item.sourceAppID, TargetAppIDs: item.targetAppIDs, Data: json.RawMessage(item.data), CreatedAt: item.createdAt}, Receipt: receipt, Attempt: attempt, Generation: item.generation, LeaseExpiresAt: leaseExpires, OrderingKey: queueStringValue(item.orderingKey), Priority: item.priority, Metadata: json.RawMessage(item.metadata)})
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

func (client *Client) SettleQueueDeliveries(ctx context.Context, appID, subscriptionID string, settlements []store.QueueSettlement, now time.Time) ([]store.QueueSettlementResult, error) {
	tx, err := client.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	subscription, err := scanQueueSubscription(tx.QueryRow(ctx, `SELECT `+queueSubscriptionColumns+` FROM queue_subscriptions WHERE app_id=$1 AND id=$2 FOR SHARE`, appID, subscriptionID))
	if err != nil {
		return nil, err
	}
	results := make([]store.QueueSettlementResult, 0, len(settlements))
	for _, settlement := range settlements {
		status := "invalid_receipt"
		nextStatus := "acked"
		availableAt := now
		switch settlement.Disposition {
		case store.QueueRetry:
			nextStatus, availableAt = "available", now.Add(settlement.Delay)
		case store.QueueDeadLetter:
			nextStatus = "dead_letter"
		case store.QueueAcknowledge:
		default:
			results = append(results, store.QueueSettlementResult{Receipt: settlement.Receipt, Status: status})
			continue
		}
		var deliveryID string
		var eventID string
		var generation int64
		var attempt int
		var persistedStatus string
		err := tx.QueryRow(ctx, `UPDATE queue_deliveries SET status=CASE WHEN $4='available' AND attempts >= $8 THEN 'dead_letter' ELSE $4 END,available_at=$5,receipt_hash=NULL,lease_started_at=NULL,lease_expires_at=NULL,lease_max_expires_at=NULL,last_reason=CASE WHEN $4='available' AND attempts >= $8 THEN 'max_attempts_exhausted' ELSE NULLIF($6,'') END,updated_at=$7 WHERE subscription_id=$1 AND app_id=$2 AND receipt_hash=$3 AND status='in_flight' AND lease_expires_at>$7 RETURNING id,event_id,generation,attempts,status`, subscriptionID, appID, queueReceiptHash(settlement.Receipt), nextStatus, availableAt, settlement.Reason, now, subscription.MaxAttempts).Scan(&deliveryID, &eventID, &generation, &attempt, &persistedStatus)
		if err == nil {
			status = persistedStatus
			outcome, reason := string(settlement.Disposition), settlement.Reason
			if persistedStatus == "dead_letter" && settlement.Disposition == store.QueueRetry {
				outcome, reason = "dead_letter", "max_attempts_exhausted"
			}
			_, err = tx.Exec(ctx, `INSERT INTO queue_delivery_attempts(delivery_id,generation,attempt,outcome,reason,occurred_at) VALUES($1,$2,$3,$4,NULLIF($5,''),$6)`, deliveryID, generation, attempt, outcome, reason, now)
			if err == nil && (persistedStatus == "acked" || persistedStatus == "dead_letter") {
				err = insertQueueResultCallback(ctx, tx, subscription, deliveryID, eventID, generation, attempt, persistedStatus, now)
			}
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}
		results = append(results, store.QueueSettlementResult{Receipt: settlement.Receipt, Status: status})
	}
	return results, tx.Commit(ctx)
}

func (client *Client) ExtendQueueLeases(ctx context.Context, appID, subscriptionID string, extensions []store.QueueLeaseExtension, now time.Time) ([]store.QueueSettlementResult, error) {
	tx, err := client.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err := scanQueueSubscription(tx.QueryRow(ctx, `SELECT `+queueSubscriptionColumns+` FROM queue_subscriptions WHERE app_id=$1 AND id=$2 FOR SHARE`, appID, subscriptionID)); err != nil {
		return nil, err
	}
	results := make([]store.QueueSettlementResult, 0, len(extensions))
	for _, extension := range extensions {
		result, err := tx.Exec(ctx, `UPDATE queue_deliveries SET lease_expires_at=LEAST(lease_expires_at+($4::bigint*interval '1 microsecond'),lease_max_expires_at),updated_at=$5 WHERE subscription_id=$1 AND app_id=$2 AND receipt_hash=$3 AND status='in_flight' AND lease_expires_at>$5`, subscriptionID, appID, queueReceiptHash(extension.Receipt), extension.Extension.Microseconds(), now)
		if err != nil {
			return nil, err
		}
		status := "invalid_receipt"
		if result.RowsAffected() == 1 {
			status = "extended"
		}
		results = append(results, store.QueueSettlementResult{Receipt: extension.Receipt, Status: status})
	}
	return results, tx.Commit(ctx)
}

func (client *Client) QueueDepth(ctx context.Context, appID, subscriptionID string, now time.Time) (domain.QueueDepth, error) {
	var depth domain.QueueDepth
	err := client.pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE d.status='available' AND d.available_at<=$3),count(*) FILTER (WHERE d.status='in_flight'),count(*) FILTER (WHERE d.status='acked'),count(*) FILTER (WHERE d.status='dead_letter'),min(d.available_at) FILTER (WHERE d.status='available' AND d.available_at<=$3) FROM queue_deliveries d JOIN queue_subscriptions s ON s.id=d.subscription_id WHERE s.app_id=$1 AND s.id=$2`, appID, subscriptionID, now).Scan(&depth.Available, &depth.InFlight, &depth.Acknowledged, &depth.DeadLetter, &depth.OldestAvailable)
	return depth, err
}

func (client *Client) ListQueueDeadLetters(ctx context.Context, appID, subscriptionID string, query store.QueueDeadLetterQuery) ([]domain.QueueDeadLetter, error) {
	rows, err := client.pool.Query(ctx, `SELECT d.id,d.subscription_id,d.event_id,d.attempts,d.generation,COALESCE(d.last_reason,''),d.updated_at FROM queue_deliveries d JOIN queue_subscriptions s ON s.id=d.subscription_id WHERE s.app_id=$1 AND s.id=$2 AND d.status='dead_letter' AND ($3='' OR d.id>$3) ORDER BY d.id LIMIT $4`, appID, subscriptionID, query.Cursor, query.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []domain.QueueDeadLetter{}
	for rows.Next() {
		var item domain.QueueDeadLetter
		if err := rows.Scan(&item.DeliveryID, &item.SubscriptionID, &item.EventID, &item.Attempts, &item.Generation, &item.Reason, &item.UpdatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (client *Client) ReplayQueueDeadLetters(ctx context.Context, appID, subscriptionID string, ids []string, now time.Time) (int, error) {
	result, err := client.pool.Exec(ctx, `UPDATE queue_deliveries d SET status='available',generation=generation+1,attempts=0,available_at=$4,receipt_hash=NULL,lease_started_at=NULL,lease_expires_at=NULL,lease_max_expires_at=NULL,last_reason=NULL,updated_at=$4 FROM queue_subscriptions s WHERE s.id=d.subscription_id AND s.app_id=$1 AND s.id=$2 AND d.id=ANY($3) AND d.status='dead_letter'`, appID, subscriptionID, ids, now)
	return int(result.RowsAffected()), err
}

func (client *Client) DeleteQueueDeadLetters(ctx context.Context, appID, subscriptionID string, ids []string) (int, error) {
	result, err := client.pool.Exec(ctx, `DELETE FROM queue_deliveries d USING queue_subscriptions s WHERE s.id=d.subscription_id AND s.app_id=$1 AND s.id=$2 AND d.id=ANY($3) AND d.status='dead_letter'`, appID, subscriptionID, ids)
	return int(result.RowsAffected()), err
}

func queueReceiptHash(receipt string) string {
	digest := sha256.Sum256([]byte(receipt))
	return hex.EncodeToString(digest[:])
}

func queueStringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
