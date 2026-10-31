package migrate

import (
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Options struct {
	SourceURL string
	TargetURL string
	Mode      string
	Confirm   string
}

var sourceTables = []string{
	"admin_users", "applications", "application_credentials", "callback_endpoints",
	"events", "deliveries", "queue_subscriptions", "queue_schedules", "functions",
	"delivery_attempts", "outbox", "routing_rules", "audit_log", "realtime_files", "realtime_push_devices",
	"realtime_push_bindings", "realtime_push_outcomes",
}

func Run(ctx context.Context, options Options) (Report, error) {
	if err := ValidateMode(options.Mode); err != nil {
		return Report{}, err
	}
	if options.SourceURL == "" || options.TargetURL == "" {
		return Report{}, errors.New("source and target PostgreSQL URLs are required")
	}
	report := Report{SourceSystem: "legacy-go", Counts: map[string]int64{}, TargetCounts: map[string]int64{}, Migrated: map[string]int64{}, Unsupported: map[string]int64{}, State: options.Mode}
	if options.Mode == "apply" && options.Confirm != "APPLY_RELAYHUB_MIGRATION" {
		return report, errConfirmation
	}
	source, err := pgxpool.New(ctx, options.SourceURL)
	if err != nil {
		return report, errors.New("connect source PostgreSQL")
	}
	defer source.Close()
	target, err := pgxpool.New(ctx, options.TargetURL)
	if err != nil {
		return report, errors.New("connect target PostgreSQL")
	}
	defer target.Close()
	if err := source.Ping(ctx); err != nil {
		return report, errors.New("source PostgreSQL is unavailable")
	}
	if err := target.Ping(ctx); err != nil {
		return report, errors.New("target PostgreSQL is unavailable")
	}
	for _, table := range sourceTables {
		count, err := tableCount(ctx, source, table)
		if err != nil {
			return report, fmt.Errorf("count source %s: %w", table, err)
		}
		report.Counts[table] = count
	}
	for _, table := range []string{"identity_users", "identity_apps", "identity_credentials", "functions", "routing_rules", "queue_subscriptions", "queue_schedules", "events", "deliveries", "delivery_attempts", "event_outbox", "admin_audit_log", "realtime_files", "realtime_push_devices", "realtime_push_bindings", "realtime_push_outcomes"} {
		count, err := tableCount(ctx, target, table)
		if err != nil {
			return report, fmt.Errorf("count target %s: %w", table, err)
		}
		report.TargetCounts[table] = count
	}
	if options.Mode == "apply" {
		if err := applyAdminAndAppInventory(ctx, source, target, &report); err != nil {
			return report, err
		}
	}
	if options.Mode == "verify" {
		if err := verifyAdminAndAppInventory(ctx, source, target, &report); err != nil {
			return report, err
		}
	}
	if options.Mode == "rollback-report" {
		if err := loadLatestRun(ctx, target, &report); err != nil {
			return report, err
		}
	}
	if options.Mode == "plan" {
		report.State = "planned"
	}
	return report, nil
}

func tableCount(ctx context.Context, pool *pgxpool.Pool, table string) (int64, error) {
	var count int64
	if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", table)).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func applyAdminAndAppInventory(ctx context.Context, source, target *pgxpool.Pool, report *Report) error {
	tx, err := target.Begin(ctx)
	if err != nil {
		return errors.New("begin target migration")
	}
	defer tx.Rollback(ctx)
	runID := uuid.New()
	report.RunID = runID.String()
	if _, err := tx.Exec(ctx, `INSERT INTO relayhub_migration_runs(id,source_system,mode,state,report) VALUES($1,'legacy-go','apply','planned',$2)`, runID, reportJSON(*report)); err != nil {
		return errors.New("create migration ledger row")
	}
	rows, err := source.Query(ctx, `SELECT id,email,password_hash,role,enabled,created_at,updated_at FROM admin_users ORDER BY id`)
	if err != nil {
		return errors.New("read legacy Admin users")
	}
	defer rows.Close()
	var workspaceID uuid.UUID
	for rows.Next() {
		var sourceID, email, passwordHash, role string
		var enabled bool
		var createdAt, updatedAt time.Time
		if err := rows.Scan(&sourceID, &email, &passwordHash, &role, &enabled, &createdAt, &updatedAt); err != nil {
			return errors.New("scan legacy Admin user")
		}
		var targetID uuid.UUID
		err := tx.QueryRow(ctx, `SELECT id FROM identity_users WHERE legacy_source_id=$1`, sourceID).Scan(&targetID)
		if errors.Is(err, pgx.ErrNoRows) {
			targetID = uuid.New()
			if _, err := tx.Exec(ctx, `INSERT INTO identity_users(id,email,is_admin,password_hash,enabled,created_at,updated_at,legacy_source_id,legacy_password_hash) VALUES($1,$2,$3,NULL,$4,$5,$6,$7,$8)`, targetID, email, role == "admin", enabled, createdAt, updatedAt, sourceID, passwordHash); err != nil {
				return errors.New("insert migrated Admin user")
			}
			if _, err := tx.Exec(ctx, `INSERT INTO identity_workspaces(id,owner_id,created_at) VALUES($1,$2,$3) ON CONFLICT(owner_id) DO NOTHING`, uuid.New(), targetID, createdAt); err != nil {
				return errors.New("insert migrated Admin workspace")
			}
		} else if err != nil {
			return errors.New("lookup migrated Admin user")
		}
		if err := tx.QueryRow(ctx, `SELECT id FROM identity_workspaces WHERE owner_id=$1`, targetID).Scan(&workspaceID); err != nil {
			return errors.New("find migrated Admin workspace")
		}
		report.Migrated["admin_users"]++
	}
	if err := rows.Err(); err != nil {
		return errors.New("iterate legacy Admin users")
	}
	if workspaceID == uuid.Nil {
		return errors.New("find migrated Admin workspace")
	}
	appRows, err := source.Query(ctx, `SELECT id,name,callback_url,delivery_mode,enabled,created_at,updated_at FROM applications ORDER BY id`)
	if err != nil {
		return errors.New("read legacy applications")
	}
	defer appRows.Close()
	for appRows.Next() {
		var sourceID, name, mode string
		var callbackURL *string
		var enabled bool
		var createdAt, updatedAt time.Time
		if err := appRows.Scan(&sourceID, &name, &callbackURL, &mode, &enabled, &createdAt, &updatedAt); err != nil {
			return errors.New("scan legacy application")
		}
		if mode != "queue" && mode != "websocket" && mode != "callback" && mode != "all" {
			mode = "queue"
		}
		_, err := tx.Exec(ctx, `INSERT INTO identity_apps(id,workspace_id,name,delivery_mode,callback_url,enabled,created_at,updated_at,legacy_source_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(legacy_source_id) DO UPDATE SET name=EXCLUDED.name,delivery_mode=EXCLUDED.delivery_mode,callback_url=EXCLUDED.callback_url,enabled=EXCLUDED.enabled,updated_at=EXCLUDED.updated_at`, uuid.New(), workspaceID, name, mode, callbackURL, enabled, createdAt, updatedAt, sourceID)
		if err != nil {
			return errors.New("upsert migrated application")
		}
		report.Migrated["applications"]++
	}
	if err := appRows.Err(); err != nil {
		return errors.New("iterate legacy applications")
	}
	if err := applyLegacyControlPlane(ctx, source, tx, report); err != nil {
		return err
	}
	if err := applyLegacyDataPlane(ctx, source, tx, workspaceID, report); err != nil {
		return err
	}
	report.State = "applied"
	if _, err := tx.Exec(ctx, `UPDATE relayhub_migration_runs SET state='applied',report=$2,completed_at=clock_timestamp() WHERE id=$1`, runID, reportJSON(*report)); err != nil {
		return errors.New("complete migration ledger row")
	}
	if err := tx.Commit(ctx); err != nil {
		return errors.New("commit target migration")
	}
	return nil
}

func targetApp(ctx context.Context, tx pgx.Tx, sourceID string) (uuid.UUID, uuid.UUID, error) {
	var appID, workspaceID uuid.UUID
	err := tx.QueryRow(ctx, `SELECT id,workspace_id FROM identity_apps WHERE legacy_source_id=$1`, sourceID).Scan(&appID, &workspaceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, uuid.Nil, fmt.Errorf("legacy application %q was not migrated", sourceID)
	}
	return appID, workspaceID, err
}

func randomTokenHash() ([]byte, error) {
	value := make([]byte, 32)
	if _, err := crand.Read(value); err != nil {
		return nil, err
	}
	return value, nil
}

func applyLegacyControlPlane(ctx context.Context, source *pgxpool.Pool, target pgx.Tx, report *Report) error {
	credentialRows, err := source.Query(ctx, `SELECT app_id,api_key_hash,encrypted_hmac_secret,created_at FROM application_credentials WHERE revoked_at IS NULL ORDER BY app_id`)
	if err != nil {
		return errors.New("read legacy application credentials")
	}
	defer credentialRows.Close()
	for credentialRows.Next() {
		var sourceAppID, apiKeyHash, encryptedSecret string
		var createdAt time.Time
		if err := credentialRows.Scan(&sourceAppID, &apiKeyHash, &encryptedSecret, &createdAt); err != nil {
			return errors.New("scan legacy application credential")
		}
		appID, workspaceID, err := targetApp(ctx, target, sourceAppID)
		if err != nil {
			return err
		}
		tokenHash, err := randomTokenHash()
		if err != nil {
			return errors.New("generate migrated credential token hash")
		}
		var targetCredentialID uuid.UUID
		err = target.QueryRow(ctx, `UPDATE identity_credentials SET workspace_id=$2,app_id=$3,kind='application',token_hash=$4,created_at=$5,legacy_api_key_hash=$6,encrypted_hmac_secret=$7,revoked_at=NULL WHERE legacy_source_id=$1 RETURNING id`, sourceAppID, workspaceID, appID, tokenHash, createdAt, apiKeyHash, encryptedSecret).Scan(&targetCredentialID)
		if errors.Is(err, pgx.ErrNoRows) {
			_, err = target.Exec(ctx, `INSERT INTO identity_credentials(id,workspace_id,app_id,kind,token_hash,created_at,legacy_api_key_hash,encrypted_hmac_secret,legacy_source_id) VALUES($1,$2,$3,'application',$4,$5,$6,$7,$8)`, uuid.New(), workspaceID, appID, tokenHash, createdAt, apiKeyHash, encryptedSecret, sourceAppID)
		}
		if err != nil {
			return errors.New("insert migrated application credential")
		}
		report.Migrated["application_credentials"]++
	}
	if err := credentialRows.Err(); err != nil {
		return errors.New("iterate legacy application credentials")
	}

	functionRows, err := source.Query(ctx, `SELECT id,app_id,name,timeout_seconds,enabled,created_at,updated_at FROM functions ORDER BY id`)
	if err != nil {
		return errors.New("read legacy functions")
	}
	defer functionRows.Close()
	for functionRows.Next() {
		var sourceID, sourceAppID, name string
		var timeout int
		var enabled bool
		var createdAt, updatedAt time.Time
		if err := functionRows.Scan(&sourceID, &sourceAppID, &name, &timeout, &enabled, &createdAt, &updatedAt); err != nil {
			return errors.New("scan legacy function")
		}
		appID, workspaceID, err := targetApp(ctx, target, sourceAppID)
		if err != nil {
			return err
		}
		var targetID uuid.UUID
		err = target.QueryRow(ctx, `UPDATE functions SET workspace_id=$2,app_id=$3,name=$4,timeout_seconds=$5,enabled=$6,created_at=$7,updated_at=$8 WHERE legacy_source_id=$1 RETURNING id`, sourceID, workspaceID, appID, name, timeout, enabled, createdAt, updatedAt).Scan(&targetID)
		if errors.Is(err, pgx.ErrNoRows) {
			targetID = uuid.New()
			_, err = target.Exec(ctx, `INSERT INTO functions(id,workspace_id,app_id,name,timeout_seconds,enabled,created_at,updated_at,legacy_source_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, targetID, workspaceID, appID, name, timeout, enabled, createdAt, updatedAt, sourceID)
		}
		if err != nil {
			return errors.New("upsert migrated function")
		}
		report.Migrated["functions"]++
	}
	if err := functionRows.Err(); err != nil {
		return errors.New("iterate legacy functions")
	}

	ruleRows, err := source.Query(ctx, `SELECT id,source_app_id,event_type,target_app_id,realtime_channel,enabled,created_at,updated_at,deleted_at FROM routing_rules ORDER BY id`)
	if err != nil {
		return errors.New("read legacy routing rules")
	}
	defer ruleRows.Close()
	for ruleRows.Next() {
		var sourceID, eventType, targetSourceAppID string
		var sourceSourceAppID, channel *string
		var enabled bool
		var createdAt, updatedAt time.Time
		var deletedAt *time.Time
		if err := ruleRows.Scan(&sourceID, &sourceSourceAppID, &eventType, &targetSourceAppID, &channel, &enabled, &createdAt, &updatedAt, &deletedAt); err != nil {
			return errors.New("scan legacy routing rule")
		}
		targetAppID, workspaceID, err := targetApp(ctx, target, targetSourceAppID)
		if err != nil {
			return err
		}
		var sourceAppID *uuid.UUID
		if sourceSourceAppID != nil {
			mapped, _, err := targetApp(ctx, target, *sourceSourceAppID)
			if err != nil {
				return err
			}
			sourceAppID = &mapped
		}
		var targetID uuid.UUID
		err = target.QueryRow(ctx, `UPDATE routing_rules SET workspace_id=$2,source_app_id=$3,event_type=$4,target_app_id=$5,realtime_channel=$6,enabled=$7,created_at=$8,updated_at=$9,deleted_at=$10 WHERE legacy_source_id=$1 RETURNING id`, sourceID, workspaceID, sourceAppID, eventType, targetAppID, channel, enabled, createdAt, updatedAt, deletedAt).Scan(&targetID)
		if errors.Is(err, pgx.ErrNoRows) {
			targetID = uuid.New()
			_, err = target.Exec(ctx, `INSERT INTO routing_rules(id,workspace_id,source_app_id,event_type,target_app_id,realtime_channel,enabled,created_at,updated_at,deleted_at,legacy_source_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, targetID, workspaceID, sourceAppID, eventType, targetAppID, channel, enabled, createdAt, updatedAt, deletedAt, sourceID)
		}
		if err != nil {
			return errors.New("upsert migrated routing rule")
		}
		report.Migrated["routing_rules"]++
	}
	if err := ruleRows.Err(); err != nil {
		return errors.New("iterate legacy routing rules")
	}

	if err := migrateQueueConfig(ctx, source, target, report); err != nil {
		return err
	}
	return nil
}

func migrateQueueConfig(ctx context.Context, source *pgxpool.Pool, target pgx.Tx, report *Report) error {
	rows, err := source.Query(ctx, `SELECT id,app_id,name,mode,enabled,paused_at,event_types,max_attempts,default_visibility_seconds,max_visibility_seconds,max_total_lease_seconds,retention_seconds,max_in_flight,max_batch_size,retry_delay_seconds,ordering_mode,deduplication_seconds,max_dispatch_rate,success_callback_url,failure_callback_url,policy_version,created_at,updated_at,result_callback_metadata FROM queue_subscriptions ORDER BY id`)
	if err != nil {
		return errors.New("read legacy queue subscriptions")
	}
	defer rows.Close()
	for rows.Next() {
		var sourceID, sourceAppID, name, mode, ordering string
		var enabled bool
		var pausedAt *time.Time
		var eventTypes []string
		var maxAttempts, visibility, maxVisibility, maxLease, retention, maxInFlight, maxBatch, retryDelay, dedup int
		var maxRate *int
		var successURL, failureURL *string
		var policy int64
		var createdAt, updatedAt time.Time
		var callbackMetadata []byte
		if err := rows.Scan(&sourceID, &sourceAppID, &name, &mode, &enabled, &pausedAt, &eventTypes, &maxAttempts, &visibility, &maxVisibility, &maxLease, &retention, &maxInFlight, &maxBatch, &retryDelay, &ordering, &dedup, &maxRate, &successURL, &failureURL, &policy, &createdAt, &updatedAt, &callbackMetadata); err != nil {
			return errors.New("scan legacy queue subscription")
		}
		appID, workspaceID, err := targetApp(ctx, target, sourceAppID)
		if err != nil {
			return err
		}
		var targetID uuid.UUID
		err = target.QueryRow(ctx, `UPDATE queue_subscriptions SET workspace_id=$2,app_id=$3,name=$4,enabled=$5,paused_at=$6,max_in_flight=$7,max_batch_size=$8,ordering_mode=$9,max_dispatch_rate=$10,success_callback_url=$11,failure_callback_url=$12,policy_version=$13,created_at=$14,updated_at=$15,mode=$16,event_types=$17,max_attempts=$18,default_visibility_seconds=$19,max_visibility_seconds=$20,max_total_lease_seconds=$21,retention_seconds=$22,retry_delay_seconds=$23,deduplication_seconds=$24,result_callback_metadata=$25 WHERE legacy_source_id=$1 RETURNING id`, sourceID, workspaceID, appID, name, enabled, pausedAt, maxInFlight, maxBatch, ordering, maxRate, successURL, failureURL, policy, createdAt, updatedAt, mode, eventTypes, maxAttempts, visibility, maxVisibility, maxLease, retention, retryDelay, dedup, string(callbackMetadata)).Scan(&targetID)
		if errors.Is(err, pgx.ErrNoRows) {
			targetID = uuid.New()
			_, err = target.Exec(ctx, `INSERT INTO queue_subscriptions(id,workspace_id,app_id,name,enabled,paused_at,max_in_flight,max_batch_size,ordering_mode,max_dispatch_rate,success_callback_url,failure_callback_url,policy_version,created_at,updated_at,legacy_source_id,mode,event_types,max_attempts,default_visibility_seconds,max_visibility_seconds,max_total_lease_seconds,retention_seconds,retry_delay_seconds,deduplication_seconds,result_callback_metadata) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)`, targetID, workspaceID, appID, name, enabled, pausedAt, maxInFlight, maxBatch, ordering, maxRate, successURL, failureURL, policy, createdAt, updatedAt, sourceID, mode, eventTypes, maxAttempts, visibility, maxVisibility, maxLease, retention, retryDelay, dedup, string(callbackMetadata))
		}
		if err != nil {
			return errors.New("upsert migrated queue subscription")
		}
		report.Migrated["queue_subscriptions"]++
	}
	if err := rows.Err(); err != nil {
		return errors.New("iterate legacy queue subscriptions")
	}

	scheduleRows, err := source.Query(ctx, `SELECT id,app_id,subscription_id,name,enabled,cron_expression,timezone,event_type,data,ordering_key,priority,metadata,next_run_at,last_run_at,policy_version,created_at,updated_at FROM queue_schedules ORDER BY id`)
	if err != nil {
		return errors.New("read legacy queue schedules")
	}
	defer scheduleRows.Close()
	for scheduleRows.Next() {
		var sourceID, sourceAppID, sourceSubscriptionID, name, cron, timezone, eventType string
		var enabled bool
		var data, metadata []byte
		var orderingKey *string
		var priority, policy int
		var nextRunAt time.Time
		var lastRunAt *time.Time
		var createdAt, updatedAt time.Time
		if err := scheduleRows.Scan(&sourceID, &sourceAppID, &sourceSubscriptionID, &name, &enabled, &cron, &timezone, &eventType, &data, &orderingKey, &priority, &metadata, &nextRunAt, &lastRunAt, &policy, &createdAt, &updatedAt); err != nil {
			return errors.New("scan legacy queue schedule")
		}
		appID, workspaceID, err := targetApp(ctx, target, sourceAppID)
		if err != nil {
			return err
		}
		var subscriptionID uuid.UUID
		if err := target.QueryRow(ctx, `SELECT id FROM queue_subscriptions WHERE legacy_source_id=$1 AND app_id=$2`, sourceSubscriptionID, appID).Scan(&subscriptionID); err != nil {
			return fmt.Errorf("legacy queue subscription %q was not migrated", sourceSubscriptionID)
		}
		var targetID uuid.UUID
		err = target.QueryRow(ctx, `UPDATE queue_schedules SET workspace_id=$2,app_id=$3,subscription_id=$4,name=$5,enabled=$6,cron_expression=$7,timezone=$8,event_type=$9,data_text=$10,ordering_key=$11,priority=$12,metadata_text=$13,next_run_at=$14,last_run_at=$15,policy_version=$16,created_at=$17,updated_at=$18 WHERE legacy_source_id=$1 RETURNING id`, sourceID, workspaceID, appID, subscriptionID, name, enabled, cron, timezone, eventType, string(data), orderingKey, priority, string(metadata), nextRunAt, lastRunAt, policy, createdAt, updatedAt).Scan(&targetID)
		if errors.Is(err, pgx.ErrNoRows) {
			targetID = uuid.New()
			_, err = target.Exec(ctx, `INSERT INTO queue_schedules(id,workspace_id,app_id,subscription_id,name,enabled,cron_expression,timezone,event_type,data_text,ordering_key,priority,metadata_text,next_run_at,last_run_at,policy_version,created_at,updated_at,legacy_source_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)`, targetID, workspaceID, appID, subscriptionID, name, enabled, cron, timezone, eventType, string(data), orderingKey, priority, string(metadata), nextRunAt, lastRunAt, policy, createdAt, updatedAt, sourceID)
		}
		if err != nil {
			return errors.New("upsert migrated queue schedule")
		}
		report.Migrated["queue_schedules"]++
	}
	if err := scheduleRows.Err(); err != nil {
		return errors.New("iterate legacy queue schedules")
	}
	return nil
}

type legacyEventInput struct {
	EndpointID uuid.UUID  `json:"endpoint_id"`
	EventType  string     `json:"event_type"`
	Method     string     `json:"method"`
	Path       string     `json:"path"`
	Headers    [][]string `json:"headers"`
	Body       string     `json:"body"`
}

func ensureLegacyEndpoint(ctx context.Context, target pgx.Tx, workspaceID, appID uuid.UUID) (uuid.UUID, error) {
	var endpointID uuid.UUID
	err := target.QueryRow(ctx, `SELECT id FROM event_endpoints WHERE workspace_id=$1 AND app_id=$2 ORDER BY id LIMIT 1`, workspaceID, appID).Scan(&endpointID)
	if err == nil {
		return endpointID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, err
	}
	endpointID = uuid.New()
	_, err = target.Exec(ctx, `INSERT INTO event_endpoints(id,workspace_id,app_id,policy,public_ingress,ingress_secret_hash) VALUES($1,$2,$3,'{"mode":"async"}'::jsonb,false,NULL)`, endpointID, workspaceID, appID)
	return endpointID, err
}

func legacyDeliveryState(status string) (string, error) {
	switch status {
	case "pending", "dispatched", "retrying":
		return "pending", nil
	case "acked":
		return "succeeded", nil
	case "dead_letter":
		return "dlq", nil
	default:
		return "", fmt.Errorf("unsupported legacy delivery status %q", status)
	}
}

func legacyAttemptOutcome(outcome string) string {
	switch outcome {
	case "success", "acked", "delivered":
		return "success"
	case "lease_expired", "visibility_timeout", "timeout":
		return "lease_expired"
	default:
		return "retryable_failure"
	}
}

func applyLegacyDataPlane(ctx context.Context, source *pgxpool.Pool, target pgx.Tx, workspaceID uuid.UUID, report *Report) error {
	eventIDs := make(map[string]uuid.UUID)
	deliveryIDs := make(map[string]uuid.UUID)
	deliveryTerminal := make(map[string]bool)

	events, err := source.Query(ctx, `SELECT id,type,source_app_id,target_app_ids,data::text,created_at,expires_at FROM events ORDER BY id`)
	if err != nil {
		return errors.New("read legacy events")
	}
	defer events.Close()
	for events.Next() {
		var sourceID, eventType, sourceAppID, dataText string
		var targetAppIDs []string
		var createdAt, expiresAt time.Time
		if err := events.Scan(&sourceID, &eventType, &sourceAppID, &targetAppIDs, &dataText, &createdAt, &expiresAt); err != nil {
			return errors.New("scan legacy event")
		}
		appID, mappedWorkspace, err := targetApp(ctx, target, sourceAppID)
		if err != nil {
			return err
		}
		if mappedWorkspace != workspaceID {
			return fmt.Errorf("legacy event %q belongs to another workspace", sourceID)
		}
		if !expiresAt.After(createdAt) {
			return fmt.Errorf("legacy event %q has invalid retention", sourceID)
		}
		for _, targetSourceID := range targetAppIDs {
			if _, _, err := targetApp(ctx, target, targetSourceID); err != nil {
				return fmt.Errorf("legacy event %q target: %w", sourceID, err)
			}
		}
		endpointID, err := ensureLegacyEndpoint(ctx, target, workspaceID, appID)
		if err != nil {
			return errors.New("create legacy event endpoint")
		}
		input := legacyEventInput{
			EndpointID: endpointID,
			EventType:  eventType,
			Method:     "POST",
			Path:       "/legacy/events/" + sourceID,
			Headers:    [][]string{},
			Body:       base64.StdEncoding.EncodeToString([]byte(dataText)),
		}
		payload, err := json.Marshal(input)
		if err != nil {
			return errors.New("encode legacy event payload")
		}
		hash := sha256.Sum256(payload)
		var eventID uuid.UUID
		err = target.QueryRow(ctx, `INSERT INTO events(id,workspace_id,app_id,endpoint_id,idempotency_key,payload,payload_hash,payload_bytes,created_at,expires_at,legacy_source_id,legacy_target_app_ids) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT (legacy_source_id) WHERE legacy_source_id IS NOT NULL DO UPDATE SET workspace_id=EXCLUDED.workspace_id,app_id=EXCLUDED.app_id,endpoint_id=EXCLUDED.endpoint_id,idempotency_key=EXCLUDED.idempotency_key,payload=EXCLUDED.payload,payload_hash=EXCLUDED.payload_hash,payload_bytes=EXCLUDED.payload_bytes,created_at=EXCLUDED.created_at,expires_at=EXCLUDED.expires_at,legacy_target_app_ids=EXCLUDED.legacy_target_app_ids RETURNING id`, uuid.New(), workspaceID, appID, endpointID, "legacy:"+sourceID, payload, hash[:], len([]byte(dataText)), createdAt, expiresAt, sourceID, targetAppIDs).Scan(&eventID)
		if err != nil {
			return fmt.Errorf("upsert migrated event: %w", err)
		}
		eventIDs[sourceID] = eventID
		report.Migrated["events"]++
	}
	if err := events.Err(); err != nil {
		return errors.New("iterate legacy events")
	}

	deliveries, err := source.Query(ctx, `SELECT id,public_job_id,event_id,source_app_id,target_app_id,sink,status,attempts,created_at,updated_at FROM deliveries ORDER BY id`)
	if err != nil {
		return errors.New("read legacy deliveries")
	}
	defer deliveries.Close()
	for deliveries.Next() {
		var sourceID, publicJobID, sourceEventID, sourceAppID, targetSourceAppID, sink, status string
		var attempts int
		var createdAt, updatedAt time.Time
		if err := deliveries.Scan(&sourceID, &publicJobID, &sourceEventID, &sourceAppID, &targetSourceAppID, &sink, &status, &attempts, &createdAt, &updatedAt); err != nil {
			return errors.New("scan legacy delivery")
		}
		if attempts < 0 || attempts > 5 {
			return fmt.Errorf("legacy delivery %q has attempts outside Rust bounds", sourceID)
		}
		eventID, ok := eventIDs[sourceEventID]
		if !ok {
			return fmt.Errorf("legacy delivery %q references unmigrated event %q", sourceID, sourceEventID)
		}
		sourceAppIDTarget, mappedWorkspace, err := targetApp(ctx, target, sourceAppID)
		if err != nil {
			return err
		}
		targetAppID, targetWorkspace, err := targetApp(ctx, target, targetSourceAppID)
		if err != nil {
			return err
		}
		if mappedWorkspace != workspaceID || targetWorkspace != workspaceID {
			return fmt.Errorf("legacy delivery %q belongs to another workspace", sourceID)
		}
		state, err := legacyDeliveryState(status)
		if err != nil {
			return err
		}
		var deliveryID uuid.UUID
		err = target.QueryRow(ctx, `SELECT id FROM deliveries WHERE legacy_source_id=$1`, sourceID).Scan(&deliveryID)
		if errors.Is(err, pgx.ErrNoRows) {
			deliveryID = uuid.New()
			if _, err := target.Exec(ctx, `INSERT INTO deliveries(id,event_id,workspace_id,app_id,target_app_id,generation,state,attempt_count,created_at,legacy_source_id,legacy_public_job_id,legacy_sink) VALUES($1,$2,$3,$4,$5,1,'pending',$6,$7,$8,$9,$10)`, deliveryID, eventID, workspaceID, sourceAppIDTarget, targetAppID, attempts, createdAt, sourceID, publicJobID, sink); err != nil {
				return errors.New("insert migrated delivery")
			}
		} else if err != nil {
			return errors.New("lookup migrated delivery")
		}
		deliveryIDs[sourceID] = deliveryID
		deliveryTerminal[sourceID] = state != "pending"
		if state != "pending" {
			if _, err := target.Exec(ctx, `UPDATE deliveries SET state=$2,updated_at=$3 WHERE id=$1`, deliveryID, state, updatedAt); err != nil {
				return errors.New("restore migrated delivery state")
			}
		}
		attemptRows, err := source.Query(ctx, `SELECT attempt,outcome,created_at FROM delivery_attempts WHERE delivery_id=$1 ORDER BY attempt`, sourceID)
		if err != nil {
			return errors.New("read legacy delivery attempts")
		}
		for attemptRows.Next() {
			var attempt int
			var outcome string
			var occurredAt time.Time
			if err := attemptRows.Scan(&attempt, &outcome, &occurredAt); err != nil {
				attemptRows.Close()
				return errors.New("scan legacy delivery attempt")
			}
			if attempt < 1 || attempt > 5 {
				attemptRows.Close()
				return fmt.Errorf("legacy delivery %q has an attempt outside Rust bounds", sourceID)
			}
			token, agent := uuid.New(), uuid.New()
			_, err = target.Exec(ctx, `INSERT INTO delivery_attempts(delivery_id,attempt,attempt_token,agent_id,started_at,lease_expires_at,finished_at,outcome,legacy_source_id) VALUES($1,$2,$3,$4,$5,$5,$5,$6,$7) ON CONFLICT(delivery_id,attempt) DO NOTHING`, deliveryID, attempt, token, agent, occurredAt, legacyAttemptOutcome(outcome), fmt.Sprintf("legacy:%s:%d", sourceID, attempt))
			if err != nil {
				attemptRows.Close()
				return errors.New("insert migrated delivery attempt")
			}
			report.Migrated["delivery_attempts"]++
		}
		attemptRows.Close()
		if err := attemptRows.Err(); err != nil {
			return errors.New("iterate legacy delivery attempts")
		}
		report.Migrated["deliveries"]++
		if report.Migrated["delivery_attempts"] < 0 {
			return errors.New("invalid delivery attempt counter")
		}
	}
	if err := deliveries.Err(); err != nil {
		return errors.New("iterate legacy deliveries")
	}

	outboxRows, err := source.Query(ctx, `SELECT id,delivery_id,dispatched_at,last_error,created_at,updated_at FROM outbox ORDER BY id`)
	if err != nil {
		return errors.New("read legacy outbox")
	}
	defer outboxRows.Close()
	for outboxRows.Next() {
		var sourceID, sourceDeliveryID string
		var dispatchedAt *time.Time
		var createdAt, updatedAt time.Time
		var lastError *string
		if err := outboxRows.Scan(&sourceID, &sourceDeliveryID, &dispatchedAt, &lastError, &createdAt, &updatedAt); err != nil {
			return errors.New("scan legacy outbox")
		}
		deliveryID, ok := deliveryIDs[sourceDeliveryID]
		if !ok {
			return fmt.Errorf("legacy outbox %q references unmigrated delivery %q", sourceID, sourceDeliveryID)
		}
		var sentAt, discardedAt *time.Time
		if deliveryTerminal[sourceDeliveryID] {
			sentAt, discardedAt = dispatchedAt, &updatedAt
		}
		created := createdAt
		_, err = target.Exec(ctx, `INSERT INTO event_outbox(delivery_id,workspace_id,app_id,created_at,sent_at,discarded_at,legacy_source_id,legacy_dispatched_at,legacy_last_error) SELECT $1,workspace_id,app_id,$2,$3,$4,$5,$6,$7 FROM deliveries WHERE id=$1 ON CONFLICT (legacy_source_id) WHERE legacy_source_id IS NOT NULL DO UPDATE SET sent_at=EXCLUDED.sent_at,discarded_at=EXCLUDED.discarded_at,legacy_dispatched_at=EXCLUDED.legacy_dispatched_at,legacy_last_error=EXCLUDED.legacy_last_error`, deliveryID, created, sentAt, discardedAt, sourceID, dispatchedAt, lastError)
		if err != nil {
			return errors.New("insert migrated outbox")
		}
		report.Migrated["outbox"]++
	}
	if err := outboxRows.Err(); err != nil {
		return errors.New("iterate legacy outbox")
	}

	auditRows, err := source.Query(ctx, `SELECT id,occurred_at,actor_type,actor_id,action,resource_type,resource_id,outcome,metadata::text FROM audit_log ORDER BY id`)
	if err != nil {
		return errors.New("read legacy audit log")
	}
	defer auditRows.Close()
	for auditRows.Next() {
		var sourceID int64
		var occurredAt time.Time
		var actorType, action, resourceType, outcome, metadata string
		var actorID, resourceID *string
		if err := auditRows.Scan(&sourceID, &occurredAt, &actorType, &actorID, &action, &resourceType, &resourceID, &outcome, &metadata); err != nil {
			return errors.New("scan legacy audit row")
		}
		if metadata == "" {
			metadata = "{}"
		}
		if _, err := target.Exec(ctx, `INSERT INTO admin_audit_log(workspace_id,occurred_at,actor_type,actor_id,action,resource_type,resource_id,outcome,metadata,legacy_source_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10) ON CONFLICT (legacy_source_id) WHERE legacy_source_id IS NOT NULL DO NOTHING`, workspaceID, occurredAt, actorType, actorID, action, resourceType, resourceID, outcome, metadata, fmt.Sprintf("legacy:%d", sourceID)); err != nil {
			return errors.New("insert migrated audit row")
		}
		report.Migrated["audit_log"]++
	}
	if err := auditRows.Err(); err != nil {
		return errors.New("iterate legacy audit rows")
	}
	if err := migrateRealtimeResources(ctx, source, target, report); err != nil {
		return err
	}
	return nil
}

func migrateRealtimeResources(ctx context.Context, source *pgxpool.Pool, target pgx.Tx, report *Report) error {
	files, err := source.Query(ctx, `SELECT id,app_id,channel,name,mime_type,size_bytes,sha256,object_key,status,created_at,expires_at,completed_at FROM realtime_files ORDER BY id`)
	if err != nil {
		return errors.New("read legacy realtime files")
	}
	for files.Next() {
		var sourceID, sourceAppID, channel, name, mimeType, sha256Value, objectKey, status string
		var size int64
		var createdAt, expiresAt time.Time
		var completedAt *time.Time
		if err := files.Scan(&sourceID, &sourceAppID, &channel, &name, &mimeType, &size, &sha256Value, &objectKey, &status, &createdAt, &expiresAt, &completedAt); err != nil {
			files.Close()
			return errors.New("scan legacy realtime file")
		}
		appID, _, err := targetApp(ctx, target, sourceAppID)
		if err != nil {
			files.Close()
			return err
		}
		_, err = target.Exec(ctx, `INSERT INTO realtime_files(id,app_id,channel,name,mime_type,size_bytes,sha256,object_key,status,created_at,expires_at,completed_at,legacy_source_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT(legacy_source_id) DO UPDATE SET app_id=EXCLUDED.app_id,channel=EXCLUDED.channel,name=EXCLUDED.name,mime_type=EXCLUDED.mime_type,size_bytes=EXCLUDED.size_bytes,sha256=EXCLUDED.sha256,object_key=EXCLUDED.object_key,status=EXCLUDED.status,created_at=EXCLUDED.created_at,expires_at=EXCLUDED.expires_at,completed_at=EXCLUDED.completed_at`, sourceID, appID, channel, name, mimeType, size, sha256Value, objectKey, status, createdAt, expiresAt, completedAt, sourceID)
		if err != nil {
			files.Close()
			return errors.New("insert migrated realtime file")
		}
		report.Migrated["realtime_files"]++
	}
	if err := files.Err(); err != nil {
		files.Close()
		return errors.New("iterate legacy realtime files")
	}
	files.Close()

	deviceMap := make(map[string]uuid.UUID)
	devices, err := source.Query(ctx, `SELECT id,app_id,provider,token_hash,encrypted_token,created_at,updated_at FROM realtime_push_devices ORDER BY id`)
	if err != nil {
		return errors.New("read legacy push devices")
	}
	for devices.Next() {
		var sourceID, sourceAppID, provider, tokenHash, encryptedToken string
		var createdAt, updatedAt time.Time
		if err := devices.Scan(&sourceID, &sourceAppID, &provider, &tokenHash, &encryptedToken, &createdAt, &updatedAt); err != nil {
			devices.Close()
			return errors.New("scan legacy push device")
		}
		appID, _, err := targetApp(ctx, target, sourceAppID)
		if err != nil {
			devices.Close()
			return err
		}
		var targetID uuid.UUID
		err = target.QueryRow(ctx, `SELECT id FROM realtime_push_devices WHERE legacy_source_id=$1`, sourceID).Scan(&targetID)
		if errors.Is(err, pgx.ErrNoRows) {
			targetID = uuid.New()
			_, err = target.Exec(ctx, `INSERT INTO realtime_push_devices(id,app_id,provider,token_hash,encrypted_token,created_at,updated_at,legacy_source_id) VALUES($1,$2,$3,decode($4,'hex'),$5,$6,$7,$8) ON CONFLICT(app_id,provider,token_hash) DO UPDATE SET encrypted_token=EXCLUDED.encrypted_token,updated_at=EXCLUDED.updated_at`, targetID, appID, provider, tokenHash, encryptedToken, createdAt, updatedAt, sourceID)
		}
		if err != nil {
			devices.Close()
			return errors.New("insert migrated push device")
		}
		if err := target.QueryRow(ctx, `SELECT id FROM realtime_push_devices WHERE legacy_source_id=$1`, sourceID).Scan(&targetID); err != nil {
			devices.Close()
			return errors.New("resolve migrated push device")
		}
		deviceMap[sourceID] = targetID
		report.Migrated["realtime_push_devices"]++
	}
	if err := devices.Err(); err != nil {
		devices.Close()
		return errors.New("iterate legacy push devices")
	}
	devices.Close()

	bindings, err := source.Query(ctx, `SELECT app_id,channel,device_id,created_at FROM realtime_push_bindings ORDER BY app_id,channel,device_id`)
	if err != nil {
		return errors.New("read legacy push bindings")
	}
	for bindings.Next() {
		var sourceAppID, channel, sourceDeviceID string
		var createdAt time.Time
		if err := bindings.Scan(&sourceAppID, &channel, &sourceDeviceID, &createdAt); err != nil {
			bindings.Close()
			return errors.New("scan legacy push binding")
		}
		appID, _, err := targetApp(ctx, target, sourceAppID)
		if err != nil {
			bindings.Close()
			return err
		}
		deviceID, ok := deviceMap[sourceDeviceID]
		if !ok {
			bindings.Close()
			return fmt.Errorf("legacy push binding references unknown device %q", sourceDeviceID)
		}
		legacyID := fmt.Sprintf("legacy:%s:%s:%s", sourceAppID, channel, sourceDeviceID)
		if _, err := target.Exec(ctx, `INSERT INTO realtime_push_bindings(app_id,channel,device_id,created_at,legacy_source_id) VALUES($1,$2,$3,$4,$5) ON CONFLICT(legacy_source_id) DO NOTHING`, appID, channel, deviceID, createdAt, legacyID); err != nil {
			bindings.Close()
			return errors.New("insert migrated push binding")
		}
		report.Migrated["realtime_push_bindings"]++
	}
	if err := bindings.Err(); err != nil {
		bindings.Close()
		return errors.New("iterate legacy push bindings")
	}
	bindings.Close()

	outcomes, err := source.Query(ctx, `SELECT id,app_id,device_id,channel,provider,status,provider_message_id,reason,created_at FROM realtime_push_outcomes ORDER BY id`)
	if err != nil {
		return errors.New("read legacy push outcomes")
	}
	for outcomes.Next() {
		var sourceID, sourceAppID, sourceDeviceID, channel, provider, status string
		var providerMessageID, reason *string
		var createdAt time.Time
		if err := outcomes.Scan(&sourceID, &sourceAppID, &sourceDeviceID, &channel, &provider, &status, &providerMessageID, &reason, &createdAt); err != nil {
			outcomes.Close()
			return errors.New("scan legacy push outcome")
		}
		appID, _, err := targetApp(ctx, target, sourceAppID)
		if err != nil {
			outcomes.Close()
			return err
		}
		deviceID, ok := deviceMap[sourceDeviceID]
		if !ok {
			outcomes.Close()
			return fmt.Errorf("legacy push outcome references unknown device %q", sourceDeviceID)
		}
		targetID := uuid.New()
		if _, err := target.Exec(ctx, `INSERT INTO realtime_push_outcomes(id,app_id,device_id,channel,provider,status,provider_message_id,reason,created_at,legacy_source_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(legacy_source_id) DO NOTHING`, targetID, appID, deviceID, channel, provider, status, providerMessageID, reason, createdAt, sourceID); err != nil {
			outcomes.Close()
			return errors.New("insert migrated push outcome")
		}
		report.Migrated["realtime_push_outcomes"]++
	}
	if err := outcomes.Err(); err != nil {
		outcomes.Close()
		return errors.New("iterate legacy push outcomes")
	}
	outcomes.Close()
	return nil
}

func verifyAdminAndAppInventory(ctx context.Context, source, target *pgxpool.Pool, report *Report) error {
	var count int64
	if err := target.QueryRow(ctx, `SELECT count(*) FROM identity_users WHERE legacy_source_id IS NOT NULL`).Scan(&count); err != nil {
		return errors.New("count migrated Admin users")
	}
	report.Migrated["admin_users"] = count
	if err := target.QueryRow(ctx, `SELECT count(*) FROM identity_apps WHERE legacy_source_id IS NOT NULL`).Scan(&count); err != nil {
		return errors.New("count migrated applications")
	}
	report.Migrated["applications"] = count
	for _, table := range []string{"identity_credentials", "functions", "routing_rules", "queue_subscriptions", "queue_schedules", "realtime_files", "realtime_push_devices", "realtime_push_bindings", "realtime_push_outcomes"} {
		if err := target.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE legacy_source_id IS NOT NULL", table)).Scan(&count); err != nil {
			return fmt.Errorf("count migrated %s", table)
		}
		report.Migrated[table] = count
	}
	for sourceTable, targetTable := range map[string]string{
		"events":            "events",
		"deliveries":        "deliveries",
		"outbox":            "event_outbox",
		"audit_log":         "admin_audit_log",
		"delivery_attempts": "delivery_attempts",
	} {
		var migrated int64
		column := "legacy_source_id"
		if targetTable == "events" || targetTable == "deliveries" || targetTable == "event_outbox" || targetTable == "admin_audit_log" || targetTable == "delivery_attempts" {
			if err := target.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE %s IS NOT NULL", targetTable, column)).Scan(&migrated); err != nil {
				return fmt.Errorf("count migrated %s: %w", sourceTable, err)
			}
		}
		report.Migrated[sourceTable] = migrated
		var sourceCount int64
		if err := source.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", sourceTable)).Scan(&sourceCount); err != nil {
			return fmt.Errorf("recount source %s: %w", sourceTable, err)
		}
		if migrated != sourceCount {
			return fmt.Errorf("migration count mismatch for %s: source=%d target=%d", sourceTable, sourceCount, migrated)
		}
	}
	report.State = "verified"
	return nil
}

func loadLatestRun(ctx context.Context, target *pgxpool.Pool, report *Report) error {
	var id uuid.UUID
	var state string
	var raw []byte
	if err := target.QueryRow(ctx, `SELECT id,state,report FROM relayhub_migration_runs ORDER BY created_at DESC LIMIT 1`).Scan(&id, &state, &raw); err != nil {
		return errors.New("read migration ledger")
	}
	report.RunID, report.State = id.String(), state
	return nil
}

func reportJSON(report Report) []byte {
	b, _ := json.Marshal(report)
	return b
}
