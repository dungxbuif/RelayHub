package migrate

import (
	"context"
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
	"routing_rules", "audit_log", "realtime_files", "realtime_push_devices",
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
	for _, table := range []string{"identity_users", "identity_apps", "events", "deliveries"} {
		count, err := tableCount(ctx, target, table)
		if err != nil {
			return report, fmt.Errorf("count target %s: %w", table, err)
		}
		report.TargetCounts[table] = count
	}
	// These tables need feature-specific state translation before they can be
	// written safely into the Rust state machine. They stay visible in every
	// report and block verification rather than being silently discarded.
	for _, table := range []string{"application_credentials", "events", "deliveries", "queue_subscriptions", "queue_schedules", "functions", "routing_rules", "audit_log", "realtime_files", "realtime_push_devices"} {
		report.Unsupported[table] = report.Counts[table]
	}
	if options.Mode == "apply" {
		if err := applyAdminAndAppInventory(ctx, source, target, &report); err != nil {
			return report, err
		}
	}
	if options.Mode == "verify" {
		if err := verifyAdminAndAppInventory(ctx, target, &report); err != nil {
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
		report.Migrated["admin_users"]++
	}
	if err := rows.Err(); err != nil {
		return errors.New("iterate legacy Admin users")
	}
	var workspaceID uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM identity_workspaces w JOIN identity_users u ON u.id=w.owner_id WHERE u.is_admin ORDER BY w.created_at,w.id LIMIT 1`).Scan(&workspaceID); err != nil {
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
	report.State = "applied"
	if _, err := tx.Exec(ctx, `UPDATE relayhub_migration_runs SET state='applied',report=$2,completed_at=clock_timestamp() WHERE id=$1`, runID, reportJSON(*report)); err != nil {
		return errors.New("complete migration ledger row")
	}
	if err := tx.Commit(ctx); err != nil {
		return errors.New("commit target migration")
	}
	return nil
}

func verifyAdminAndAppInventory(ctx context.Context, target *pgxpool.Pool, report *Report) error {
	var count int64
	if err := target.QueryRow(ctx, `SELECT count(*) FROM identity_users WHERE legacy_source_id IS NOT NULL`).Scan(&count); err != nil {
		return errors.New("count migrated Admin users")
	}
	report.Migrated["admin_users"] = count
	if err := target.QueryRow(ctx, `SELECT count(*) FROM identity_apps WHERE legacy_source_id IS NOT NULL`).Scan(&count); err != nil {
		return errors.New("count migrated applications")
	}
	report.Migrated["applications"] = count
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
