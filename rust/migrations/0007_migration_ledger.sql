ALTER TABLE identity_users
    ADD COLUMN IF NOT EXISTS legacy_source_id text,
    ADD COLUMN IF NOT EXISTS legacy_password_hash text;

CREATE UNIQUE INDEX IF NOT EXISTS identity_users_legacy_source
    ON identity_users (legacy_source_id);

ALTER TABLE identity_apps
    ADD COLUMN IF NOT EXISTS legacy_source_id text;

CREATE UNIQUE INDEX IF NOT EXISTS identity_apps_legacy_source
    ON identity_apps (legacy_source_id);

CREATE TABLE IF NOT EXISTS relayhub_migration_runs (
    id uuid PRIMARY KEY,
    source_system text NOT NULL,
    mode text NOT NULL CHECK (mode IN ('plan', 'apply', 'verify', 'rollback-report')),
    state text NOT NULL CHECK (state IN ('planned', 'applied', 'verified', 'failed')),
    report jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz
);
