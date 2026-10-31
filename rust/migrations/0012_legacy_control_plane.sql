-- Crosswalk fields keep legacy IDs visible while the Rust control plane owns
-- the durable rows. Runtime payload/delivery state is migrated separately.
ALTER TABLE identity_credentials
    ADD COLUMN IF NOT EXISTS legacy_source_id text;
CREATE UNIQUE INDEX IF NOT EXISTS identity_credentials_legacy_source
    ON identity_credentials (legacy_source_id)
    WHERE legacy_source_id IS NOT NULL;

ALTER TABLE functions
    ADD COLUMN IF NOT EXISTS legacy_source_id text;
CREATE UNIQUE INDEX IF NOT EXISTS functions_legacy_source
    ON functions (legacy_source_id)
    WHERE legacy_source_id IS NOT NULL;

ALTER TABLE routing_rules
    ADD COLUMN IF NOT EXISTS legacy_source_id text;
CREATE UNIQUE INDEX IF NOT EXISTS routing_rules_legacy_source
    ON routing_rules (legacy_source_id)
    WHERE legacy_source_id IS NOT NULL;

ALTER TABLE queue_subscriptions
    ADD COLUMN IF NOT EXISTS legacy_source_id text,
    ADD COLUMN IF NOT EXISTS mode text NOT NULL DEFAULT 'pull',
    ADD COLUMN IF NOT EXISTS event_types text[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS max_attempts integer NOT NULL DEFAULT 10,
    ADD COLUMN IF NOT EXISTS default_visibility_seconds integer NOT NULL DEFAULT 60,
    ADD COLUMN IF NOT EXISTS max_visibility_seconds integer NOT NULL DEFAULT 300,
    ADD COLUMN IF NOT EXISTS max_total_lease_seconds integer NOT NULL DEFAULT 3600,
    ADD COLUMN IF NOT EXISTS retention_seconds integer NOT NULL DEFAULT 604800,
    ADD COLUMN IF NOT EXISTS retry_delay_seconds integer NOT NULL DEFAULT 5,
    ADD COLUMN IF NOT EXISTS deduplication_seconds integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS result_callback_metadata text NOT NULL DEFAULT '{}';
CREATE UNIQUE INDEX IF NOT EXISTS queue_subscriptions_legacy_source
    ON queue_subscriptions (legacy_source_id)
    WHERE legacy_source_id IS NOT NULL;

ALTER TABLE queue_schedules
    ADD COLUMN IF NOT EXISTS legacy_source_id text;
CREATE UNIQUE INDEX IF NOT EXISTS queue_schedules_legacy_source
    ON queue_schedules (legacy_source_id)
    WHERE legacy_source_id IS NOT NULL;
