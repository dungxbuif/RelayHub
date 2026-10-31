-- Preserve legacy Go event/delivery identities and fan-out metadata while the
-- Rust state machine becomes authoritative.  The legacy IDs remain a
-- crosswalk, not a second source of truth.
ALTER TABLE events
    ADD COLUMN IF NOT EXISTS legacy_source_id text,
    ADD COLUMN IF NOT EXISTS legacy_target_app_ids text[];

CREATE UNIQUE INDEX IF NOT EXISTS events_legacy_source
    ON events (legacy_source_id)
    WHERE legacy_source_id IS NOT NULL;

ALTER TABLE deliveries
    ADD COLUMN IF NOT EXISTS target_app_id uuid,
    ADD COLUMN IF NOT EXISTS legacy_source_id text,
    ADD COLUMN IF NOT EXISTS legacy_public_job_id text,
    ADD COLUMN IF NOT EXISTS legacy_sink text;

UPDATE deliveries SET target_app_id = app_id WHERE target_app_id IS NULL;
CREATE OR REPLACE FUNCTION default_delivery_target() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.target_app_id IS NULL THEN
        NEW.target_app_id := NEW.app_id;
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS deliveries_target_default ON deliveries;
CREATE TRIGGER deliveries_target_default
    BEFORE INSERT ON deliveries
    FOR EACH ROW EXECUTE FUNCTION default_delivery_target();
ALTER TABLE deliveries ALTER COLUMN target_app_id SET NOT NULL;
ALTER TABLE deliveries
    DROP CONSTRAINT IF EXISTS deliveries_workspace_id_app_id_id_fkey;
ALTER TABLE deliveries
    ADD CONSTRAINT deliveries_target_app_fk
    FOREIGN KEY (workspace_id, target_app_id) REFERENCES identity_apps(workspace_id, id);

CREATE UNIQUE INDEX IF NOT EXISTS deliveries_legacy_source
    ON deliveries (legacy_source_id)
    WHERE legacy_source_id IS NOT NULL;

-- Legacy Go fan-out created one delivery per target.  The Rust schema's
-- original event/generation key would collapse those rows, so target is part
-- of the generation identity after this migration.
ALTER TABLE deliveries
    DROP CONSTRAINT IF EXISTS deliveries_event_id_generation_key;
CREATE UNIQUE INDEX IF NOT EXISTS deliveries_event_target_generation
    ON deliveries (event_id, target_app_id, generation);

ALTER TABLE event_outbox
    ADD COLUMN IF NOT EXISTS legacy_source_id text,
    ADD COLUMN IF NOT EXISTS legacy_dispatched_at timestamptz,
    ADD COLUMN IF NOT EXISTS legacy_last_error text;

CREATE UNIQUE INDEX IF NOT EXISTS event_outbox_legacy_source
    ON event_outbox (legacy_source_id)
    WHERE legacy_source_id IS NOT NULL;

ALTER TABLE admin_audit_log
    ADD COLUMN IF NOT EXISTS legacy_source_id text;
CREATE UNIQUE INDEX IF NOT EXISTS admin_audit_log_legacy_source
    ON admin_audit_log (legacy_source_id)
    WHERE legacy_source_id IS NOT NULL;

ALTER TABLE delivery_attempts
    ADD COLUMN IF NOT EXISTS legacy_source_id text;
CREATE UNIQUE INDEX IF NOT EXISTS delivery_attempts_legacy_source
    ON delivery_attempts (legacy_source_id)
    WHERE legacy_source_id IS NOT NULL;
