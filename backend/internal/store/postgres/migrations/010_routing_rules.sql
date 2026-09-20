CREATE TABLE routing_rules (
    id text PRIMARY KEY,
    source_app_id text REFERENCES applications(id),
    event_type text NOT NULL,
    target_app_id text NOT NULL REFERENCES applications(id),
    realtime_channel text,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    deleted_at timestamptz,
    CHECK (event_type = btrim(event_type) AND event_type <> ''),
    CHECK (target_app_id = btrim(target_app_id) AND target_app_id <> ''),
    CHECK (realtime_channel IS NULL OR (realtime_channel = btrim(realtime_channel) AND realtime_channel <> ''))
);

CREATE INDEX routing_rules_match_idx
    ON routing_rules (event_type, source_app_id, enabled, created_at, id)
    WHERE deleted_at IS NULL;

CREATE INDEX routing_rules_target_idx
    ON routing_rules (target_app_id)
    WHERE deleted_at IS NULL;
