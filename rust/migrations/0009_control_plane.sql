CREATE TABLE IF NOT EXISTS functions (
    id uuid PRIMARY KEY,
    workspace_id uuid NOT NULL,
    app_id uuid NOT NULL,
    name text NOT NULL CHECK (length(btrim(name)) BETWEEN 1 AND 64),
    timeout_seconds integer NOT NULL CHECK (timeout_seconds BETWEEN 1 AND 30),
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (workspace_id, app_id) REFERENCES identity_apps(workspace_id, id),
    UNIQUE (app_id, name)
);
CREATE INDEX IF NOT EXISTS functions_app ON functions(app_id, created_at, id);

CREATE TABLE IF NOT EXISTS function_invocations (
    id uuid PRIMARY KEY,
    function_id uuid NOT NULL REFERENCES functions(id),
    workspace_id uuid NOT NULL,
    idempotency_key text NOT NULL CHECK (length(btrim(idempotency_key)) BETWEEN 1 AND 256),
    input_text text NOT NULL CHECK (length(input_text) <= 65536),
    state text NOT NULL CHECK (state IN ('queued', 'succeeded', 'failed')),
    result_text text,
    error_text text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    UNIQUE (function_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS routing_rules (
    id uuid PRIMARY KEY,
    workspace_id uuid NOT NULL,
    source_app_id uuid,
    event_type text NOT NULL CHECK (length(btrim(event_type)) BETWEEN 1 AND 200),
    target_app_id uuid NOT NULL,
    realtime_channel text,
    enabled boolean NOT NULL DEFAULT true,
    deleted_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (workspace_id, target_app_id) REFERENCES identity_apps(workspace_id, id),
    FOREIGN KEY (workspace_id, source_app_id) REFERENCES identity_apps(workspace_id, id)
);
CREATE INDEX IF NOT EXISTS routing_rules_scope ON routing_rules(workspace_id, deleted_at, created_at, id);
