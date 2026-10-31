CREATE TABLE IF NOT EXISTS admin_audit_log (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    workspace_id uuid REFERENCES identity_workspaces(id),
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    actor_type text NOT NULL CHECK (length(actor_type) BETWEEN 1 AND 64),
    actor_id text,
    action text NOT NULL CHECK (length(action) BETWEEN 1 AND 128),
    resource_type text NOT NULL CHECK (length(resource_type) BETWEEN 1 AND 64),
    resource_id text,
    outcome text NOT NULL CHECK (length(outcome) BETWEEN 1 AND 64),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    CHECK (jsonb_typeof(metadata) = 'object')
);
CREATE INDEX IF NOT EXISTS admin_audit_log_scope_time
    ON admin_audit_log (workspace_id, occurred_at DESC, id DESC);
