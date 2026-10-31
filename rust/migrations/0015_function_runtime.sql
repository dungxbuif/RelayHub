CREATE TABLE IF NOT EXISTS function_runtime_claims (
    invocation_id uuid PRIMARY KEY REFERENCES function_invocations(id) ON DELETE CASCADE,
    app_id uuid NOT NULL REFERENCES identity_apps(id) ON DELETE CASCADE,
    connection_id text NOT NULL CHECK (length(connection_id) BETWEEN 1 AND 128),
    claimed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (app_id, connection_id, invocation_id)
);
CREATE INDEX IF NOT EXISTS function_runtime_claims_app
    ON function_runtime_claims(app_id, claimed_at, invocation_id);
