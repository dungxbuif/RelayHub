CREATE TABLE function_invocations (
    id text PRIMARY KEY,
    function_id text NOT NULL,
    owner_app_id text NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
    caller_app_id text NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
    idempotency_hash bytea NOT NULL,
    function_name text NOT NULL,
    input json NOT NULL,
    state text NOT NULL CHECK (state IN ('pending','reserved','claimed','success','handler_error','unavailable','timeout')),
    connection_id text,
    reply json,
    created_at timestamptz NOT NULL,
    claim_by timestamptz NOT NULL,
    deadline timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    UNIQUE (caller_app_id, idempotency_hash),
    CHECK (claim_by <= deadline),
    CHECK (deadline <= expires_at)
);

CREATE INDEX function_invocations_expiry_idx ON function_invocations (expires_at);
CREATE INDEX function_invocations_function_idx ON function_invocations (function_id, created_at DESC);
