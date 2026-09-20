CREATE TABLE applications (
    id text PRIMARY KEY,
    name text NOT NULL,
    callback_url text,
    delivery_mode text NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE application_credentials (
    app_id text PRIMARY KEY REFERENCES applications(id) ON DELETE CASCADE,
    api_key_hash text NOT NULL UNIQUE,
    encrypted_hmac_secret text NOT NULL,
    created_at timestamptz NOT NULL,
    rotated_at timestamptz NOT NULL
);

CREATE TABLE callback_endpoints (
    app_id text PRIMARY KEY REFERENCES applications(id) ON DELETE CASCADE,
    url text NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE functions (
    id text PRIMARY KEY,
    app_id text NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
    name text NOT NULL,
    timeout_seconds integer NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    UNIQUE (app_id, name)
);

CREATE TABLE admin_sessions (
    id_hash text PRIMARY KEY,
    csrf_hash text NOT NULL,
    created_at timestamptz NOT NULL,
    idle_expires_at timestamptz NOT NULL,
    absolute_expires_at timestamptz NOT NULL,
    revoked_at timestamptz
);

CREATE TABLE audit_log (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    occurred_at timestamptz NOT NULL,
    actor_type text NOT NULL,
    actor_id text,
    action text NOT NULL,
    resource_type text NOT NULL,
    resource_id text,
    outcome text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX audit_log_occurred_at_idx ON audit_log (occurred_at DESC, id DESC);

