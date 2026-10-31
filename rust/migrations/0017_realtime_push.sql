-- Push registration is durable and app-scoped. Provider delivery is an
-- adapter concern; the control plane never returns the encrypted token.
CREATE TABLE IF NOT EXISTS realtime_push_devices (
    id uuid PRIMARY KEY,
    app_id uuid NOT NULL REFERENCES identity_apps(id) ON DELETE CASCADE,
    provider text NOT NULL CHECK (provider IN ('apns','fcm')),
    token_hash bytea NOT NULL CHECK (octet_length(token_hash) = 32),
    encrypted_token text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    legacy_source_id text UNIQUE,
    UNIQUE(app_id, provider, token_hash),
    UNIQUE(id, app_id)
);
CREATE TABLE IF NOT EXISTS realtime_push_bindings (
    app_id uuid NOT NULL REFERENCES identity_apps(id) ON DELETE CASCADE,
    channel text NOT NULL CHECK (length(btrim(channel)) BETWEEN 1 AND 128),
    device_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    legacy_source_id text UNIQUE,
    PRIMARY KEY(app_id, channel, device_id),
    FOREIGN KEY(device_id, app_id) REFERENCES realtime_push_devices(id, app_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS realtime_push_bindings_channel_idx ON realtime_push_bindings(app_id, channel);
CREATE TABLE IF NOT EXISTS realtime_push_outcomes (
    id uuid PRIMARY KEY,
    app_id uuid NOT NULL REFERENCES identity_apps(id) ON DELETE CASCADE,
    device_id uuid NOT NULL,
    channel text NOT NULL,
    provider text NOT NULL,
    status text NOT NULL CHECK(status IN ('delivered','failed')),
    provider_message_id text,
    reason text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
    ,legacy_source_id text UNIQUE
);
CREATE INDEX IF NOT EXISTS realtime_push_outcomes_app_created_idx ON realtime_push_outcomes(app_id, created_at DESC);
