CREATE TABLE realtime_push_devices (
    id text PRIMARY KEY,
    app_id text NOT NULL REFERENCES applications(id),
    provider text NOT NULL CHECK (provider IN ('apns','fcm')),
    token_hash text NOT NULL,
    encrypted_token text NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    UNIQUE(app_id,provider,token_hash),
    UNIQUE(id,app_id)
);
CREATE TABLE realtime_push_bindings (
    app_id text NOT NULL,
    channel text NOT NULL,
    device_id text NOT NULL,
    created_at timestamptz NOT NULL,
    PRIMARY KEY(app_id,channel,device_id),
    FOREIGN KEY(device_id,app_id) REFERENCES realtime_push_devices(id,app_id) ON DELETE CASCADE
);
CREATE INDEX realtime_push_bindings_channel_idx ON realtime_push_bindings(app_id,channel);
CREATE TABLE realtime_push_outcomes (
    id text PRIMARY KEY,
    app_id text NOT NULL REFERENCES applications(id),
    device_id text NOT NULL,
    channel text NOT NULL,
    provider text NOT NULL,
    status text NOT NULL CHECK(status IN ('delivered','failed')),
    provider_message_id text,
    reason text,
    created_at timestamptz NOT NULL
);
CREATE INDEX realtime_push_outcomes_app_created_idx ON realtime_push_outcomes(app_id,created_at DESC);
