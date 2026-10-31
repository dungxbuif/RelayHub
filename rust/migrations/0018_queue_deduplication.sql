CREATE TABLE IF NOT EXISTS queue_deduplication_keys (
    subscription_id uuid NOT NULL REFERENCES queue_subscriptions(id) ON DELETE CASCADE,
    key_hash text NOT NULL CHECK (length(key_hash) = 64),
    event_id uuid NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (subscription_id, key_hash)
);
CREATE INDEX IF NOT EXISTS queue_deduplication_expiry_idx
    ON queue_deduplication_keys (expires_at);
