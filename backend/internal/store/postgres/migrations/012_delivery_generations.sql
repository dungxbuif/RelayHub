ALTER TABLE deliveries
    ADD COLUMN generation bigint NOT NULL DEFAULT 1 CHECK (generation > 0);

ALTER TABLE outbox
    ADD COLUMN generation bigint NOT NULL DEFAULT 1 CHECK (generation > 0);

ALTER TABLE delivery_attempts
    ADD COLUMN generation bigint NOT NULL DEFAULT 1 CHECK (generation > 0),
    ADD COLUMN updated_at timestamptz;

UPDATE delivery_attempts SET updated_at = created_at WHERE updated_at IS NULL;
ALTER TABLE delivery_attempts ALTER COLUMN updated_at SET NOT NULL;
ALTER TABLE delivery_attempts DROP CONSTRAINT delivery_attempts_delivery_id_attempt_key;
ALTER TABLE delivery_attempts
    ADD CONSTRAINT delivery_attempts_delivery_generation_attempt_key
    UNIQUE (delivery_id, generation, attempt);

CREATE TABLE admin_replay_requests (
    idempotency_key_hash text PRIMARY KEY CHECK (length(idempotency_key_hash) = 64),
    request_fingerprint text NOT NULL CHECK (length(request_fingerprint) = 64),
    result jsonb NOT NULL CHECK (jsonb_typeof(result) = 'object'),
    actor_id text NOT NULL,
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL CHECK (expires_at > created_at)
);

CREATE INDEX admin_replay_requests_expiry_idx ON admin_replay_requests (expires_at);

CREATE TABLE delivery_replays (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_id text NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
    from_generation bigint NOT NULL CHECK (from_generation > 0),
    generation bigint NOT NULL CHECK (generation = from_generation + 1),
    idempotency_key_hash text NOT NULL REFERENCES admin_replay_requests(idempotency_key_hash),
    actor_id text NOT NULL,
    occurred_at timestamptz NOT NULL,
    UNIQUE (delivery_id, generation)
);

CREATE INDEX delivery_replays_timeline_idx
    ON delivery_replays (delivery_id, occurred_at, id);
