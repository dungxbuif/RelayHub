CREATE TABLE events (
    id text PRIMARY KEY,
    type text NOT NULL,
    source_app_id text NOT NULL REFERENCES applications(id),
    target_app_ids text[] NOT NULL,
    data json NOT NULL CHECK (json_typeof(data) = 'object'),
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL
);

CREATE TABLE event_idempotency (
    source_app_id text NOT NULL REFERENCES applications(id),
    key_hash text NOT NULL,
    event_id text NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    publication bytea NOT NULL,
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (source_app_id, key_hash)
);

CREATE INDEX event_idempotency_expires_idx ON event_idempotency (expires_at);

CREATE TABLE deliveries (
    id text PRIMARY KEY,
    public_job_id text NOT NULL,
    event_id text NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    source_app_id text NOT NULL REFERENCES applications(id),
    target_app_id text NOT NULL REFERENCES applications(id),
    sink text NOT NULL CHECK (sink IN ('stream', 'callback')),
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'dispatched', 'retrying', 'acked', 'dead_letter')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    UNIQUE (event_id, target_app_id, sink)
);

CREATE INDEX deliveries_target_state_idx ON deliveries (target_app_id, sink, status, created_at, id);

CREATE TABLE delivery_attempts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_id text NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
    attempt integer NOT NULL CHECK (attempt > 0),
    outcome text NOT NULL,
    reason text,
    created_at timestamptz NOT NULL,
    UNIQUE (delivery_id, attempt)
);

CREATE TABLE outbox (
    id text PRIMARY KEY,
    event_id text NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    delivery_id text NOT NULL UNIQUE REFERENCES deliveries(id) ON DELETE CASCADE,
    subject text NOT NULL,
    payload bytea NOT NULL,
    message_id text NOT NULL UNIQUE,
    attempts bigint NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at timestamptz NOT NULL,
    claimed_at timestamptz,
    claim_token text,
    dispatched_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CHECK ((claimed_at IS NULL) = (claim_token IS NULL))
);

CREATE INDEX outbox_pending_idx ON outbox (available_at, created_at, id)
    WHERE dispatched_at IS NULL;
