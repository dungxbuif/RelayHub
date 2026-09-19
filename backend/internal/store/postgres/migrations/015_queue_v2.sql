ALTER TABLE events
    ADD COLUMN queue_available_at timestamptz,
    ADD COLUMN queue_ordering_key text,
    ADD COLUMN queue_priority smallint NOT NULL DEFAULT 0 CHECK (queue_priority BETWEEN -10 AND 10),
    ADD COLUMN queue_metadata jsonb NOT NULL DEFAULT '{}';

CREATE TABLE queue_subscriptions (
    id text PRIMARY KEY,
    app_id text NOT NULL REFERENCES applications(id),
    name text NOT NULL,
    mode text NOT NULL DEFAULT 'pull' CHECK (mode = 'pull'),
    enabled boolean NOT NULL DEFAULT true,
    paused_at timestamptz,
    event_types text[] NOT NULL DEFAULT '{}',
    max_attempts integer NOT NULL DEFAULT 10 CHECK (max_attempts BETWEEN 1 AND 100),
    default_visibility_seconds integer NOT NULL DEFAULT 60 CHECK (default_visibility_seconds BETWEEN 1 AND 900),
    max_visibility_seconds integer NOT NULL DEFAULT 300 CHECK (max_visibility_seconds BETWEEN default_visibility_seconds AND 3600),
    max_total_lease_seconds integer NOT NULL DEFAULT 3600 CHECK (max_total_lease_seconds BETWEEN max_visibility_seconds AND 86400),
    retention_seconds integer NOT NULL DEFAULT 604800 CHECK (retention_seconds BETWEEN 60 AND 2592000),
    max_in_flight integer NOT NULL DEFAULT 100 CHECK (max_in_flight BETWEEN 1 AND 10000),
    max_batch_size integer NOT NULL DEFAULT 20 CHECK (max_batch_size BETWEEN 1 AND 100),
    retry_delay_seconds integer NOT NULL DEFAULT 5 CHECK (retry_delay_seconds BETWEEN 0 AND 86400),
    ordering_mode text NOT NULL DEFAULT 'none' CHECK (ordering_mode IN ('none','key')),
    deduplication_seconds integer NOT NULL DEFAULT 0 CHECK (deduplication_seconds BETWEEN 0 AND 86400),
    max_dispatch_rate integer CHECK (max_dispatch_rate BETWEEN 1 AND 100000),
    policy_version bigint NOT NULL DEFAULT 1 CHECK (policy_version > 0),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    UNIQUE (app_id, name),
    CHECK ((paused_at IS NULL) OR enabled)
);

CREATE INDEX queue_subscriptions_app_idx ON queue_subscriptions (app_id, created_at, id);

CREATE TABLE queue_deduplication_keys (
    subscription_id text NOT NULL REFERENCES queue_subscriptions(id) ON DELETE CASCADE,
    key_hash text NOT NULL,
    event_id text NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (subscription_id, key_hash)
);

CREATE INDEX queue_deduplication_expiry_idx ON queue_deduplication_keys (expires_at);

CREATE TABLE queue_deliveries (
    id text PRIMARY KEY,
    subscription_id text NOT NULL REFERENCES queue_subscriptions(id) ON DELETE CASCADE,
    app_id text NOT NULL REFERENCES applications(id),
    event_id text NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    generation bigint NOT NULL DEFAULT 1 CHECK (generation > 0),
    status text NOT NULL DEFAULT 'available' CHECK (status IN ('available','in_flight','acked','dead_letter')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at timestamptz NOT NULL,
    receipt_hash text,
    lease_started_at timestamptz,
    lease_expires_at timestamptz,
    lease_max_expires_at timestamptz,
    ordering_key text,
    priority smallint NOT NULL DEFAULT 0 CHECK (priority BETWEEN -10 AND 10),
    metadata jsonb NOT NULL DEFAULT '{}',
    last_reason text,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    UNIQUE (subscription_id, event_id),
    CHECK ((status = 'in_flight') = (receipt_hash IS NOT NULL)),
    CHECK ((receipt_hash IS NULL) = (lease_started_at IS NULL)),
    CHECK ((receipt_hash IS NULL) = (lease_expires_at IS NULL)),
    CHECK ((receipt_hash IS NULL) = (lease_max_expires_at IS NULL))
);

CREATE INDEX queue_deliveries_pull_idx ON queue_deliveries
    (subscription_id, status, available_at, priority DESC, created_at, id);
CREATE INDEX queue_deliveries_lease_idx ON queue_deliveries (lease_expires_at)
    WHERE status = 'in_flight';
CREATE INDEX queue_deliveries_dlq_idx ON queue_deliveries
    (subscription_id, updated_at DESC, id) WHERE status = 'dead_letter';

CREATE TABLE queue_delivery_attempts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_id text NOT NULL REFERENCES queue_deliveries(id) ON DELETE CASCADE,
    generation bigint NOT NULL CHECK (generation > 0),
    attempt integer NOT NULL CHECK (attempt > 0),
    outcome text NOT NULL,
    reason text,
    occurred_at timestamptz NOT NULL,
    UNIQUE (delivery_id, generation, attempt, outcome)
);

CREATE INDEX queue_delivery_attempts_dispatch_idx ON queue_delivery_attempts (occurred_at, delivery_id)
    WHERE outcome = 'leased';
