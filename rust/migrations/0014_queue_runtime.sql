ALTER TABLE queue_subscriptions
    ADD COLUMN IF NOT EXISTS drain_deadline_at timestamptz;
CREATE UNIQUE INDEX IF NOT EXISTS queue_subscriptions_id_app
    ON queue_subscriptions(id, app_id);

CREATE TABLE IF NOT EXISTS queue_deliveries (
    id uuid PRIMARY KEY,
    subscription_id uuid NOT NULL REFERENCES queue_subscriptions(id) ON DELETE CASCADE,
    app_id uuid NOT NULL,
    event_id uuid NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    generation integer NOT NULL DEFAULT 1 CHECK (generation > 0),
    status text NOT NULL DEFAULT 'available' CHECK (status IN ('available','in_flight','acked','dead_letter')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at timestamptz NOT NULL,
    receipt_hash bytea,
    lease_started_at timestamptz,
    lease_expires_at timestamptz,
    lease_max_expires_at timestamptz,
    ordering_key text,
    priority integer NOT NULL DEFAULT 0 CHECK (priority BETWEEN -10 AND 10),
    metadata jsonb NOT NULL DEFAULT '{}',
    last_reason text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL,
    legacy_source_id text,
    FOREIGN KEY (subscription_id, app_id) REFERENCES queue_subscriptions(id, app_id),
    UNIQUE (subscription_id, event_id),
    CHECK ((status = 'in_flight') = (receipt_hash IS NOT NULL)),
    CHECK ((receipt_hash IS NULL) = (lease_started_at IS NULL)),
    CHECK ((receipt_hash IS NULL) = (lease_expires_at IS NULL)),
    CHECK ((receipt_hash IS NULL) = (lease_max_expires_at IS NULL))
);
CREATE INDEX IF NOT EXISTS queue_deliveries_pull_idx
    ON queue_deliveries(subscription_id, status, available_at, priority DESC, created_at, id);
CREATE INDEX IF NOT EXISTS queue_deliveries_dlq_idx
    ON queue_deliveries(subscription_id, updated_at DESC, id)
    WHERE status = 'dead_letter';
CREATE UNIQUE INDEX IF NOT EXISTS queue_deliveries_legacy_source
    ON queue_deliveries(legacy_source_id)
    WHERE legacy_source_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS queue_delivery_attempts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_id uuid NOT NULL REFERENCES queue_deliveries(id) ON DELETE CASCADE,
    generation integer NOT NULL CHECK (generation > 0),
    attempt integer NOT NULL CHECK (attempt > 0),
    outcome text NOT NULL,
    reason text,
    occurred_at timestamptz NOT NULL,
    legacy_source_id text,
    UNIQUE (delivery_id, generation, attempt, outcome)
);
CREATE UNIQUE INDEX IF NOT EXISTS queue_delivery_attempts_legacy_source
    ON queue_delivery_attempts(legacy_source_id)
    WHERE legacy_source_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS queue_result_callbacks (
    id uuid PRIMARY KEY,
    app_id uuid NOT NULL,
    subscription_id uuid NOT NULL REFERENCES queue_subscriptions(id) ON DELETE CASCADE,
    delivery_id uuid NOT NULL REFERENCES queue_deliveries(id) ON DELETE CASCADE,
    event_id uuid NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    generation integer NOT NULL,
    outcome text NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    reason text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS queue_result_callbacks_subscription
    ON queue_result_callbacks(subscription_id, updated_at DESC, id);
