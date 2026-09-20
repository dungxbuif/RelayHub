ALTER TABLE queue_subscriptions
    ADD COLUMN success_callback_url text,
    ADD COLUMN failure_callback_url text,
    ADD COLUMN result_callback_metadata jsonb NOT NULL DEFAULT '{}';

CREATE TABLE queue_result_callbacks (
    id text PRIMARY KEY,
    app_id text NOT NULL REFERENCES applications(id),
    subscription_id text NOT NULL REFERENCES queue_subscriptions(id) ON DELETE CASCADE,
    delivery_id text NOT NULL,
    event_id text NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    outcome text NOT NULL CHECK (outcome IN ('success','failure')),
    url text NOT NULL,
    payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','delivered','failed')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 6),
    available_at timestamptz NOT NULL,
    claim_token text,
    claim_expires_at timestamptz,
    claim_generation bigint NOT NULL DEFAULT 0 CHECK (claim_generation >= 0),
    last_reason text,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    UNIQUE(delivery_id,generation,outcome),
    CHECK ((claim_token IS NULL) = (claim_expires_at IS NULL))
);

CREATE INDEX queue_result_callbacks_pending_idx ON queue_result_callbacks(available_at,id) WHERE status='pending';

CREATE TABLE queue_result_callback_attempts (
    callback_id text NOT NULL REFERENCES queue_result_callbacks(id) ON DELETE CASCADE,
    attempt integer NOT NULL CHECK (attempt BETWEEN 1 AND 6),
    claim_generation bigint NOT NULL CHECK (claim_generation > 0),
    outcome text NOT NULL,
    reason text,
    occurred_at timestamptz NOT NULL,
    PRIMARY KEY(callback_id,attempt)
);
