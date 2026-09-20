ALTER TABLE queue_subscriptions
    ADD COLUMN draining_at timestamptz,
    ADD COLUMN drain_deadline_at timestamptz,
    ADD COLUMN drained_at timestamptz,
    ADD CONSTRAINT queue_subscription_drain_order CHECK (
        (draining_at IS NULL AND drain_deadline_at IS NULL AND drained_at IS NULL)
        OR (draining_at IS NOT NULL AND drain_deadline_at IS NOT NULL AND drain_deadline_at >= draining_at AND (drained_at IS NULL OR drained_at >= draining_at))
    );

ALTER TABLE queue_subscriptions ADD CONSTRAINT queue_subscriptions_app_id_id_key UNIQUE(app_id, id);

CREATE TABLE queue_schedules (
    id text PRIMARY KEY,
    app_id text NOT NULL REFERENCES applications(id),
    subscription_id text NOT NULL REFERENCES queue_subscriptions(id) ON DELETE CASCADE,
    name text NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    cron_expression text NOT NULL,
    timezone text NOT NULL,
    event_type text NOT NULL,
    data jsonb NOT NULL CHECK (jsonb_typeof(data) = 'object'),
    ordering_key text,
    priority smallint NOT NULL DEFAULT 0 CHECK (priority BETWEEN -10 AND 10),
    metadata jsonb NOT NULL DEFAULT '{}',
    next_run_at timestamptz NOT NULL,
    last_run_at timestamptz,
    claim_token text,
    claim_expires_at timestamptz,
    claim_generation bigint NOT NULL DEFAULT 0 CHECK (claim_generation >= 0),
    policy_version bigint NOT NULL DEFAULT 1 CHECK (policy_version > 0),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    UNIQUE(app_id, subscription_id, name),
    FOREIGN KEY(app_id, subscription_id) REFERENCES queue_subscriptions(app_id, id),
    CHECK ((claim_token IS NULL) = (claim_expires_at IS NULL))
);

CREATE INDEX queue_schedules_due_idx ON queue_schedules(next_run_at, id) WHERE enabled;

CREATE TABLE queue_schedule_occurrences (
    schedule_id text NOT NULL REFERENCES queue_schedules(id) ON DELETE CASCADE,
    scheduled_at timestamptz NOT NULL,
    event_id text NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    created_at timestamptz NOT NULL,
    PRIMARY KEY(schedule_id, scheduled_at),
    UNIQUE(event_id)
);
