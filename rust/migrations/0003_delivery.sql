-- Add lease fencing to Task 3's authoritative delivery, preserving its quota trigger.
ALTER TABLE deliveries
    ADD COLUMN attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count BETWEEN 0 AND 5),
    ADD COLUMN attempt_token uuid,
    ADD COLUMN agent_id uuid,
    ADD COLUMN attempt_started_at timestamptz,
    ADD COLUMN lease_expires_at timestamptz,
    ADD COLUMN next_attempt_at timestamptz;
CREATE INDEX deliveries_agent_leases ON deliveries(workspace_id, app_id, agent_id, lease_expires_at)
    WHERE state = 'in_flight';

CREATE TABLE delivery_attempts (
    delivery_id uuid NOT NULL REFERENCES deliveries(id),
    attempt integer NOT NULL CHECK (attempt BETWEEN 1 AND 5),
    attempt_token uuid NOT NULL UNIQUE,
    agent_id uuid NOT NULL,
    started_at timestamptz NOT NULL,
    lease_expires_at timestamptz NOT NULL,
    finished_at timestamptz,
    outcome text CHECK (outcome IN ('success', 'retryable_failure', 'lease_expired')),
    PRIMARY KEY (delivery_id, attempt),
    CHECK ((finished_at IS NULL) = (outcome IS NULL))
);

CREATE TABLE delivery_replays (
    delivery_id uuid PRIMARY KEY REFERENCES deliveries(id),
    source_delivery_id uuid NOT NULL REFERENCES deliveries(id),
    actor_user_id uuid NOT NULL REFERENCES identity_users(id),
    created_at timestamptz NOT NULL DEFAULT now()
);
