ALTER TABLE deliveries
    ADD COLUMN assigned_connection_id text,
    ADD COLUMN assignment_token text,
    ADD COLUMN assignment_expires_at timestamptz,
    ADD CONSTRAINT deliveries_assignment_complete CHECK (
        (assigned_connection_id IS NULL AND assignment_token IS NULL AND assignment_expires_at IS NULL)
        OR
        (assigned_connection_id IS NOT NULL AND assignment_token IS NOT NULL AND assignment_expires_at IS NOT NULL)
    );

CREATE INDEX deliveries_assignment_expiry_idx
    ON deliveries (assignment_expires_at)
    WHERE assignment_token IS NOT NULL;

ALTER TABLE outbox ADD COLUMN failed_at timestamptz;
DROP INDEX outbox_pending_idx;
CREATE INDEX outbox_pending_idx ON outbox (available_at, created_at, id)
    WHERE dispatched_at IS NULL AND failed_at IS NULL;
