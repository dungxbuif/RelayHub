ALTER TABLE deliveries DROP CONSTRAINT deliveries_assignment_complete;

ALTER TABLE deliveries
    ADD COLUMN assignment_started_at timestamptz,
    ADD COLUMN assignment_max_expires_at timestamptz;

UPDATE deliveries
SET assignment_started_at = updated_at,
    assignment_max_expires_at = assignment_expires_at
WHERE assignment_token IS NOT NULL;

ALTER TABLE deliveries ADD CONSTRAINT deliveries_assignment_complete CHECK (
    (assigned_connection_id IS NULL AND assignment_token IS NULL
        AND assignment_expires_at IS NULL AND assignment_started_at IS NULL
        AND assignment_max_expires_at IS NULL)
    OR
    (assigned_connection_id IS NOT NULL AND assignment_token IS NOT NULL
        AND assignment_expires_at IS NOT NULL AND assignment_started_at IS NOT NULL
        AND assignment_max_expires_at IS NOT NULL
        AND assignment_started_at <= assignment_expires_at
        AND assignment_expires_at <= assignment_max_expires_at)
);

DROP INDEX deliveries_assignment_expiry_idx;
CREATE INDEX deliveries_assignment_expiry_idx
    ON deliveries (assignment_expires_at)
    WHERE assignment_token IS NOT NULL AND status NOT IN ('acked', 'dead_letter');
