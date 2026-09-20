ALTER TABLE deliveries
    ADD COLUMN callback_token text,
    ADD COLUMN callback_expires_at timestamptz,
    ADD COLUMN callback_retry_at timestamptz,
    ADD COLUMN callback_url text,
    ADD COLUMN callback_credential_version bigint,
    ADD COLUMN callback_reason text,
    ADD COLUMN callback_dlq_published_at timestamptz,
    ADD CONSTRAINT deliveries_callback_lease_complete CHECK (
        (callback_token IS NULL AND callback_expires_at IS NULL)
        OR
        (callback_token IS NOT NULL AND callback_expires_at IS NOT NULL)
    );

ALTER TABLE deliveries DROP CONSTRAINT deliveries_status_check;
ALTER TABLE deliveries ADD CONSTRAINT deliveries_status_check
    CHECK (status IN ('pending', 'dispatched', 'retrying', 'delivered', 'acked', 'dead_letter'));

CREATE INDEX deliveries_callback_retry_idx
    ON deliveries (callback_retry_at, created_at, id)
    WHERE sink = 'callback' AND status IN ('pending', 'retrying');

ALTER TABLE delivery_attempts ADD COLUMN callback_token text;
