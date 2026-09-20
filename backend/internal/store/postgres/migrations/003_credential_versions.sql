ALTER TABLE application_credentials
    ADD COLUMN version bigint NOT NULL DEFAULT 1,
    ADD COLUMN revoked_at timestamptz;

ALTER TABLE application_credentials DROP CONSTRAINT application_credentials_pkey;
ALTER TABLE application_credentials ADD PRIMARY KEY (app_id, version);
CREATE UNIQUE INDEX application_credentials_active_app_idx
    ON application_credentials (app_id) WHERE revoked_at IS NULL;

