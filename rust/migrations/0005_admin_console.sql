ALTER TABLE identity_users
    ADD COLUMN IF NOT EXISTS enabled boolean NOT NULL DEFAULT true,
    ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS identity_users_enabled_email
    ON identity_users (lower(email))
    WHERE enabled;
