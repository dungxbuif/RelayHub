ALTER TABLE identity_apps
    ADD COLUMN IF NOT EXISTS delivery_mode text NOT NULL DEFAULT 'queue',
    ADD COLUMN IF NOT EXISTS callback_url text,
    ADD COLUMN IF NOT EXISTS enabled boolean NOT NULL DEFAULT true,
    ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now(),
    ADD CONSTRAINT identity_apps_delivery_mode_valid
        CHECK (delivery_mode IN ('queue', 'websocket', 'callback', 'all'));

ALTER TABLE identity_credentials
    ADD COLUMN IF NOT EXISTS hmac_secret_hash bytea;
