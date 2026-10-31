ALTER TABLE identity_credentials
    ADD COLUMN IF NOT EXISTS legacy_api_key_hash text,
    ADD COLUMN IF NOT EXISTS encrypted_hmac_secret text;

CREATE UNIQUE INDEX IF NOT EXISTS identity_credentials_legacy_api_key
    ON identity_credentials(legacy_api_key_hash)
    WHERE legacy_api_key_hash IS NOT NULL;
