-- Durable metadata for realtime file messages. Object bytes stay in the
-- configured object store; the control plane owns identity and integrity.
CREATE TABLE IF NOT EXISTS realtime_files (
    id text PRIMARY KEY,
    app_id uuid NOT NULL REFERENCES identity_apps(id) ON DELETE CASCADE,
    channel text NOT NULL CHECK (length(btrim(channel)) BETWEEN 1 AND 128),
    name text NOT NULL CHECK (length(btrim(name)) BETWEEN 1 AND 255),
    mime_type text NOT NULL CHECK (length(btrim(mime_type)) BETWEEN 1 AND 128),
    size_bytes bigint NOT NULL CHECK (size_bytes > 0 AND size_bytes <= 26214400),
    sha256 text NOT NULL CHECK (sha256 ~ '^[a-f0-9]{64}$'),
    object_key text NOT NULL UNIQUE,
    status text NOT NULL CHECK (status IN ('pending','ready','quarantined')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL,
    completed_at timestamptz,
    legacy_source_id text UNIQUE,
    CHECK (expires_at > created_at),
    CHECK ((status = 'ready' AND completed_at IS NOT NULL) OR status <> 'ready')
);
CREATE INDEX IF NOT EXISTS realtime_files_app_created_idx ON realtime_files(app_id, created_at DESC);
CREATE INDEX IF NOT EXISTS realtime_files_expiry_idx ON realtime_files(expires_at);

