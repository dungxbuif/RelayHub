CREATE TABLE realtime_files (
    id text PRIMARY KEY,
    app_id text NOT NULL REFERENCES applications(id),
    channel text NOT NULL,
    name text NOT NULL,
    mime_type text NOT NULL,
    size_bytes bigint NOT NULL CHECK (size_bytes > 0 AND size_bytes <= 26214400),
    sha256 text NOT NULL CHECK (sha256 ~ '^[a-f0-9]{64}$'),
    object_key text NOT NULL UNIQUE,
    status text NOT NULL CHECK (status IN ('pending', 'ready', 'quarantined')),
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    completed_at timestamptz,
    CHECK (expires_at > created_at),
    CHECK ((status = 'ready' AND completed_at IS NOT NULL) OR status <> 'ready')
);
CREATE INDEX realtime_files_app_created_idx ON realtime_files(app_id, created_at DESC);
CREATE INDEX realtime_files_expiry_idx ON realtime_files(expires_at);
