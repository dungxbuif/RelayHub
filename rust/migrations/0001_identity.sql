CREATE TABLE identity_users (
    id uuid PRIMARY KEY,
    email text NOT NULL UNIQUE CHECK (email = lower(email)),
    is_admin boolean NOT NULL DEFAULT false,
    password_hash text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE identity_workspaces (
    id uuid PRIMARY KEY,
    owner_id uuid NOT NULL UNIQUE REFERENCES identity_users(id),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE identity_setup_tokens (
    user_id uuid PRIMARY KEY REFERENCES identity_users(id),
    token_hash bytea NOT NULL UNIQUE CHECK (octet_length(token_hash) = 32),
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    consumed_at timestamptz,
    CHECK (expires_at = created_at + interval '24 hours')
);

CREATE TABLE identity_sessions (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES identity_users(id),
    token_hash bytea NOT NULL UNIQUE CHECK (octet_length(token_hash) = 32),
    csrf_hash bytea NOT NULL CHECK (octet_length(csrf_hash) = 32),
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz
);
CREATE INDEX identity_sessions_user ON identity_sessions(user_id);

CREATE TABLE identity_apps (
    id uuid PRIMARY KEY,
    workspace_id uuid NOT NULL REFERENCES identity_workspaces(id),
    name text NOT NULL CHECK (length(btrim(name)) BETWEEN 1 AND 200),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, id)
);

CREATE TABLE identity_credentials (
    id uuid PRIMARY KEY,
    workspace_id uuid NOT NULL,
    app_id uuid NOT NULL,
    kind text NOT NULL CHECK (kind IN ('application', 'agent')),
    agent_id uuid,
    token_hash bytea NOT NULL UNIQUE CHECK (octet_length(token_hash) = 32),
    created_at timestamptz NOT NULL DEFAULT now(),
    revoked_at timestamptz,
    FOREIGN KEY (workspace_id, app_id) REFERENCES identity_apps(workspace_id, id),
    CHECK ((kind = 'agent') = (agent_id IS NOT NULL))
);
CREATE INDEX identity_credentials_app ON identity_credentials(workspace_id, app_id);
