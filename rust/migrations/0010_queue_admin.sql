CREATE TABLE IF NOT EXISTS queue_subscriptions (
    id uuid PRIMARY KEY,
    workspace_id uuid NOT NULL,
    app_id uuid NOT NULL,
    name text NOT NULL CHECK (length(btrim(name)) BETWEEN 1 AND 64),
    enabled boolean NOT NULL DEFAULT true,
    paused_at timestamptz,
    draining_at timestamptz,
    drained_at timestamptz,
    max_in_flight integer NOT NULL DEFAULT 10 CHECK (max_in_flight BETWEEN 1 AND 10000),
    max_batch_size integer NOT NULL DEFAULT 10 CHECK (max_batch_size BETWEEN 1 AND 100),
    ordering_mode text NOT NULL DEFAULT 'none' CHECK (ordering_mode IN ('none','key')),
    max_dispatch_rate integer,
    success_callback_url text,
    failure_callback_url text,
    policy_version integer NOT NULL DEFAULT 1 CHECK (policy_version > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (workspace_id, app_id) REFERENCES identity_apps(workspace_id, id),
    UNIQUE (app_id, name)
);
CREATE INDEX IF NOT EXISTS queue_subscriptions_app ON queue_subscriptions(app_id, created_at, id);

CREATE TABLE IF NOT EXISTS queue_schedules (
    id uuid PRIMARY KEY,
    workspace_id uuid NOT NULL,
    app_id uuid NOT NULL,
    subscription_id uuid NOT NULL REFERENCES queue_subscriptions(id) ON DELETE CASCADE,
    name text NOT NULL CHECK (length(btrim(name)) BETWEEN 1 AND 64),
    enabled boolean NOT NULL DEFAULT true,
    cron_expression text NOT NULL CHECK (length(btrim(cron_expression)) BETWEEN 1 AND 128),
    timezone text NOT NULL DEFAULT 'UTC' CHECK (length(btrim(timezone)) BETWEEN 1 AND 64),
    event_type text NOT NULL CHECK (length(btrim(event_type)) BETWEEN 1 AND 200),
    data_text text NOT NULL DEFAULT '{}',
    ordering_key text,
    priority integer NOT NULL DEFAULT 0,
    metadata_text text NOT NULL DEFAULT '{}',
    next_run_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_run_at timestamptz,
    policy_version integer NOT NULL DEFAULT 1 CHECK (policy_version > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (workspace_id, app_id) REFERENCES identity_apps(workspace_id, id),
    UNIQUE (subscription_id, name)
);
CREATE INDEX IF NOT EXISTS queue_schedules_subscription ON queue_schedules(subscription_id, created_at, id);
