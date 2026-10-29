-- Task 5 extends this ownership relation with endpoint policy and authentication.
CREATE TABLE event_endpoints (
    id uuid PRIMARY KEY,
    workspace_id uuid NOT NULL,
    app_id uuid NOT NULL,
    FOREIGN KEY (workspace_id, app_id) REFERENCES identity_apps(workspace_id, id),
    UNIQUE (workspace_id, app_id, id)
);

CREATE TABLE workspace_quotas (
    workspace_id uuid PRIMARY KEY REFERENCES identity_workspaces(id),
    outstanding_deliveries bigint NOT NULL DEFAULT 0 CHECK (outstanding_deliveries BETWEEN 0 AND 10000),
    outstanding_payload_bytes bigint NOT NULL DEFAULT 0 CHECK (outstanding_payload_bytes BETWEEN 0 AND 104857600)
);

CREATE TABLE events (
    id uuid PRIMARY KEY,
    workspace_id uuid NOT NULL,
    app_id uuid NOT NULL,
    endpoint_id uuid NOT NULL,
    idempotency_key text NOT NULL CHECK (octet_length(idempotency_key) BETWEEN 1 AND 256),
    payload bytea NOT NULL CHECK (octet_length(payload) <= 2097152),
    payload_hash bytea NOT NULL CHECK (octet_length(payload_hash) = 32),
    payload_bytes bigint NOT NULL CHECK (payload_bytes BETWEEN 0 AND 1048576),
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL DEFAULT now() + interval '7 days',
    FOREIGN KEY (workspace_id, app_id, endpoint_id) REFERENCES event_endpoints(workspace_id, app_id, id),
    UNIQUE (workspace_id, app_id, id),
    UNIQUE (workspace_id, app_id, idempotency_key),
    CHECK (expires_at > created_at)
);
CREATE INDEX events_expiry ON events(expires_at);

CREATE TABLE deliveries (
    id uuid PRIMARY KEY,
    event_id uuid NOT NULL,
    workspace_id uuid NOT NULL,
    app_id uuid NOT NULL,
    generation integer NOT NULL DEFAULT 1 CHECK (generation > 0),
    state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'in_flight', 'retry_wait', 'succeeded', 'dlq', 'expired', 'cancelled')),
    payload_bytes bigint NOT NULL CHECK (payload_bytes BETWEEN 0 AND 1048576),
    created_at timestamptz NOT NULL DEFAULT now(),
    terminal_at timestamptz,
    FOREIGN KEY (workspace_id, app_id, event_id) REFERENCES events(workspace_id, app_id, id),
    UNIQUE (event_id, generation),
    UNIQUE (workspace_id, app_id, id)
);
CREATE INDEX deliveries_outstanding ON deliveries(event_id) WHERE terminal_at IS NULL;

CREATE TABLE event_outbox (
    delivery_id uuid PRIMARY KEY,
    workspace_id uuid NOT NULL,
    app_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    sent_at timestamptz,
    discarded_at timestamptz,
    FOREIGN KEY (workspace_id, app_id, delivery_id) REFERENCES deliveries(workspace_id, app_id, id),
    CHECK (sent_at IS NULL OR discarded_at IS NULL)
);
CREATE INDEX event_outbox_pending ON event_outbox(created_at, delivery_id)
    WHERE sent_at IS NULL AND discarded_at IS NULL;

-- Every caller (including future settlement/replay) uses the same quota transition.
CREATE FUNCTION account_delivery() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'pending' OR NEW.terminal_at IS NOT NULL THEN
            RAISE EXCEPTION 'new delivery must be pending';
        END IF;
        SELECT payload_bytes INTO STRICT NEW.payload_bytes FROM events
            WHERE id = NEW.event_id AND workspace_id = NEW.workspace_id AND app_id = NEW.app_id;
        INSERT INTO workspace_quotas (workspace_id) VALUES (NEW.workspace_id) ON CONFLICT DO NOTHING;
        UPDATE workspace_quotas SET outstanding_deliveries = outstanding_deliveries + 1,
            outstanding_payload_bytes = outstanding_payload_bytes + NEW.payload_bytes
            WHERE workspace_id = NEW.workspace_id AND outstanding_deliveries < 10000
              AND outstanding_payload_bytes + NEW.payload_bytes <= 104857600;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING ERRCODE = 'P0001', CONSTRAINT = 'workspace_quota_exceeded', MESSAGE = 'workspace quota exceeded';
        END IF;
    ELSIF TG_OP = 'UPDATE' THEN
        IF (NEW.id, NEW.event_id, NEW.workspace_id, NEW.app_id, NEW.generation, NEW.payload_bytes)
            IS DISTINCT FROM (OLD.id, OLD.event_id, OLD.workspace_id, OLD.app_id, OLD.generation, OLD.payload_bytes) THEN
            RAISE EXCEPTION 'delivery identity and charge are immutable';
        END IF;
        IF OLD.terminal_at IS NOT NULL THEN
            IF NEW.state <> OLD.state OR NEW.terminal_at IS DISTINCT FROM OLD.terminal_at THEN
                RAISE EXCEPTION 'terminal delivery is immutable; replay requires a new generation';
            END IF;
        ELSIF NEW.state IN ('succeeded', 'dlq', 'expired', 'cancelled') THEN
            NEW.terminal_at := now();
            UPDATE workspace_quotas SET outstanding_deliveries = outstanding_deliveries - 1,
                outstanding_payload_bytes = outstanding_payload_bytes - OLD.payload_bytes
                WHERE workspace_id = OLD.workspace_id;
            UPDATE event_outbox SET discarded_at = now()
                WHERE delivery_id = OLD.id AND sent_at IS NULL AND discarded_at IS NULL;
        ELSIF NEW.terminal_at IS NOT NULL THEN
            RAISE EXCEPTION 'nonterminal delivery cannot have a terminal time';
        END IF;
    ELSIF TG_OP = 'DELETE' AND OLD.terminal_at IS NULL THEN
        UPDATE workspace_quotas SET outstanding_deliveries = outstanding_deliveries - 1,
            outstanding_payload_bytes = outstanding_payload_bytes - OLD.payload_bytes
            WHERE workspace_id = OLD.workspace_id;
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER delivery_accounting BEFORE INSERT OR UPDATE OR DELETE ON deliveries
    FOR EACH ROW EXECUTE FUNCTION account_delivery();
