-- Additive to Task 4/version 3. Unconfigured endpoints remain closed to public ingress.
ALTER TABLE event_endpoints
    ADD COLUMN policy jsonb NOT NULL DEFAULT '{"mode":"async"}',
    ADD COLUMN public_ingress boolean NOT NULL DEFAULT false,
    ADD COLUMN ingress_secret_hash text,
    ADD CONSTRAINT endpoint_auth_mode CHECK (NOT public_ingress OR ingress_secret_hash IS NULL),
    ADD CONSTRAINT endpoint_policy_valid CHECK (COALESCE(
        policy->>'mode' = 'async' OR
        (policy->>'mode' = 'sync' AND (policy->>'timeout_seconds')::integer BETWEEN 1 AND 30
         AND policy->>'offline' IN ('queue','reject')
         AND policy->>'timeout' IN ('continue','expire_pending')), false));

CREATE TABLE webhook_responses (
    delivery_id uuid PRIMARY KEY REFERENCES deliveries(id),
    policy jsonb NOT NULL,
    state text NOT NULL DEFAULT 'open' CHECK (state IN ('open','answered','timed_out')),
    deadline_at timestamptz,
    result bytea CHECK (octet_length(result) <= 2097152),
    response_at timestamptz
);
CREATE TABLE webhook_response_audit (
    id uuid PRIMARY KEY,
    delivery_id uuid NOT NULL REFERENCES deliveries(id),
    attempt_token uuid NOT NULL,
    agent_id uuid NOT NULL,
    outcome text NOT NULL CHECK (outcome IN ('staged','answered','late','duplicate','rejected')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX webhook_response_audit_delivery ON webhook_response_audit(delivery_id,created_at);
-- A candidate is immutable and fenced to one actual Task 4 attempt. It is not a delivery.
CREATE TABLE webhook_response_candidates (
    attempt_token uuid PRIMARY KEY REFERENCES delivery_attempts(attempt_token),
    audit_id uuid NOT NULL REFERENCES webhook_response_audit(id),
    result bytea NOT NULL CHECK (octet_length(result) <= 2097152)
);

CREATE FUNCTION initialize_webhook_response() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO webhook_responses(delivery_id,policy)
        SELECT NEW.id,p.policy FROM events e JOIN event_endpoints p ON p.id=e.endpoint_id
        WHERE e.id=NEW.event_id;
    RETURN NEW;
END $$;
CREATE TRIGGER webhook_response_acceptance AFTER INSERT ON deliveries
    FOR EACH ROW EXECUTE FUNCTION initialize_webhook_response();
-- Upgrade already-accepted work without changing its state, quota or outbox.
INSERT INTO webhook_responses(delivery_id,policy)
    SELECT d.id,p.policy FROM deliveries d JOIN events e ON e.id=d.event_id
    JOIN event_endpoints p ON p.id=e.endpoint_id;

CREATE FUNCTION finalize_webhook_response() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    candidate webhook_response_candidates%ROWTYPE;
    disposition text;
BEGIN
    IF OLD.state='in_flight' AND NEW.state='succeeded' THEN
        SELECT * INTO candidate FROM webhook_response_candidates WHERE attempt_token=OLD.attempt_token;
        IF FOUND THEN
            UPDATE webhook_responses SET result=candidate.result, response_at=clock_timestamp(),
                state=CASE WHEN state='open' AND (deadline_at IS NULL OR deadline_at>clock_timestamp())
                           THEN 'answered' ELSE 'timed_out' END
                WHERE delivery_id=NEW.id AND result IS NULL RETURNING state INTO disposition;
            UPDATE webhook_response_audit SET outcome=CASE WHEN disposition='answered' THEN 'answered' ELSE 'late' END
                WHERE id=candidate.audit_id;
        END IF;
    END IF;
    RETURN NEW;
END $$;
-- Result, winner audit, attempt success and quota release commit together via settle_delivery.
CREATE TRIGGER webhook_response_settlement AFTER UPDATE OF state ON deliveries
    FOR EACH ROW EXECUTE FUNCTION finalize_webhook_response();
