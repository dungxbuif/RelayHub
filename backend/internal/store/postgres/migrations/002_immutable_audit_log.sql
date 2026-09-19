CREATE FUNCTION relayhub_reject_audit_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'audit_log is append-only' USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER audit_log_reject_update_delete
BEFORE UPDATE OR DELETE ON audit_log
FOR EACH ROW EXECUTE FUNCTION relayhub_reject_audit_mutation();

