CREATE TRIGGER audit_log_reject_truncate
BEFORE TRUNCATE ON audit_log
FOR EACH STATEMENT EXECUTE FUNCTION relayhub_reject_audit_mutation();

