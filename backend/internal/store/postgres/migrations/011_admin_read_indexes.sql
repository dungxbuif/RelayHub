CREATE INDEX events_admin_created_idx ON events (created_at DESC, id DESC);
CREATE INDEX events_admin_source_created_idx ON events (source_app_id, created_at DESC, id DESC);
CREATE INDEX events_admin_type_created_idx ON events (type, created_at DESC, id DESC);

CREATE INDEX deliveries_admin_dlq_updated_idx
    ON deliveries (updated_at DESC, id DESC)
    WHERE status = 'dead_letter';
CREATE INDEX deliveries_admin_dlq_target_updated_idx
    ON deliveries (target_app_id, updated_at DESC, id DESC)
    WHERE status = 'dead_letter';

CREATE INDEX audit_log_action_occurred_idx ON audit_log (action, occurred_at DESC, id DESC);
CREATE INDEX audit_log_resource_occurred_idx ON audit_log (resource_type, occurred_at DESC, id DESC);
