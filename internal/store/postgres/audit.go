package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

type AuditEntry struct {
	OccurredAt   time.Time
	ActorType    string
	ActorID      string
	Action       string
	ResourceType string
	ResourceID   string
	Outcome      string
	Metadata     json.RawMessage
}

func (client *Client) AppendAudit(ctx context.Context, entry AuditEntry) error {
	if entry.OccurredAt.IsZero() || entry.ActorType == "" || entry.Action == "" || entry.ResourceType == "" || entry.Outcome == "" {
		return errors.New("audit entry is incomplete")
	}
	if len(entry.Metadata) == 0 {
		entry.Metadata = json.RawMessage(`{}`)
	}
	if err := validateAuditMetadata(entry.Metadata); err != nil {
		return err
	}
	_, err := client.pool.Exec(ctx, `INSERT INTO audit_log(occurred_at,actor_type,actor_id,action,resource_type,resource_id,outcome,metadata) VALUES($1,$2,NULLIF($3,''),$4,$5,NULLIF($6,''),$7,$8)`, entry.OccurredAt, entry.ActorType, entry.ActorID, entry.Action, entry.ResourceType, entry.ResourceID, entry.Outcome, entry.Metadata)
	return err
}

func validateAuditMetadata(raw json.RawMessage) error {
	var object map[string]any
	if len(raw) > 16*1024 || json.Unmarshal(raw, &object) != nil || object == nil {
		return errors.New("audit metadata must be a JSON object up to 16 KiB")
	}
	allowed := map[string]struct{}{
		"ip": {}, "request_id": {}, "reason": {}, "changed_fields": {},
		"previous_state": {}, "new_state": {}, "credential_version": {},
	}
	for key, value := range object {
		if _, ok := allowed[key]; !ok || !safeAuditValue(value) {
			return errors.New("audit metadata contains an unsupported field or value")
		}
	}
	return nil
}

func safeAuditValue(value any) bool {
	switch typed := value.(type) {
	case string:
		return len(typed) <= 1024
	case bool, nil:
		return true
	case float64:
		return typed >= 0 && typed <= 1<<53
	case []any:
		if len(typed) > 64 {
			return false
		}
		for _, item := range typed {
			text, ok := item.(string)
			if !ok || len(text) > 128 {
				return false
			}
		}
		return true
	default:
		return false
	}
}
