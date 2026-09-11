package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
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
	forbidden := map[string]struct{}{"api_key": {}, "hmac_secret": {}, "secret": {}, "password": {}, "access_token": {}, "authorization": {}, "cookie": {}, "body": {}, "request_body": {}}
	var inspect func(any) bool
	inspect = func(value any) bool {
		switch typed := value.(type) {
		case map[string]any:
			for key, nested := range typed {
				if _, blocked := forbidden[strings.ToLower(key)]; blocked || inspect(nested) {
					return true
				}
			}
		case []any:
			for _, nested := range typed {
				if inspect(nested) {
					return true
				}
			}
		}
		return false
	}
	if inspect(object) {
		return errors.New("audit metadata contains a forbidden sensitive field")
	}
	return nil
}
