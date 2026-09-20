//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/adminread"
)

func TestAdminReadModelsPaginationFiltersAndCounts(t *testing.T) {
	client := integrationPostgresClient(t)
	ctx := context.Background()
	base := time.Date(2026, 9, 20, 3, 0, 0, 0, time.UTC)
	for _, app := range []string{"app_source", "app_target", "app_other"} {
		if _, err := client.pool.Exec(ctx, `INSERT INTO applications(id,name,delivery_mode,enabled,created_at,updated_at) VALUES($1,$1,'websocket',true,$2,$2)`, app, base); err != nil {
			t.Fatal(err)
		}
	}
	for index, id := range []string{"evt_3", "evt_2", "evt_1"} {
		eventType := "invoice.created"
		if index == 2 {
			eventType = "invoice.cancelled"
		}
		if _, err := client.pool.Exec(ctx, `INSERT INTO events(id,type,source_app_id,target_app_ids,data,created_at,expires_at) VALUES($1,$2,'app_source',ARRAY['app_target'],$3::json,$4,$5)`, id, eventType, `{"private":"SENTINEL_EVENT_PAYLOAD"}`, base, base.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := client.pool.Exec(ctx, `
		INSERT INTO deliveries(id,public_job_id,event_id,source_app_id,target_app_id,sink,status,attempts,created_at,updated_at) VALUES
		('dlv_pending','job_pending','evt_1','app_source','app_target','stream','pending',0,$1,$1),
		('dlv_retry','job_retry','evt_2','app_source','app_target','callback','retrying',2,$1,$1),
		('dlv_acked','job_acked','evt_1','app_source','app_other','stream','acked',1,$1,$3),
		('dlv_delivered','job_delivered','evt_3','app_source','app_target','callback','delivered',1,$1,$4),
		('dlv_dead_b','job_dead_b','evt_2','app_source','app_other','callback','dead_letter',4,$1,$2),
		('dlv_dead_a','job_dead_a','evt_3','app_source','app_other','stream','dead_letter',1,$1,$2)`, base, base.Add(time.Minute), base.Add(100*time.Millisecond), base.Add(500*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.pool.Exec(ctx, `UPDATE deliveries SET callback_reason='attempts_exhausted' WHERE id='dlv_dead_b'`); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"routing.create", "app.rotate", "app.create"} {
		if _, err := client.pool.Exec(ctx, `INSERT INTO audit_log(occurred_at,actor_type,actor_id,action,resource_type,resource_id,outcome,metadata) VALUES($1,'admin','operator',$2,'app','app_target','success','{"changed_fields":["name"]}')`, base, action); err != nil {
			t.Fatal(err)
		}
	}

	eventFilters := adminread.EventFilters{SourceAppID: "app_source"}
	first, err := client.ListAdminEvents(ctx, adminread.EventListQuery{Options: adminread.ListOptions{Limit: 2}, Filters: eventFilters})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Items) != 2 || first.Items[0].ID != "evt_3" || first.Items[1].ID != "evt_2" || first.NextCursor == "" {
		t.Fatalf("first event page = %#v", first)
	}
	cursor, err := adminread.DecodeCursor(first.NextCursor, eventFilters.Fingerprint())
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.ListAdminEvents(ctx, adminread.EventListQuery{Options: adminread.ListOptions{Limit: 2, Cursor: &cursor}, Filters: eventFilters})
	if err != nil || len(second.Items) != 1 || second.Items[0].ID != "evt_1" || second.NextCursor != "" {
		t.Fatalf("second event page = %#v, %v", second, err)
	}
	raw, _ := json.Marshal(first)
	if string(raw) == "" || containsSentinel(raw) {
		t.Fatalf("event list leaked payload: %s", raw)
	}

	dlq, err := client.ListAdminDeadLetters(ctx, adminread.DeadLetterListQuery{Options: adminread.ListOptions{Limit: 10}, Filters: adminread.DeadLetterFilters{Sink: "callback", Reason: "attempts_exhausted"}})
	if err != nil || len(dlq.Items) != 1 || dlq.Items[0].DeliveryID != "dlv_dead_b" {
		t.Fatalf("DLQ page = %#v, %v", dlq, err)
	}
	auditFilters := adminread.AuditFilters{ActorType: "admin"}
	audit, err := client.ListAdminAudit(ctx, adminread.AuditListQuery{Options: adminread.ListOptions{Limit: 2}, Filters: auditFilters})
	if err != nil || len(audit.Items) != 2 || audit.Items[0].ID <= audit.Items[1].ID || audit.NextCursor == "" {
		t.Fatalf("audit page = %#v, %v", audit, err)
	}
	auditCursor, err := adminread.DecodeCursor(audit.NextCursor, auditFilters.Fingerprint())
	if err != nil {
		t.Fatal(err)
	}
	auditTail, err := client.ListAdminAudit(ctx, adminread.AuditListQuery{Options: adminread.ListOptions{Limit: 2, Cursor: &auditCursor}, Filters: auditFilters})
	if err != nil || len(auditTail.Items) != 1 || auditTail.NextCursor != "" {
		t.Fatalf("audit tail = %#v, %v", auditTail, err)
	}
	counts, err := client.AdminDurableCounts(ctx)
	if err != nil || counts.Pending != 1 || counts.Retrying != 1 || counts.DeadLetter != 2 || counts.OldestPendingAt == nil || counts.DeliveryLatencyP50MS == nil || *counts.DeliveryLatencyP50MS < 299 || *counts.DeliveryLatencyP50MS > 301 || counts.DeliveryLatencyP95MS == nil || *counts.DeliveryLatencyP95MS < 479 || *counts.DeliveryLatencyP95MS > 481 {
		t.Fatalf("counts = %#v, %v", counts, err)
	}
}

func containsSentinel(raw []byte) bool {
	for index := 0; index+len("SENTINEL_EVENT_PAYLOAD") <= len(raw); index++ {
		if string(raw[index:index+len("SENTINEL_EVENT_PAYLOAD")]) == "SENTINEL_EVENT_PAYLOAD" {
			return true
		}
	}
	return false
}
