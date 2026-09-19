package adminread

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestListModelsExposeOnlyBoundedOperationalFields(t *testing.T) {
	t.Parallel()
	stamp := time.Date(2026, 9, 20, 3, 0, 0, 0, time.UTC)
	models := []any{
		EventSummary{ID: "evt_1", Type: "order.created", SourceAppID: "app_1", TargetCount: 2, DeliveryCount: 3, CreatedAt: stamp},
		DeadLetterSummary{DeliveryID: "dlv_1", JobID: "job_1", EventID: "evt_1", SourceAppID: "app_1", TargetAppID: "app_2", Sink: "callback", Reason: "attempts_exhausted", Attempts: 4, CreatedAt: stamp, UpdatedAt: stamp},
		AuditSummary{ID: 1, OccurredAt: stamp, ActorType: "admin", Action: "app.create", ResourceType: "app", Outcome: "success"},
	}
	for _, model := range models {
		raw, err := json.Marshal(model)
		if err != nil {
			t.Fatal(err)
		}
		serialized := strings.ToLower(string(raw))
		for _, forbidden := range []string{"payload", "api_key", "secret", "callback_url", "authorization", "cookie"} {
			if strings.Contains(serialized, forbidden) {
				t.Fatalf("%T JSON contains forbidden field %q: %s", model, forbidden, raw)
			}
		}
	}
}
