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

func TestDurableCountsMarshalDistinctFieldsAndLatencyPercentiles(t *testing.T) {
	p50, p95, p99 := 125.0, 480.0, 510.0
	raw, err := json.Marshal(DurableCounts{
		Pending: 2, Retrying: 3, DeadLetter: 4,
		DeliveryLatencyP50MS: &p50, DeliveryLatencyP95MS: &p95, DeliveryLatencyP99MS: &p99,
	})
	if err != nil {
		t.Fatal(err)
	}
	serialized := string(raw)
	for _, expected := range []string{`"pending":2`, `"retrying":3`, `"dead_letter":4`, `"delivery_latency_p50_ms":125`, `"delivery_latency_p95_ms":480`, `"delivery_latency_p99_ms":510`} {
		if !strings.Contains(serialized, expected) {
			t.Fatalf("durable JSON missing %s: %s", expected, serialized)
		}
	}
}

func TestReplayCommandRequiresBoundedUniqueSelection(t *testing.T) {
	valid := ReplayCommand{DeliveryIDs: []string{"dlv_1", "dlv_2"}, IdempotencyKeyHash: strings.Repeat("a", 64), RequestFingerprint: strings.Repeat("b", 64), ActorID: "session_1", Now: time.Now().UTC()}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid replay command: %v", err)
	}
	invalid := []ReplayCommand{
		{},
		{DeliveryIDs: []string{"dlv_1", "dlv_1"}, IdempotencyKeyHash: valid.IdempotencyKeyHash, RequestFingerprint: valid.RequestFingerprint, ActorID: valid.ActorID, Now: valid.Now},
		{DeliveryIDs: []string{" dlv_1"}, IdempotencyKeyHash: valid.IdempotencyKeyHash, RequestFingerprint: valid.RequestFingerprint, ActorID: valid.ActorID, Now: valid.Now},
		{DeliveryIDs: append(make([]string, MaxReplayBatch), "dlv_overflow"), IdempotencyKeyHash: valid.IdempotencyKeyHash, RequestFingerprint: valid.RequestFingerprint, ActorID: valid.ActorID, Now: valid.Now},
		{DeliveryIDs: []string{"dlv_1"}, IdempotencyKeyHash: "raw-key", RequestFingerprint: valid.RequestFingerprint, ActorID: valid.ActorID, Now: valid.Now},
	}
	for index, command := range invalid {
		if err := command.Validate(); err == nil {
			t.Fatalf("invalid replay command %d accepted: %#v", index, command)
		}
	}
}

func TestTimelineAndReplayModelsExposeOnlySafeFields(t *testing.T) {
	stamp := time.Date(2026, 9, 20, 4, 0, 0, 0, time.UTC)
	models := []any{
		DeliveryAttemptSummary{DeliveryID: "dlv_1", Generation: 2, Attempt: 1, Outcome: "retrying", Reason: "timeout", StartedAt: stamp, UpdatedAt: stamp},
		TimelineItem{ID: "attempt:dlv_1:2:1", Type: "callback.retrying", OccurredAt: stamp, EventID: "evt_1", DeliveryID: "dlv_1", JobID: "job_1", Generation: 2, Attempt: 1, Outcome: "retrying", Reason: "timeout", ActorType: "system"},
		ReplayResult{Items: []ReplayItem{{DeliveryID: "dlv_1", EventID: "evt_1", JobID: "job_1", FromGeneration: 1, Generation: 2, Status: "pending"}}},
	}
	for _, model := range models {
		raw, err := json.Marshal(model)
		if err != nil {
			t.Fatal(err)
		}
		serialized := strings.ToLower(string(raw))
		for _, forbidden := range []string{"callback_url", "api_key", "hmac", "authorization", "cookie", "payload", "secret"} {
			if strings.Contains(serialized, forbidden) {
				t.Fatalf("%T leaked %q: %s", model, forbidden, raw)
			}
		}
	}
}
