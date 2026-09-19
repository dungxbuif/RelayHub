package observability

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestDashboardRecorderReceivesOnlyBoundedMetricFields(t *testing.T) {
	var mu sync.Mutex
	var fields []string
	restore := SetDashboardRecorder(func(field string) {
		mu.Lock()
		fields = append(fields, field)
		mu.Unlock()
	})
	t.Cleanup(restore)

	HTTPRequest("POST", "/api/v1/events", 202, time.Millisecond)
	HTTPRequest("GET", "/ws", 101, time.Second)
	EventOutcome("published")
	EventOutcome("unbounded-value")
	CallbackOutcome("dead_letter")
	NATSEvent("reconnected")
	NATSEvent("slow_consumer")

	mu.Lock()
	defer mu.Unlock()
	want := []string{"request_total", "status_2xx", "request_total", "event_published", "delivery_dead_letter", "nats_reconnected"}
	if !reflect.DeepEqual(fields, want) {
		t.Fatalf("dashboard fields = %#v, want %#v", fields, want)
	}
}
