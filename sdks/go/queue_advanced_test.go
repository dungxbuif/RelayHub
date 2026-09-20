package relayhub

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestQueueAdvancedHelpersPreserveEscapedPathAndQuerySigning(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		if request.Header.Get("X-RelayHub-Signature") != sign("secret", request.Header.Get("X-RelayHub-Timestamp"), request.Method, request.URL.RequestURI(), body) {
			t.Fatal("signature does not cover exact request target")
		}
		switch request.Method + " " + request.URL.RequestURI() {
		case "POST /api/v2/subscriptions/sub_1/schedules":
			response.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(response, `{"id":"qsch_1","policy_version":1}`)
		case "POST /api/v2/subscriptions/sub_1/drain":
			response.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(response, `{"subscription_id":"sub_1","status":"draining","in_flight":1}`)
		case "GET /api/v2/subscriptions/sub_1/dead-letters/export?format=json&limit=25&cursor=qdl_0":
			_, _ = io.WriteString(response, `{"items":[{"delivery_id":"qdl_1"}]}`)
		default:
			t.Fatalf("unexpected request %s %s", request.Method, request.URL.RequestURI())
		}
	}))
	defer server.Close()
	client, err := New(Config{BaseURL: server.URL, APIKey: "key", HMACSecret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	input := QueueScheduleInput{Name: "daily", CronExpression: "0 9 * * *", Timezone: "Asia/Ho_Chi_Minh", EventType: "report.daily", Data: []byte(`{"kind":"daily"}`)}
	if schedule, err := client.CreateQueueSchedule(context.Background(), "sub_1", input); err != nil || schedule.ID != "qsch_1" {
		t.Fatalf("schedule=%#v error=%v", schedule, err)
	}
	if drain, err := client.DrainQueueSubscription(context.Background(), "sub_1", 45*time.Second); err != nil || drain.Status != "draining" {
		t.Fatalf("drain=%#v error=%v", drain, err)
	}
	items, next, err := client.ExportQueueDeadLetters(context.Background(), "sub_1", "qdl_0", 25)
	if err != nil || len(items) != 1 || next != "" {
		t.Fatalf("items=%#v next=%q error=%v", items, next, err)
	}
}
