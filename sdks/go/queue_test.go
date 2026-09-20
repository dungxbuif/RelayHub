package relayhub

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestQueueV2HelpersUseCurrentRoutes(t *testing.T) {
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		calls = append(calls, request.Method+" "+request.URL.Path)
		body, _ := io.ReadAll(request.Body)
		if request.Header.Get("X-RelayHub-Signature") != sign("secret", request.Header.Get("X-RelayHub-Timestamp"), request.Method, request.URL.RequestURI(), body) {
			t.Error("queue request was not signed")
		}
		switch request.URL.Path {
		case "/api/v2/subscriptions":
			response.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(response, `{"id":"sub_1","name":"orders"}`)
		case "/api/v2/subscriptions/sub_1/pull":
			_, _ = io.WriteString(response, `{"items":[{"id":"qdl_1","subscription_id":"sub_1","receipt":"opaque","attempt":1,"generation":1,"lease_expires_at":"2026-09-20T00:01:00Z","priority":5,"event":{"id":"evt_1","type":"order.created","source_app_id":"producer","target_app_ids":["worker"],"data":{},"created_at":"2026-09-20T00:00:00Z"}}]}`)
		case "/api/v2/subscriptions/sub_1/settle":
			_, _ = io.WriteString(response, `{"items":[{"receipt":"opaque","status":"acked"}]}`)
		default:
			t.Fatalf("unexpected route %s", request.URL.Path)
		}
	}))
	defer server.Close()
	client, err := New(Config{BaseURL: server.URL, APIKey: "key", HMACSecret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := client.CreateQueueSubscription(context.Background(), QueueSubscriptionInput{Name: "orders"})
	if err != nil || subscription.ID != "sub_1" {
		t.Fatalf("subscription=%+v error=%v", subscription, err)
	}
	items, err := client.PullQueue(context.Background(), subscription.ID, 10, 0, 30*time.Second)
	if err != nil || len(items) != 1 || items[0].Receipt != "opaque" || items[0].Priority != 5 {
		t.Fatalf("items=%+v error=%v", items, err)
	}
	settled, err := client.SettleQueue(context.Background(), subscription.ID, []QueueSettlement{{Receipt: items[0].Receipt, Disposition: "ack"}})
	if err != nil || settled[0].Status != "acked" {
		t.Fatalf("settled=%+v error=%v", settled, err)
	}
	if len(calls) != 3 {
		t.Fatalf("calls=%v", calls)
	}
}

func TestQueueWorkerCancelsHandlerOnLostLeaseWithoutSettlement(t *testing.T) {
	var settlements atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/subscriptions/sub_1/leases/extend" {
			_, _ = io.WriteString(w, `{"items":[{"receipt":"opaque","status":"invalid_receipt"}]}`)
			return
		}
		settlements.Add(1)
		_, _ = io.WriteString(w, `{"items":[{"receipt":"opaque","status":"acked"}]}`)
	}))
	defer server.Close()
	client, _ := New(Config{BaseURL: server.URL, APIKey: "key", HMACSecret: "secret"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var observed atomic.Bool
	processQueueDelivery(ctx, client, "sub_1", QueueDelivery{Receipt: "opaque"}, func(ctx context.Context, _ QueueDelivery) QueueResult {
		<-ctx.Done()
		if ctx.Err() == context.Canceled {
			observed.Store(true)
		}
		return QueueACK()
	}, QueueWorkerOptions{Heartbeat: 10 * time.Millisecond, RetryDelay: time.Second})
	if !observed.Load() {
		t.Fatal("lost lease did not cancel handler")
	}
	if settlements.Load() != 0 {
		t.Fatal("settled after losing lease")
	}
}

func TestQueueWorkerRetriesAndDrains(t *testing.T) {
	var pulls atomic.Int64
	settled := make(chan QueueSettlement, 1)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/api/v2/subscriptions/sub_1/pull":
			if pulls.Add(1) == 1 {
				_, _ = io.WriteString(response, `{"items":[{"id":"qdl_1","subscription_id":"sub_1","receipt":"opaque","attempt":1,"generation":1,"lease_expires_at":"2026-09-20T00:01:00Z","priority":0,"event":{"id":"evt_1","type":"x","source_app_id":"p","target_app_ids":["w"],"data":{},"created_at":"2026-09-20T00:00:00Z"}}]}`)
			} else {
				time.Sleep(time.Millisecond)
				_, _ = io.WriteString(response, `{"items":[]}`)
			}
		case "/api/v2/subscriptions/sub_1/settle":
			var input struct {
				Items []QueueSettlement `json:"items"`
			}
			_ = json.NewDecoder(request.Body).Decode(&input)
			settled <- input.Items[0]
			_, _ = io.WriteString(response, `{"items":[{"receipt":"opaque","status":"available"}]}`)
		default:
			t.Fatalf("unexpected route %s", request.URL.Path)
		}
	}))
	defer server.Close()
	client, _ := New(Config{BaseURL: server.URL, APIKey: "key", HMACSecret: "secret"})
	worker, err := client.WorkQueue(context.Background(), "sub_1", func(context.Context, QueueDelivery) QueueResult { return QueueRetry(2500*time.Millisecond, "busy") }, QueueWorkerOptions{Concurrency: 1, BatchSize: 1, Wait: time.Millisecond, Visibility: time.Minute, Heartbeat: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case item := <-settled:
		if item.Disposition != "retry" || item.DelaySeconds != 3 || item.Reason != "busy" {
			t.Fatalf("settlement=%+v", item)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not settle")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := worker.Drain(ctx); err != nil {
		t.Fatal(err)
	}
}
