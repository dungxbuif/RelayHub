package relayhub

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/auth"
)

func TestSharedHMACFixtures(t *testing.T) {
	raw, err := os.ReadFile("../../public-docs/schemas/hmac-signing-fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases struct {
		Fixtures []struct {
			Name      string `json:"name"`
			Secret    string `json:"secret_utf8"`
			Timestamp string `json:"timestamp"`
			Method    string `json:"method"`
			Target    string `json:"request_target"`
			Body      string `json:"body_utf8"`
			Signature string `json:"signature_hex"`
		} `json:"fixtures"`
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases.Fixtures) < 2 {
		t.Fatal("missing shared signing cases")
	}
	for _, fixture := range cases.Fixtures {
		t.Run(fixture.Name, func(t *testing.T) {
			if got := sign(fixture.Secret, fixture.Timestamp, fixture.Method, fixture.Target, []byte(fixture.Body)); got != fixture.Signature {
				t.Fatalf("signature=%s want=%s", got, fixture.Signature)
			}
		})
	}
}

func TestPublishSignsTransmittedBytesAndPreservesReplay(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.Method != "POST" || r.URL.RequestURI() != "/api/v1/events" || r.Header.Get("Idempotency-Key") != "order-1" {
			t.Errorf("wrong request contract: %s %s", r.Method, r.URL.RequestURI())
		}
		if r.Header.Get("X-RelayHub-Api-Key") != "rhk_test" || r.Header.Get("X-RelayHub-Signature") != auth.Sign([]byte("rhs_test"), r.Header.Get("X-RelayHub-Timestamp"), r.Method, r.URL.RequestURI(), body) {
			t.Error("signature did not match actual body/target")
		}
		if !strings.Contains(string(body), "9007199254740993") || !strings.Contains(string(body), "Tiếng Việt") {
			t.Error("payload changed")
		}
		w.Header().Set("Idempotent-Replayed", "true")
		w.WriteHeader(202)
		_, _ = io.WriteString(w, `{"event":{"id":"evt_1","data":{"n":9007199254740993}},"jobs":[{"id":"job_1"}]}`)
	}))
	defer server.Close()
	client, err := New(Config{BaseURL: server.URL, APIKey: "rhk_test", HMACSecret: "rhs_test"})
	if err != nil {
		t.Fatal(err)
	}
	publication, err := client.Publish(context.Background(), EventInput{Type: "created", TargetAppIDs: []string{"target"}, Data: json.RawMessage(`{"text":"Tiếng Việt","n":9007199254740993}`)}, IdempotencyKey("order-1"))
	if err != nil || publication.Event.ID != "evt_1" || !publication.Replayed || string(publication.Event.Data) != `{"n":9007199254740993}` {
		t.Fatalf("publication=%#v error=%v", publication, err)
	}
}

func TestPublishAllowsRoutedEventsWithoutExplicitTargets(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), "target_app_ids") {
			t.Fatalf("routed publish should omit target_app_ids: %s", body)
		}
		if r.URL.RequestURI() != "/api/v1/events" || r.Header.Get("Idempotency-Key") != "routed-1" {
			t.Fatalf("wrong routed publish request: %s %s", r.Method, r.URL.RequestURI())
		}
		w.WriteHeader(202)
		_, _ = io.WriteString(w, `{"event":{"id":"evt_routed","target_app_ids":["app_target"],"data":{}},"jobs":[]}`)
	}))
	defer server.Close()
	client, err := New(Config{BaseURL: server.URL, APIKey: "key", HMACSecret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := client.Publish(context.Background(), EventInput{Type: "order.created", Data: json.RawMessage(`{}`)}, IdempotencyKey("routed-1"))
	if err != nil || got.Event.ID != "evt_routed" {
		t.Fatalf("routed publish=%#v error=%v", got, err)
	}
}

func TestRealtimePublishAndAdminHelpersUseCurrentRoutes(t *testing.T) {
	var sawRealtime, sawAdmin bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		switch r.URL.RequestURI() {
		case "/api/v1/realtime/channels/orders.live/publish":
			sawRealtime = true
			if r.Header.Get("X-RelayHub-Api-Key") != "key" || r.Header.Get("X-RelayHub-Signature") != auth.Sign([]byte("secret"), r.Header.Get("X-RelayHub-Timestamp"), r.Method, r.URL.RequestURI(), body) {
				t.Fatal("realtime publish was not signed")
			}
			w.WriteHeader(202)
		case "/api/v1/apps":
			sawAdmin = true
			if r.Header.Get("Authorization") != "Bearer admin-token" || strings.Contains(r.Header.Get("X-RelayHub-Api-Key"), "key") {
				t.Fatal("admin request used wrong authentication")
			}
			w.WriteHeader(201)
			_, _ = io.WriteString(w, `{"app_id":"app_1","api_key":"rhk_1","hmac_secret":"rhs_1"}`)
		case "/api/v1/routing/rules":
			if r.Header.Get("Authorization") != "Bearer admin-token" {
				t.Fatal("routing request missing admin bearer")
			}
			w.WriteHeader(201)
			_, _ = io.WriteString(w, `{"id":"route_1","event_type":"order.created","target_app_id":"app_1","enabled":true}`)
		default:
			t.Fatalf("unexpected route %s", r.URL.RequestURI())
		}
	}))
	defer server.Close()
	client, err := New(Config{BaseURL: server.URL, APIKey: "key", HMACSecret: "secret", AdminToken: "admin-token"})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.PublishRealtime(context.Background(), "orders.live", json.RawMessage(`{"id":"ord_1"}`)); err != nil {
		t.Fatal(err)
	}
	creds, err := client.CreateApp(context.Background(), CreateAppInput{Name: "orders", DeliveryMode: "websocket"})
	if err != nil || creds.AppID != "app_1" {
		t.Fatalf("create app=%#v error=%v", creds, err)
	}
	rule, err := client.CreateRoutingRule(context.Background(), RoutingRuleInput{EventType: "order.created", TargetAppID: "app_1"})
	if err != nil || rule.ID != "route_1" {
		t.Fatalf("routing rule=%#v error=%v", rule, err)
	}
	if !sawRealtime || !sawAdmin {
		t.Fatal("missing realtime or admin request")
	}
}

func TestHTTPValidationRedirectPrivacyAndCancellation(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Redirect(w, r, "https://example.invalid/?secret=never-follow", 302)
	}))
	defer server.Close()
	client, _ := New(Config{BaseURL: server.URL, APIKey: "key", HMACSecret: "secret"})
	for _, input := range []EventInput{{Type: "x", TargetAppIDs: []string{"target"}, Data: json.RawMessage(`[]`)}, {Type: "x", TargetAppIDs: []string{"target"}, Data: append([]byte(`{"x":"`), 0xff, '"', '}')}} {
		if _, err := client.Publish(context.Background(), input, IdempotencyKey("key")); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("invalid payload=%v", err)
		}
	}
	if calls != 0 {
		t.Fatal("invalid payload sent")
	}
	_, err := client.Publish(context.Background(), EventInput{Type: "x", TargetAppIDs: []string{"target"}, Data: json.RawMessage(`{}`)}, IdempotencyKey("key"))
	var api *APIError
	if !errors.As(err, &api) || api.StatusCode != 302 || strings.Contains(err.Error(), "never-follow") {
		t.Fatalf("redirect error=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Publish(ctx, EventInput{}, IdempotencyKey("key")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
}

func TestInvokePreservesScalarResultAndTypedHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "failed") {
			w.Header().Set("Idempotent-Replayed", "true")
			w.WriteHeader(504)
			_, _ = io.WriteString(w, `{"error":{"code":"function_timeout","message":"private details"}}`)
			return
		}
		_, _ = io.WriteString(w, `{"invocation_id":"inv_1","ok":true,"result":9007199254740993}`)
	}))
	defer server.Close()
	client, _ := New(Config{BaseURL: server.URL, APIKey: "key", HMACSecret: "secret", HTTPClient: &http.Client{Timeout: time.Second}})
	result, err := client.Invoke(context.Background(), "fn_1", json.RawMessage(`{}`), IdempotencyKey("one"))
	if err != nil || string(result.Result) != "9007199254740993" {
		t.Fatalf("invoke=%#v %v", result, err)
	}
	_, err = client.Invoke(context.Background(), "failed", json.RawMessage(`{}`), IdempotencyKey("two"))
	var api *APIError
	if !errors.As(err, &api) || api.Code != "function_timeout" || !api.Replayed || strings.Contains(err.Error(), "private") {
		t.Fatalf("typed error=%v", err)
	}
}
