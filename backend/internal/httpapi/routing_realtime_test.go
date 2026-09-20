package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/dungxbuif/RelayHub/internal/auth"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/realtime"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/dungxbuif/RelayHub/internal/store"
)

func TestManagementConsoleUseCaseRoutesEventAndRealtimeOverWebSocket(t *testing.T) {
	apps := newHTTPMemoryStore()
	appService := service.NewAppService(apps, service.AppOptions{Now: func() time.Time { return time.Unix(1789120800, 0) }})
	rules := newHTTPRoutingMemory()
	routing := service.NewRoutingService(rules, apps, service.RoutingOptions{Now: func() time.Time { return time.Unix(1789120800, 0) }, NewID: func(string) (string, error) { return "rr_console", nil }})
	events := &httpEventMemory{events: map[string]domain.Event{}, jobs: map[string]domain.Job{}, idem: map[string]store.Publication{}}
	hub := realtime.NewHub()
	defer hub.Close()
	issuer := auth.NewTokenIssuer([]byte("console-smoke-token-secret"), time.Now)
	router := NewRouter(Dependencies{
		Apps:          appService,
		Events:        service.NewEventService(events, apps, service.EventOptions{Router: routing, Realtime: hub, Notifier: hub, Now: func() time.Time { return time.Unix(1789120800, 0) }, NewID: sequentialIDs()}),
		Routing:       routing,
		Realtime:      hub,
		TokenIssuer:   issuer,
		AdminSessions: testAdminSessions(t),
		Now:           func() time.Time { return time.Unix(1789120800, 0) },
		Admin:         fstest.MapFS{},
		Metrics:       http.NotFoundHandler(),
	})
	server := httptest.NewServer(router)
	defer server.Close()

	producer := createAppViaHTTP(t, router, "console-producer")
	target := createAppViaHTTP(t, router, "console-target")
	tokenResponse := signedEventRequest(t, router, target, http.MethodPost, "/api/v1/socket/token", []byte(`{"scopes":["ws:connect","ws:subscribe","ws:read"],"ttl_seconds":300}`), "")
	if tokenResponse.Code != http.StatusCreated {
		t.Fatalf("socket token: %d %s", tokenResponse.Code, tokenResponse.Body.String())
	}
	var token struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(tokenResponse.Body.Bytes(), &token); err != nil || token.Token == "" {
		t.Fatalf("socket token response=%s err=%v", tokenResponse.Body.String(), err)
	}
	conn := wsDial(t, server, token.Token, "")
	if ready := wsRead(t, conn); ready.Type != "ready" || ready.AppID != target.AppID {
		t.Fatalf("ready frame=%#v", ready)
	}
	if err := conn.WriteJSON(map[string]any{"type": "subscribe", "topics": []string{"events", "channel:orders.live"}}); err != nil {
		t.Fatal(err)
	}
	if subscribed := wsRead(t, conn); subscribed.Type != "subscribed" {
		t.Fatalf("subscribed frame=%#v", subscribed)
	}

	ruleBody := []byte(`{"source_app_id":"` + producer.AppID + `","event_type":"order.created","target_app_id":"` + target.AppID + `","realtime_channel":"orders.live"}`)
	createdRule := requestJSON(t, router, http.MethodPost, "/api/v1/routing/rules", ruleBody, adminSessionHeaders(t, router))
	if createdRule.Code != http.StatusCreated {
		t.Fatalf("create rule: %d %s", createdRule.Code, createdRule.Body.String())
	}
	published := signedEventRequest(t, router, producer, http.MethodPost, "/api/v1/events", []byte(`{"type":"order.created","data":{"order_id":"ord_1"}}`), "console-event-1")
	if published.Code != http.StatusAccepted {
		t.Fatalf("publish routed event: %d %s", published.Code, published.Body.String())
	}

	seenEvent, seenChannel := false, false
	deadline := time.After(2 * time.Second)
	for !(seenEvent && seenChannel) {
		select {
		case <-deadline:
			t.Fatalf("missing frames: event=%t channel=%t", seenEvent, seenChannel)
		default:
		}
		frame := wsRead(t, conn)
		switch frame.Type {
		case "event":
			if frame.Event == nil || frame.Event.SourceAppID != producer.AppID || len(frame.Event.TargetAppIDs) != 1 || frame.Event.TargetAppIDs[0] != target.AppID || string(frame.Event.Data) != `{"order_id":"ord_1"}` {
				t.Fatalf("event frame=%#v", frame)
			}
			seenEvent = true
		case "channel.message":
			if frame.Channel != "orders.live" || frame.PublisherAppID != producer.AppID || string(frame.Data) != `{"order_id":"ord_1"}` {
				t.Fatalf("channel frame=%#v", frame)
			}
			seenChannel = true
		case "job.updated":
			// Job hints may arrive between the event and channel frames.
		default:
			t.Fatalf("unexpected frame=%#v", frame)
		}
	}
}

func createAppViaHTTP(t *testing.T, router http.Handler, name string) service.AppCredentials {
	t.Helper()
	response := requestJSON(t, router, http.MethodPost, "/api/v1/apps", []byte(`{"name":"`+name+`","delivery_mode":"websocket"}`), adminSessionHeaders(t, router))
	if response.Code != http.StatusCreated {
		t.Fatalf("create app %s: %d %s", name, response.Code, response.Body.String())
	}
	var credentials service.AppCredentials
	if err := json.Unmarshal(response.Body.Bytes(), &credentials); err != nil || credentials.AppID == "" || credentials.APIKey == "" || credentials.HMACSecret == "" {
		t.Fatalf("credentials=%#v body=%s err=%v", credentials, response.Body.String(), err)
	}
	return credentials
}

func sequentialIDs() func(string) (string, error) {
	counters := map[string]int{}
	return func(prefix string) (string, error) {
		counters[prefix]++
		return prefix + strings.TrimSuffix(prefix, "_") + "_" + strconv.Itoa(counters[prefix]), nil
	}
}

func TestRoutingRuleHTTPRoutes(t *testing.T) {
	apps := newHTTPMemoryStore()
	appService := service.NewAppService(apps, service.AppOptions{Now: func() time.Time { return time.Unix(1789120800, 0) }})
	_, source, err := appService.Create(context.Background(), service.CreateApp{Name: "producer", DeliveryMode: domain.DeliveryQueue})
	if err != nil {
		t.Fatal(err)
	}
	_, target, err := appService.Create(context.Background(), service.CreateApp{Name: "consumer", DeliveryMode: domain.DeliveryQueue})
	if err != nil {
		t.Fatal(err)
	}
	routes := newHTTPRoutingMemory()
	routing := service.NewRoutingService(routes, apps, service.RoutingOptions{Now: func() time.Time { return time.Unix(1789120800, 0) }, NewID: func(string) (string, error) { return "route_1", nil }})
	router := NewRouter(Dependencies{Apps: appService, Routing: routing, AdminSessions: testAdminSessions(t), Now: func() time.Time { return time.Unix(1789120800, 0) }, Admin: fstest.MapFS{}, Metrics: http.NotFoundHandler()})

	body := []byte(`{"source_app_id":"` + source.AppID + `","event_type":"order.created","target_app_id":"` + target.AppID + `","realtime_channel":"orders.live"}`)
	created := requestJSON(t, router, http.MethodPost, "/api/v1/routing/rules", body, adminSessionHeaders(t, router))
	if created.Code != http.StatusCreated {
		t.Fatalf("create routing rule: %d %s", created.Code, created.Body.String())
	}
	var rule domain.RoutingRule
	if err := json.Unmarshal(created.Body.Bytes(), &rule); err != nil || rule.ID != "route_1" || !rule.Enabled || rule.RealtimeChannel == nil || *rule.RealtimeChannel != "orders.live" {
		t.Fatalf("created rule=%#v err=%v", rule, err)
	}
	listed := requestJSON(t, router, http.MethodGet, "/api/v1/routing/rules", nil, adminSessionHeaders(t, router))
	if listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), "order.created") {
		t.Fatalf("list routing rules: %d %s", listed.Code, listed.Body.String())
	}
	patched := requestJSON(t, router, http.MethodPatch, "/api/v1/routing/rules/route_1", []byte(`{"enabled":false,"realtime_channel":null}`), adminSessionHeaders(t, router))
	if patched.Code != http.StatusOK || strings.Contains(patched.Body.String(), "orders.live") {
		t.Fatalf("patch routing rule: %d %s", patched.Code, patched.Body.String())
	}
	deleted := requestJSON(t, router, http.MethodDelete, "/api/v1/routing/rules/route_1", nil, adminSessionHeaders(t, router))
	if deleted.Code != http.StatusNoContent {
		t.Fatalf("delete routing rule: %d %s", deleted.Code, deleted.Body.String())
	}
	unauthorized := requestJSON(t, router, http.MethodGet, "/api/v1/routing/rules", nil, nil)
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unsigned admin route: %d", unauthorized.Code)
	}
}

func TestRealtimePublishHTTPRouteFansOutToChannelSubscribers(t *testing.T) {
	apps := newHTTPMemoryStore()
	appService := service.NewAppService(apps, service.AppOptions{})
	_, publisher, err := appService.Create(context.Background(), service.CreateApp{Name: "publisher", DeliveryMode: domain.DeliveryQueue})
	if err != nil {
		t.Fatal(err)
	}
	hub := realtime.NewHub()
	defer hub.Close()
	session := hub.Register("browser")
	if err := hub.Subscribe(session, []string{"channel:orders.live"}); err != nil {
		t.Fatal(err)
	}
	router := NewRouter(Dependencies{Apps: appService, Realtime: hub, AdminSessions: testAdminSessions(t), Now: func() time.Time { return time.Unix(1789120800, 0) }, Admin: fstest.MapFS{}, Metrics: http.NotFoundHandler()})
	response := signedEventRequest(t, router, publisher, http.MethodPost, "/api/v1/realtime/channels/orders.live/publish", []byte(`{"data":{"id":"ord_1"}}`), "unused")
	if response.Code != http.StatusAccepted {
		t.Fatalf("publish realtime: %d %s", response.Code, response.Body.String())
	}
	frame := receiveHTTPRealtime(t, session)
	if frame.Type != "channel.message" || frame.Channel != "orders.live" || frame.PublisherAppID != publisher.AppID || string(frame.Data) != `{"id":"ord_1"}` {
		t.Fatalf("channel message=%#v", frame)
	}
}

func receiveHTTPRealtime(t *testing.T, session *realtime.Session) realtime.ServerFrame {
	t.Helper()
	select {
	case raw := <-session.Frames():
		var frame realtime.ServerFrame
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatal(err)
		}
		return frame
	case <-time.After(time.Second):
		t.Fatal("missing realtime frame")
		return realtime.ServerFrame{}
	}
}

type httpRoutingMemory struct {
	mu    sync.Mutex
	rules map[string]domain.RoutingRule
}

func newHTTPRoutingMemory() *httpRoutingMemory {
	return &httpRoutingMemory{rules: make(map[string]domain.RoutingRule)}
}

func (memory *httpRoutingMemory) CreateRoutingRule(_ context.Context, rule domain.RoutingRule) error {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	if _, exists := memory.rules[rule.ID]; exists {
		return store.ErrConflict
	}
	memory.rules[rule.ID] = rule
	return nil
}

func (memory *httpRoutingMemory) ListRoutingRules(context.Context) ([]domain.RoutingRule, error) {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	var rules []domain.RoutingRule
	for _, rule := range memory.rules {
		if rule.DeletedAt == nil {
			rules = append(rules, rule)
		}
	}
	return rules, nil
}

func (memory *httpRoutingMemory) GetRoutingRule(_ context.Context, id string) (domain.RoutingRule, error) {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	rule, exists := memory.rules[id]
	if !exists || rule.DeletedAt != nil {
		return domain.RoutingRule{}, store.ErrNotFound
	}
	return rule, nil
}

func (memory *httpRoutingMemory) UpdateRoutingRule(_ context.Context, rule domain.RoutingRule) (domain.RoutingRule, error) {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	if _, exists := memory.rules[rule.ID]; !exists {
		return domain.RoutingRule{}, store.ErrNotFound
	}
	memory.rules[rule.ID] = rule
	return rule, nil
}

func (memory *httpRoutingMemory) DeleteRoutingRule(_ context.Context, id string, now time.Time) error {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	rule, exists := memory.rules[id]
	if !exists {
		return store.ErrNotFound
	}
	rule.DeletedAt = &now
	memory.rules[id] = rule
	return nil
}

func (memory *httpRoutingMemory) ResolveRoutingRules(_ context.Context, sourceAppID, eventType string) ([]domain.RoutingRule, error) {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	var rules []domain.RoutingRule
	for _, rule := range memory.rules {
		if rule.DeletedAt != nil || !rule.Enabled || rule.EventType != eventType {
			continue
		}
		if rule.SourceAppID != nil && *rule.SourceAppID != sourceAppID {
			continue
		}
		rules = append(rules, rule)
	}
	return rules, nil
}
