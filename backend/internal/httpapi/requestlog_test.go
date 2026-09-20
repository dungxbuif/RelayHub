package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/dungxbuif/RelayHub/internal/auth"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/realtime"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/gorilla/websocket"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// Logging a raw URL, headers, request ID supplied by a client, or body leaks
// sentinel secrets. Omitting generated IDs/status/route/outcome loses audit data.
func TestRequestLogUsesGeneratedIDsAndRedactsSensitiveData(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	router := chi.NewRouter()
	router.Use(middleware.RequestID)
	router.Use(requestLog(logger))
	router.Post("/api/v1/events/{eventID}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(202)
		_, _ = w.Write([]byte(`{"result":"SENTINEL_RESULT"}`))
	})
	request := httptest.NewRequest("POST", "/api/v1/events/SENTINEL_PATH?token=SENTINEL_TOKEN", strings.NewReader(`{"data":"SENTINEL_PAYLOAD","input":"SENTINEL_INPUT"}`))
	for key, value := range map[string]string{"Authorization": "Bearer SENTINEL_ADMIN", "X-RelayHub-Api-Key": "SENTINEL_API_KEY", "X-RelayHub-Signature": "SENTINEL_SIGNATURE", "X-Request-Id": "SENTINEL_REQUEST_ID", "X-Secret": "SENTINEL_SECRET"} {
		request.Header.Set(key, value)
	}
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if strings.Contains(output.String(), "SENTINEL") {
		t.Fatalf("sensitive data logged")
	}
	var entry map[string]any
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatal(err)
	}
	if entry["request_id"] == nil || entry["request_id"] == "" || entry["route"] != "/api/v1/events/{eventID}" || entry["status"] != float64(202) || entry["method"] != "POST" || entry["outcome"] != "success" || entry["latency_ms"] == nil {
		t.Fatal("missing bounded structured request fields")
	}
	if entry["request_id"] != response.Header().Get("X-Request-ID") || entry["response_bytes"] != float64(response.Body.Len()) {
		t.Fatal("response correlation or byte count missing")
	}
}

func TestRequestLogSeverity(t *testing.T) {
	for _, tc := range []struct {
		status int
		level  string
	}{{200, "INFO"}, {401, "WARN"}, {503, "ERROR"}} {
		t.Run(tc.level, func(t *testing.T) {
			var output bytes.Buffer
			handler := requestLog(slog.New(slog.NewJSONHandler(&output, nil)))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.status) }))
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
			var entry map[string]any
			if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
				t.Fatal(err)
			}
			if entry["level"] != tc.level {
				t.Fatalf("level=%v want=%s", entry["level"], tc.level)
			}
		})
	}
}
func TestRequestLogUnmatchedPathAndUnknownMethodAreBounded(t *testing.T) {
	var output bytes.Buffer
	router := chi.NewRouter()
	router.Use(requestLog(slog.New(slog.NewJSONHandler(&output, nil))))
	router.Get("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("SENTINEL_METHOD", "/SENTINEL_PATH?token=SENTINEL_QUERY", nil))
	if strings.Contains(output.String(), "SENTINEL") {
		t.Fatal("unmatched request leaked")
	}
	var entry map[string]any
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatal(err)
	}
	if entry["route"] != "unmatched" || entry["method"] != "OTHER" || entry["outcome"] != "client_error" {
		t.Fatal("unbounded fallback log fields")
	}
}

func TestRequestLogReportsWebSocketUpgrade(t *testing.T) {
	var output bytes.Buffer
	finished := make(chan struct{})
	router := chi.NewRouter()
	router.Use(requestLog(slog.New(slog.NewJSONHandler(&output, nil))))
	router.Get("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, _, _ = conn.ReadMessage()
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { router.ServeHTTP(w, r); close(finished) }))
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	<-finished
	var entry map[string]any
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatal(err)
	}
	if entry["status"] != float64(101) {
		t.Fatalf("WebSocket upgrade status=%v", entry["status"])
	}
}

type capturedLog struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (c *capturedLog) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.b.Write(p)
}
func (c *capturedLog) text() string { c.mu.Lock(); defer c.mu.Unlock(); return c.b.String() }
func TestOperationLogsTypedEventAndInvocationIDsWithoutSensitiveValues(t *testing.T) {
	var logs capturedLog
	apps := newHTTPMemoryStore()
	appService := service.NewAppService(apps, service.AppOptions{})
	var credentials []service.AppCredentials
	for _, name := range []string{"SENTINEL_PRODUCER_NAME", "SENTINEL_CONSUMER_NAME"} {
		_, c, e := appService.Create(context.Background(), service.CreateApp{Name: name, DeliveryMode: domain.DeliveryQueue})
		if e != nil {
			t.Fatal(e)
		}
		credentials = append(credentials, c)
	}
	events := &httpEventMemory{events: map[string]domain.Event{}, jobs: map[string]domain.Job{}, idem: map[string]store.Publication{}}
	hub := realtime.NewHub()
	defer hub.Close()
	issuer := auth.NewTokenIssuer([]byte("SENTINEL_SERVER_SECRET"), time.Now)
	functions := service.NewFunctionService(newFunctionMemory(), service.FunctionOptions{Notifier: hub})
	router := NewRouter(Dependencies{Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Apps: appService, Events: service.NewEventService(events, apps, service.EventOptions{}), Functions: functions, Realtime: hub, TokenIssuer: issuer, AdminToken: "SENTINEL_ADMIN", Now: func() time.Time { return time.Unix(1789120800, 0) }})
	body := []byte(fmt.Sprintf(`{"type":"SENTINEL_EVENT_TYPE","target_app_ids":[%q],"data":{"secret":"SENTINEL_PAYLOAD"}}`, credentials[1].AppID))
	res := signedEventRequest(t, router, credentials[0], "POST", "/api/v1/events", body, "SENTINEL_IDEMPOTENCY_KEY")
	if res.Code != 202 {
		t.Fatal("publish failed")
	}
	var pub store.Publication
	if e := json.Unmarshal(res.Body.Bytes(), &pub); e != nil {
		t.Fatal(e)
	}
	registration := signedEventRequest(t, router, credentials[1], "POST", "/api/v1/functions", []byte(`{"name":"SENTINEL_FUNCTION_NAME","timeout_seconds":1}`), "")
	if registration.Code != 201 {
		t.Fatal("register failed")
	}
	var fn domain.Function
	_ = json.Unmarshal(registration.Body.Bytes(), &fn)
	server := httptest.NewServer(router)
	defer server.Close()
	owner := wsDial(t, server, wsToken(t, issuer, credentials[1].AppID, "ws:connect"), "")
	wsRead(t, owner)
	_ = owner.WriteJSON(map[string]any{"type": "subscribe", "topics": []string{"functions"}})
	wsRead(t, owner)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- signedEventRequest(t, router, credentials[0], "POST", "/api/v1/functions/"+fn.ID+"/invoke", []byte(`{"input":{"value":"SENTINEL_INPUT"}}`), "SENTINEL_RPC_KEY")
	}()
	frame := wsRead(t, owner)
	_ = owner.WriteJSON(map[string]any{"type": "rpc.result", "invocation_id": frame.InvocationID, "ok": true, "result": map[string]string{"value": "SENTINEL_RESULT"}})
	if (<-done).Code != 200 {
		t.Fatal("invoke failed")
	}
	// A successful replay may carry an arbitrary changed function path; never log it.
	replay := signedEventRequest(t, router, credentials[0], "POST", "/api/v1/functions/SENTINEL_REPLAY_PATH/invoke", []byte(`{"input":{}}`), "SENTINEL_RPC_KEY")
	if replay.Code != 200 {
		t.Fatal("replay failed")
	}
	text := logs.text()
	if strings.Contains(text, "SENTINEL") {
		t.Fatal("sensitive value in operation logs")
	}
	for _, c := range credentials {
		if strings.Contains(text, c.APIKey) || strings.Contains(text, c.HMACSecret) {
			t.Fatal("credential logged")
		}
	}
	has := func(fields map[string]any) bool {
		for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
			var v map[string]any
			if json.Unmarshal([]byte(line), &v) != nil {
				t.Fatal("invalid log JSON")
			}
			match := true
			for k, want := range fields {
				if v[k] != want {
					match = false
				}
			}
			if match {
				return true
			}
		}
		return false
	}
	for _, fields := range []map[string]any{
		{"app_id": credentials[0].AppID, "event_id": pub.Event.ID, "job_id": pub.Jobs[0].ID, "outcome": "published"},
		{"app_id": credentials[1].AppID, "function_id": fn.ID, "outcome": "registered"},
		{"app_id": credentials[0].AppID, "invocation_id": frame.InvocationID, "outcome": "success"},
		{"app_id": credentials[0].AppID, "route": "/api/v1/events", "status": float64(202)},
	} {
		if !has(fields) {
			t.Errorf("required typed operation fields absent: %v", fields)
		}
	}
}

func TestWebSocketLogIncludesVerifiedAppIDWithoutToken(t *testing.T) {
	var logs capturedLog
	hub := realtime.NewHub()
	defer hub.Close()
	issuer := auth.NewTokenIssuer([]byte("SENTINEL_SERVER_SECRET"), time.Now)
	token := wsToken(t, issuer, "app_websocket_expected", "ws:connect")
	router := NewRouter(Dependencies{Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Realtime: hub, TokenIssuer: issuer})
	finished := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { router.ServeHTTP(w, r); close(finished) }))
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/ws?token="+token+"&extra=SENTINEL_QUERY", http.Header{"X-Request-Id": []string{"SENTINEL_CLIENT_ID"}})
	if err != nil {
		t.Fatal(err)
	}
	var ready map[string]any
	if err := conn.ReadJSON(&ready); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("socket log deadline")
	}
	text := logs.text()
	if strings.Contains(text, token) || strings.Contains(text, "SENTINEL") {
		t.Fatal("socket credential or query logged")
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(text), &entry); err != nil {
		t.Fatal(err)
	}
	if entry["app_id"] != "app_websocket_expected" || entry["status"] != float64(101) || entry["route"] != "/ws" {
		t.Fatal("socket verified app identity/status missing")
	}
}
