package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/dungxbuif/RelayHub/internal/auth"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/realtime"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/gorilla/websocket"
)

func wsFixture(t *testing.T) (*httptest.Server, *auth.TokenIssuer, *realtime.Hub) {
	t.Helper()
	issuer := auth.NewTokenIssuer([]byte("secret"), time.Now)
	hub := realtime.NewHub()
	srv := httptest.NewServer(NewRouter(Dependencies{TokenIssuer: issuer, Realtime: hub, AllowedOrigins: []string{"https://allowed.example"}, Docs: fstest.MapFS{}, Metrics: http.NotFoundHandler()}))
	t.Cleanup(func() { hub.Close(); srv.Close() })
	return srv, issuer, hub
}
func wsToken(t *testing.T, issuer *auth.TokenIssuer, app string, scopes ...string) string {
	t.Helper()
	token, err := issuer.Issue(app, scopes, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return token
}
func wsDial(t *testing.T, srv *httptest.Server, token, origin string) *websocket.Conn {
	t.Helper()
	headers := http.Header{}
	if origin != "" {
		headers.Set("Origin", origin)
	}
	c, r, e := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"/ws?token="+token, headers)
	if e != nil {
		if r != nil {
			t.Fatalf("dial status %d", r.StatusCode)
		}
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
func wsRead(t *testing.T, c *websocket.Conn) realtime.ServerFrame {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	var f realtime.ServerFrame
	if e := c.ReadJSON(&f); e != nil {
		t.Fatal(e)
	}
	return f
}
func TestWebSocketAuthAndOrigins(t *testing.T) {
	srv, issuer, _ := wsFixture(t)
	expired := auth.NewTokenIssuer([]byte("secret"), func() time.Time { return time.Now().Add(-2 * time.Minute) })
	for _, tc := range []struct {
		token, origin string
		status        int
	}{
		{"", "", 401}, {"invalid", "", 401}, {wsToken(t, expired, "a", "ws:connect"), "", 401}, {wsToken(t, issuer, "a", "ws:read"), "", 403}, {wsToken(t, issuer, "a", "ws:connect"), "https://evil.example", 403},
	} {
		c, r, e := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"/ws?token="+tc.token, http.Header{"Origin": []string{tc.origin}})
		if c != nil {
			_ = c.Close()
		}
		if e == nil || r == nil || r.StatusCode != tc.status {
			t.Fatalf("auth expected %d got %v", tc.status, r)
		}
		var body map[string]any
		err := json.NewDecoder(r.Body).Decode(&body)
		_ = r.Body.Close()
		if err != nil || body["error"] == nil {
			t.Fatal("nonstandard auth error")
		}
	}
	for _, origin := range []string{"", "https://allowed.example"} {
		c := wsDial(t, srv, wsToken(t, issuer, "a", "ws:connect"), origin)
		f := wsRead(t, c)
		if f.Type != "ready" || f.AppID != "a" || !strings.HasPrefix(f.ConnectionID, "conn_") {
			t.Fatalf("ready %#v", f)
		}
	}
}
func TestWebSocketFramesAndIsolation(t *testing.T) {
	srv, issuer, hub := wsFixture(t)
	a := wsDial(t, srv, wsToken(t, issuer, "a", "ws:connect"), "")
	b := wsDial(t, srv, wsToken(t, issuer, "b", "ws:connect"), "")
	wsRead(t, a)
	wsRead(t, b)
	for _, c := range []*websocket.Conn{a, b} {
		_ = c.WriteJSON(map[string]any{"type": "subscribe", "topics": []string{"events"}})
		if wsRead(t, c).Type != "subscribed" {
			t.Fatal("subscribe")
		}
	}
	for _, tc := range []struct{ raw, code string }{{`{`, "invalid_json"}, {`{"type":"weird"}`, "unknown_type"}, {`{"type":"subscribe","topics":["jobs"],"app_id":"b"}`, "invalid_frame"}, {`{"type":"rpc.result","ok":true}`, "invalid_rpc_result"}, {`{"type":"rpc.result","invocation_id":"inv_1","ok":true,"result":{}}`, "rpc_unavailable"}} {
		_ = a.WriteMessage(websocket.TextMessage, []byte(tc.raw))
		f := wsRead(t, a)
		if f.Type != "error" || f.Code != tc.code {
			t.Fatalf("error %#v", f)
		}
	}
	_ = hub.PublishEvent(context.Background(), domain.Event{ID: "only-a", TargetAppIDs: []string{"a"}})
	if wsRead(t, a).Event.ID != "only-a" {
		t.Fatal("event")
	}
	_ = b.WriteJSON(map[string]string{"type": "ping"})
	if wsRead(t, b).Type != "pong" {
		t.Fatal("cross app leak")
	}
	_ = a.WriteMessage(websocket.TextMessage, []byte(strings.Repeat("x", 65537)))
	_ = a.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := a.ReadMessage(); err == nil {
		t.Fatal("oversize connection remained open")
	}
}
func TestWebSocketHTTPPublishedEvent(t *testing.T) {
	apps := newHTTPMemoryStore()
	as := service.NewAppService(apps, service.AppOptions{})
	var creds []service.AppCredentials
	for _, name := range []string{"source", "target", "outsider"} {
		_, c, e := as.Create(context.Background(), service.CreateApp{Name: name, DeliveryMode: domain.DeliveryQueue})
		if e != nil {
			t.Fatal(e)
		}
		creds = append(creds, c)
	}
	hub := realtime.NewHub()
	defer hub.Close()
	issuer := auth.NewTokenIssuer([]byte("secret"), time.Now)
	mem := &httpEventMemory{events: map[string]domain.Event{}, jobs: map[string]domain.Job{}, idem: map[string]store.Publication{}}
	router := NewRouter(Dependencies{Apps: as, Events: service.NewEventService(mem, apps, service.EventOptions{Notifier: hub}), TokenIssuer: issuer, Realtime: hub, Now: func() time.Time { return time.Unix(1789120800, 0) }, Docs: fstest.MapFS{}, Metrics: http.NotFoundHandler()})
	srv := httptest.NewServer(router)
	defer srv.Close()
	var connections []*websocket.Conn
	for _, c := range creds {
		conn := wsDial(t, srv, wsToken(t, issuer, c.AppID, "ws:connect"), "")
		wsRead(t, conn)
		_ = conn.WriteJSON(map[string]any{"type": "subscribe", "topics": []string{"events"}})
		wsRead(t, conn)
		connections = append(connections, conn)
	}
	body := []byte(fmt.Sprintf(`{"type":"order.created","target_app_ids":[%q],"data":{"n":42}}`, creds[1].AppID))
	res := signedEventRequest(t, router, creds[0], "POST", "/api/v1/events", body, "ws-key")
	if res.Code != 202 {
		t.Fatalf("publish %d %s", res.Code, res.Body.String())
	}
	f := wsRead(t, connections[1])
	if f.Type != "event" || f.Event.SourceAppID != creds[0].AppID || string(f.Event.Data) != `{"n":42}` {
		t.Fatalf("event %#v", f)
	}
	for _, idx := range []int{0, 2} {
		_ = connections[idx].WriteJSON(map[string]string{"type": "ping"})
		if wsRead(t, connections[idx]).Type != "pong" {
			t.Fatal("event leaked")
		}
	}
}

func TestWebSocketControlFramesAndShutdown(t *testing.T) {
	srv, issuer, hub := wsFixture(t)
	c := wsDial(t, srv, wsToken(t, issuer, "control", "ws:connect"), "")
	wsRead(t, c)
	pong := false
	c.SetPongHandler(func(data string) error {
		pong = data == "probe"
		return c.WriteJSON(map[string]string{"type": "ping"})
	})
	if err := c.WriteControl(websocket.PingMessage, []byte("probe"), time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	// Only the protocol Pong handler sends the application ping, so ReadJSON
	// finishes after the control exchange without relying on scheduling order.
	if wsRead(t, c).Type != "pong" {
		t.Fatal("application pong missing")
	}
	if !pong {
		t.Fatal("protocol ping unanswered")
	}
	if err := c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"), time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	_, _, err := c.ReadMessage()
	if !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
		t.Fatalf("close handshake: %v", err)
	}
	live := wsDial(t, srv, wsToken(t, issuer, "live", "ws:connect"), "")
	wsRead(t, live)
	hub.Close()
	_ = live.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := live.ReadMessage(); err == nil {
		t.Fatal("shutdown retained socket")
	}
}

// rawClientFrame emits masked wire frames so tests control the fragmentation
// boundary independently of Gorilla's client-side message writer.
func rawClientFrame(t *testing.T, c *websocket.Conn, opcode byte, final bool, payload []byte) {
	t.Helper()
	if len(payload) >= 126 {
		t.Fatal("raw test helper supports only short frames")
	}
	first := opcode
	if final {
		first |= 0x80
	}
	mask := [4]byte{0x12, 0x34, 0x56, 0x78}
	wire := []byte{first, 0x80 | byte(len(payload)), mask[0], mask[1], mask[2], mask[3]}
	for i, value := range payload {
		wire = append(wire, value^mask[i%len(mask)])
	}
	if err := c.UnderlyingConn().SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	for len(wire) > 0 {
		n, err := c.UnderlyingConn().Write(wire)
		if err != nil {
			t.Fatal(err)
		}
		wire = wire[n:]
	}
}

func utf8WebSocket(t *testing.T) (*websocket.Conn, <-chan struct{}) {
	t.Helper()
	issuer := auth.NewTokenIssuer([]byte("utf8-test-secret"), time.Now)
	hub := realtime.NewHub()
	handlerDone := make(chan struct{})
	router := NewRouter(Dependencies{TokenIssuer: issuer, Realtime: hub, Docs: fstest.MapFS{}, Metrics: http.NotFoundHandler()})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		router.ServeHTTP(w, r)
	}))
	t.Cleanup(func() { hub.Close(); srv.Close() })
	conn := wsDial(t, srv, wsToken(t, issuer, "utf8-app", "ws:connect"), "")
	if wsRead(t, conn).Type != "ready" {
		t.Fatal("missing ready")
	}
	return conn, handlerDone
}

func TestWebSocketInvalidUTF8Closes1007(t *testing.T) {
	for _, tc := range []struct {
		name       string
		invalid    []byte
		fragmented bool
	}{
		{name: "invalid leading byte", invalid: []byte{0xff}},
		{name: "invalid continuation", invalid: []byte{0xc3, 0x28}},
		{name: "truncated code point", invalid: []byte{0xf0, 0x9f}},
		{name: "invalid fragmented sequence", invalid: []byte{0xe2, 0x28, 0xac}, fragmented: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, handlerDone := utf8WebSocket(t)
			prefix := []byte(`{"type":"ping","error":"`)
			raw := append(append(prefix, tc.invalid...), []byte(`"}`)...)
			if tc.fragmented {
				split := len(prefix) + 1
				rawClientFrame(t, conn, websocket.TextMessage, false, raw[:split])
				rawClientFrame(t, conn, 0, true, raw[split:])
			} else {
				rawClientFrame(t, conn, websocket.TextMessage, true, raw)
			}
			_ = conn.SetReadDeadline(time.Now().Add(time.Second))
			_, payload, err := conn.ReadMessage()
			if !websocket.IsCloseError(err, websocket.CloseInvalidFramePayloadData) {
				t.Fatalf("invalid UTF-8 must close 1007, got payload %q error %v", payload, err)
			}
			select {
			case <-handlerDone:
			case <-time.After(time.Second):
				t.Fatal("UTF-8 failure did not terminate both session goroutines")
			}
		})
	}
}

func TestWebSocketUTF8AcrossFragments(t *testing.T) {
	conn, _ := utf8WebSocket(t)
	// The first fragment ends after E2, the leading byte of the three-byte euro
	// character. Neither fragment is UTF-8 independently; the complete text is.
	prefix := []byte(`{"type":"rpc.result","invocation_id":"inv_1","ok":true,"result":{"value":"`)
	raw := append(append(prefix, []byte("€")...), []byte(`"}}`)...)
	split := len(prefix) + 1
	rawClientFrame(t, conn, websocket.TextMessage, false, raw[:split])
	rawClientFrame(t, conn, 0, true, raw[split:])
	response := wsRead(t, conn)
	if response.Type != "error" || response.Code != "rpc_unavailable" {
		t.Fatalf("valid reassembled text was not decoded: %#v", response)
	}
	if err := conn.WriteJSON(map[string]string{"type": "ping"}); err != nil {
		t.Fatal(err)
	}
	if wsRead(t, conn).Type != "pong" {
		t.Fatal("valid fragmented message closed the connection")
	}
}
