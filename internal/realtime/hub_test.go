package realtime

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/gorilla/websocket"
)

func receive(t *testing.T, s *Session) ServerFrame {
	t.Helper()
	select {
	case b := <-s.Frames():
		var f ServerFrame
		if err := json.Unmarshal(b, &f); err != nil {
			t.Fatal(err)
		}
		return f
	case <-time.After(time.Second):
		t.Fatal("missing frame")
		return ServerFrame{}
	}
}
func TestHubIsolationAndSubscriptions(t *testing.T) {
	h := NewHub()
	defer h.Close()
	a := h.Register("a")
	a2 := h.Register("a")
	b := h.Register("b")
	e := domain.Event{ID: "evt_1", TargetAppIDs: []string{"a"}}
	_ = h.PublishEvent(context.Background(), e)
	if len(a.outbound) != 0 {
		t.Fatal("delivery before subscription")
	}
	for _, s := range []*Session{a, a2, b} {
		if err := h.Subscribe(s, []string{"events"}); err != nil {
			t.Fatal(err)
		}
	}
	_ = h.PublishEvent(context.Background(), e)
	for _, s := range []*Session{a, a2} {
		if f := receive(t, s); f.Event == nil || f.Event.ID != e.ID {
			t.Fatalf("bad event %#v", f)
		}
	}
	if len(b.outbound) != 0 {
		t.Fatal("cross-app delivery")
	}
	_ = h.PublishJob(context.Background(), domain.Job{ID: "j", TargetAppID: "a"})
	if len(a.outbound) != 0 {
		t.Fatal("unsubscribed jobs")
	}
	if err := h.Subscribe(a, []string{"jobs"}); err != nil {
		t.Fatal(err)
	}
	_ = h.PublishJob(context.Background(), domain.Job{ID: "j", TargetAppID: "a"})
	if receive(t, a).Type != "job.updated" {
		t.Fatal("missing job")
	}
	if err := h.Subscribe(a, []string{"functions"}); err != nil {
		t.Fatal(err)
	}
	if err := h.InvokeFunction(context.Background(), "a", ServerFrame{Type: "rpc.invoke"}); err == nil {
		t.Fatal("function invoked")
	}
	if err := h.HandleResult(a, ClientFrame{Type: "rpc.result"}); err == nil {
		t.Fatal("rpc routed")
	}
	h.Disconnect(a)
	h.Disconnect(a)
	select {
	case <-a.Done():
	default:
		t.Fatal("not closed")
	}
}

type routeRecorder struct {
	ensured  []string
	released []string
	err      error
}

func (routes *routeRecorder) EnsureFunctionRoute(app string) error {
	routes.ensured = append(routes.ensured, app)
	return routes.err
}
func (routes *routeRecorder) ReleaseFunctionRoute(app string) {
	routes.released = append(routes.released, app)
}

func TestHubFunctionRouteFollowsEligibleLocalSessions(t *testing.T) {
	hub := NewHub()
	routes := &routeRecorder{}
	hub.SetFunctionRoutes(routes)
	first, second := hub.Register("owner"), hub.Register("owner")
	if err := hub.Subscribe(first, []string{"functions"}); err != nil {
		t.Fatal(err)
	}
	if err := hub.Subscribe(second, []string{"functions"}); err != nil {
		t.Fatal(err)
	}
	if len(routes.ensured) != 1 || routes.ensured[0] != "owner" {
		t.Fatalf("ensured=%v", routes.ensured)
	}
	first.Close()
	if len(routes.released) != 0 {
		t.Fatalf("route released with eligible session: %v", routes.released)
	}
	second.Close()
	if len(routes.released) != 1 || routes.released[0] != "owner" {
		t.Fatalf("released=%v", routes.released)
	}
}

func TestHubRejectsFunctionSubscriptionWhenRouteCannotStart(t *testing.T) {
	hub := NewHub()
	hub.SetFunctionRoutes(&routeRecorder{err: errors.New("NATS unavailable")})
	session := hub.Register("owner")
	if err := hub.Subscribe(session, []string{"functions"}); err == nil || err.Code != "function_unavailable" {
		t.Fatalf("Subscribe()=%v", err)
	}
	if len(hub.FunctionSessions("owner")) != 0 {
		t.Fatal("failed route made session eligible")
	}
}
func TestHubSlowClientAndShutdown(t *testing.T) {
	h := NewHub()
	s := h.Register("a")
	_ = h.Subscribe(s, []string{"events"})
	e := domain.Event{TargetAppIDs: []string{"a"}}
	for range 64 {
		_ = h.PublishEvent(context.Background(), e)
	}
	select {
	case <-s.Done():
		t.Fatal("closed before queue full")
	default:
	}
	_ = h.PublishEvent(context.Background(), e)
	select {
	case <-s.Done():
	case <-time.After(time.Second):
		t.Fatal("slow client not closed")
	}
	h.Close()
	later := h.Register("a")
	select {
	case <-later.Done():
	default:
		t.Fatal("registration after shutdown")
	}
}
func TestHubConcurrentPublishClose(t *testing.T) {
	h := NewHub()
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := h.Register("a")
			_ = h.Subscribe(s, []string{"events"})
			for range 100 {
				_ = h.PublishEvent(context.Background(), domain.Event{TargetAppIDs: []string{"a"}})
			}
			s.Close()
		}()
	}
	wg.Add(1)
	go func() { defer wg.Done(); h.Close() }()
	wg.Wait()
	h.Close()
}

// gatedSocket holds only server WebSocket writes. The HTTP upgrade completes
// before arming it, and Close always unblocks a held write during test cleanup.
type gatedSocket struct {
	net.Conn
	armed                     bool
	entered, release, stopped chan struct{}
	enterOnce, stopOnce       sync.Once
}

func (c *gatedSocket) Write(raw []byte) (int, error) {
	if c.armed {
		c.enterOnce.Do(func() { close(c.entered) })
		select {
		case <-c.release:
		case <-c.stopped:
			return 0, net.ErrClosed
		}
	}
	return c.Conn.Write(raw)
}
func (c *gatedSocket) Close() error {
	c.stopOnce.Do(func() { close(c.stopped) })
	return c.Conn.Close()
}

type gatedHijacker struct {
	http.ResponseWriter
	socket *gatedSocket
}

func (w *gatedHijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := w.ResponseWriter.(http.Hijacker).Hijack()
	if err != nil {
		return nil, nil, err
	}
	w.socket = &gatedSocket{Conn: conn, entered: make(chan struct{}), release: make(chan struct{}), stopped: make(chan struct{})}
	return w.socket, bufio.NewReadWriter(rw.Reader, bufio.NewWriter(w.socket)), nil
}

func TestSessionFatalClosePrioritizesSaturatedPongs(t *testing.T) {
	hub := NewHub()
	defer hub.Close()
	type connected struct {
		session *Session
		socket  *gatedSocket
	}
	ready := make(chan connected, 1)
	handlerDone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		gated := &gatedHijacker{ResponseWriter: w}
		conn, err := (&websocket.Upgrader{}).Upgrade(gated, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		gated.socket.armed = true
		session := hub.Register("saturated-client")
		session.Send(ServerFrame{Type: "ready"})
		ready <- connected{session, gated.socket}
		session.Serve(conn)
	}))
	defer server.Close()
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	peer := <-ready
	defer peer.session.Close()
	select {
	case <-peer.socket.entered:
	case <-time.After(time.Second):
		t.Fatal("writer did not reach transport gate")
	}
	// The writer is held inside its ready-frame write, so every Ping must occupy
	// one Pong slot without any writer draining the bounded control channel.
	for range OutboundQueueSize {
		if err := client.WriteControl(websocket.PingMessage, []byte("p"), time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(time.Second)
	for len(peer.session.controls) != OutboundQueueSize && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(peer.session.controls) != OutboundQueueSize {
		t.Fatal("Pong queue did not saturate")
	}
	// A masked one-byte text frame containing FF is not valid UTF-8.
	if _, err := client.UnderlyingConn().Write([]byte{0x81, 0x81, 1, 2, 3, 4, 0xff ^ 1}); err != nil {
		t.Fatal(err)
	}
	// Keep the transport gate shut while the reader handles invalid text. The
	// broken path terminates immediately; a reliable close waits for its writer.
	select {
	case <-peer.session.Done():
	case <-time.After(100 * time.Millisecond):
	}
	close(peer.socket.release)
	pongs := 0
	client.SetPongHandler(func(string) error { pongs++; return nil })
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	for {
		_, _, err = client.ReadMessage()
		if err != nil {
			break
		}
	}
	if !websocket.IsCloseError(err, websocket.CloseInvalidFramePayloadData) {
		t.Fatalf("saturated Pong queue lost close 1007: %v", err)
	}
	if pongs != 0 {
		t.Fatalf("fatal close was delayed behind %d queued Pongs", pongs)
	}
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("fatal close did not terminate session handler")
	}
}

func TestSessionPendingCloseCancelledByShutdown(t *testing.T) {
	hub := NewHub()
	defer hub.Close()
	session := hub.Register("pending-close")
	for range OutboundQueueSize {
		session.controls <- controlFrame{kind: websocket.PongMessage}
	}
	completed := make(chan struct{})
	go func() { session.writeClose(websocket.CloseInvalidFramePayloadData); close(completed) }()
	select {
	case <-completed:
		t.Fatal("close request was discarded instead of waiting for the writer")
	case <-time.After(20 * time.Millisecond):
	}
	hub.Close()
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not release pending close")
	}
}

type rpcBackend struct {
	mu                            sync.Mutex
	owner, connection, invocation string
	closeOnClaim                  bool
	session                       *Session
	completed                     domain.RPCResult
}

func (b *rpcBackend) ClaimInvocation(_ context.Context, app, conn, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.connection != "" {
		return fmt.Errorf("already claimed")
	}
	b.owner = app
	b.connection = conn
	b.invocation = id
	if b.closeOnClaim {
		b.session.Close()
	}
	return nil
}
func (b *rpcBackend) AcknowledgeInvocation(context.Context, string, string, string) error { return nil }
func (b *rpcBackend) ReleaseInvocation(_ context.Context, app, conn, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.connection = ""
	return nil
}
func (b *rpcBackend) CompleteResult(_ context.Context, app, conn string, r domain.RPCResult) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if app != b.owner || conn != b.connection || r.InvocationID != b.invocation {
		return fmt.Errorf("invalid")
	}
	b.completed = r
	return nil
}
func TestFunctionHubOneConnectionAndOwnerIdentity(t *testing.T) {
	h := NewHub()
	defer h.Close()
	b := &rpcBackend{}
	h.SetFunctions(b)
	owner, second, other := h.Register("owner"), h.Register("owner"), h.Register("other")
	for _, s := range []*Session{owner, second, other} {
		if e := h.Subscribe(s, []string{"functions"}); e != nil {
			t.Fatal(e)
		}
	}
	if len(h.FunctionSessions("owner")) != 2 {
		t.Fatal("eligible sessions")
	}
	f := ServerFrame{Type: "rpc.invoke", InvocationID: "inv_1", Function: "calculate", Input: json.RawMessage(`{}`), Deadline: time.Now().Add(time.Second).Format(time.RFC3339Nano)}
	if e := h.InvokeFunction(context.Background(), "owner", f); e != nil {
		t.Fatal(e)
	}
	if len(owner.outbound)+len(second.outbound) != 1 || len(other.outbound) != 0 {
		t.Fatal("invocation fanout or identity leak")
	}
	selected := owner
	if len(second.outbound) > 0 {
		selected = second
	}
	if receive(t, selected).Function != "calculate" {
		t.Fatal("wrong function")
	}
	ok := true
	r := ClientFrame{Type: "rpc.result", InvocationID: "inv_1", OK: &ok, Result: json.RawMessage(`{"value":42}`)}
	if e := h.HandleResult(other, r); e == nil || e.Code != "invalid_rpc_result" {
		t.Fatalf("cross-owner %v", e)
	}
	if e := h.HandleResult(selected, r); e != nil {
		t.Fatal(e)
	}
	if b.owner != "owner" || b.connection != selected.ID() || string(b.completed.Result) != `{"value":42}` {
		t.Fatal("token-derived identity lost")
	}
}
func TestFunctionHubClosedAndSlowClaimRelease(t *testing.T) {
	for _, closeClaim := range []bool{true, false} {
		h := NewHub()
		s := h.Register("owner")
		_ = h.Subscribe(s, []string{"functions"})
		b := &rpcBackend{closeOnClaim: closeClaim, session: s}
		h.SetFunctions(b)
		if !closeClaim {
			for range OutboundQueueSize {
				s.Send(ServerFrame{Type: "pong"})
			}
		}
		if e := h.InvokeFunction(context.Background(), "owner", ServerFrame{Type: "rpc.invoke", InvocationID: "inv_1"}); e == nil {
			t.Fatal("closed/slow claim delivered")
		}
		if b.connection != "" {
			t.Fatal("undelivered claim retained")
		}
		h.Close()
	}
}
