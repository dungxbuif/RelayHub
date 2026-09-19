package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dungxbuif/RelayHub/internal/auth"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/realtime"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/dungxbuif/RelayHub/internal/store"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"
	"unicode/utf8"
)

// The in-memory boundary replaces external Redis only; service validation,
// subscription ordering, cancellation and terminal-response mapping are real.
type functionMemory struct {
	store.FunctionStore
	mu        sync.Mutex
	functions map[string]domain.Function
	calls     map[string]domain.Invocation
	keys      map[string]string
	watches   map[string]int
}

func newFunctionMemory() *functionMemory {
	return &functionMemory{functions: map[string]domain.Function{}, calls: map[string]domain.Invocation{}, keys: map[string]string{}, watches: map[string]int{}}
}
func (m *functionMemory) CreateFunction(_ context.Context, f domain.Function) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, old := range m.functions {
		if old.AppID == f.AppID && old.Name == f.Name {
			return store.ErrConflict
		}
	}
	m.functions[f.ID] = f
	return nil
}
func (m *functionMemory) GetFunction(_ context.Context, id string) (domain.Function, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.functions[id]
	if !ok {
		return f, store.ErrNotFound
	}
	return f, nil
}
func (m *functionMemory) ListFunctions(_ context.Context, app string) ([]domain.Function, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []domain.Function{}
	for _, f := range m.functions {
		if f.AppID == app {
			out = append(out, f)
		}
	}
	return out, nil
}
func (m *functionMemory) DeleteFunction(_ context.Context, app, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.functions[id]
	if !ok || f.AppID != app {
		return store.ErrNotFound
	}
	delete(m.functions, id)
	return nil
}
func (m *functionMemory) FindInvocation(_ context.Context, app, key string) (domain.Invocation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, ok := m.keys[app+":"+key]
	if !ok {
		return domain.Invocation{}, store.ErrNotFound
	}
	return m.expire(id), nil
}
func (m *functionMemory) CreateInvocation(_ context.Context, v domain.Invocation, key string) (domain.Invocation, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := v.CallerAppID + ":" + key
	if id, ok := m.keys[k]; ok {
		return m.expire(id), true, nil
	}
	m.keys[k] = v.ID
	m.calls[v.ID] = v
	return v, false, nil
}
func (m *functionMemory) expire(id string) domain.Invocation {
	v := m.calls[id]
	if !v.Terminal() {
		if v.State == domain.InvocationClaimed && !time.Now().Before(v.Deadline) {
			v.State = domain.InvocationTimeout
		} else if v.State != domain.InvocationClaimed && !time.Now().Before(v.ClaimBy) {
			v.State = domain.InvocationUnavailable
		}
		m.calls[id] = v
	}
	return v
}
func (m *functionMemory) GetInvocation(_ context.Context, id string) (domain.Invocation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.calls[id]; !ok {
		return domain.Invocation{}, store.ErrNotFound
	}
	return m.expire(id), nil
}
func (m *functionMemory) ClaimInvocation(_ context.Context, app, conn, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := m.expire(id)
	if v.OwnerAppID != app || v.State != domain.InvocationPending {
		return store.ErrInvalidResult
	}
	v.State = domain.InvocationReserved
	v.ConnectionID = conn
	m.calls[id] = v
	return nil
}
func (m *functionMemory) AcknowledgeInvocation(_ context.Context, app, conn, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := m.expire(id)
	if v.OwnerAppID != app || v.ConnectionID != conn || v.State != domain.InvocationReserved {
		return store.ErrInvalidResult
	}
	v.State = domain.InvocationClaimed
	m.calls[id] = v
	return nil
}
func (m *functionMemory) ReleaseInvocation(_ context.Context, app, conn, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := m.expire(id)
	if v.OwnerAppID != app || v.ConnectionID != conn || v.Terminal() {
		return store.ErrInvalidResult
	}
	v.State = domain.InvocationPending
	v.ConnectionID = ""
	m.calls[id] = v
	return nil
}
func (m *functionMemory) CompleteInvocation(_ context.Context, app, conn string, r domain.RPCResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := m.expire(r.InvocationID)
	if v.OwnerAppID != app || v.ConnectionID != conn || v.State != domain.InvocationClaimed {
		return store.ErrInvalidResult
	}
	v.Reply = &r
	v.State = domain.InvocationSuccess
	if !r.OK {
		v.State = domain.InvocationHandlerError
	}
	m.calls[v.ID] = v
	return nil
}

type silentWatch struct {
	ch    chan struct{}
	close func()
}

func (w silentWatch) Updates() <-chan struct{} { return w.ch }
func (w silentWatch) Close()                   { w.close() }
func (m *functionMemory) WatchInvocation(_ context.Context, id string) (store.InvocationWatch, error) {
	m.mu.Lock()
	m.watches[id]++
	m.mu.Unlock()
	return silentWatch{make(chan struct{}, 1), func() { m.mu.Lock(); m.watches[id]--; m.mu.Unlock() }}, nil
}

func functionHTTPFixture(t *testing.T) (http.Handler, *httptest.Server, *auth.TokenIssuer, []service.AppCredentials) {
	t.Helper()
	apps := newHTTPMemoryStore()
	as := service.NewAppService(apps, service.AppOptions{})
	creds := []service.AppCredentials{}
	for _, name := range []string{"owner", "caller", "other"} {
		_, c, e := as.Create(context.Background(), service.CreateApp{Name: name, DeliveryMode: domain.DeliveryQueue})
		if e != nil {
			t.Fatal(e)
		}
		creds = append(creds, c)
	}
	hub := realtime.NewHub()
	issuer := auth.NewTokenIssuer([]byte("secret"), time.Now)
	functions := service.NewFunctionService(newFunctionMemory(), service.FunctionOptions{Notifier: hub})
	router := NewRouter(Dependencies{Apps: as, Functions: functions, Realtime: hub, TokenIssuer: issuer, Now: func() time.Time { return time.Unix(1789120800, 0) }, Docs: fstest.MapFS{}, Metrics: http.NotFoundHandler()})
	srv := httptest.NewServer(router)
	t.Cleanup(func() { hub.Close(); srv.Close() })
	return router, srv, issuer, creds
}
func registerHTTPFunction(t *testing.T, h http.Handler, c service.AppCredentials) domain.Function {
	t.Helper()
	res := signedEventRequest(t, h, c, "POST", "/api/v1/functions", []byte(`{"name":"calculate","timeout_seconds":1}`), "")
	if res.Code != 201 {
		t.Fatalf("register %d %s", res.Code, res.Body.String())
	}
	var f domain.Function
	if e := json.Unmarshal(res.Body.Bytes(), &f); e != nil {
		t.Fatal(e)
	}
	return f
}
func TestFunctionHTTPManagement(t *testing.T) {
	h, _, _, c := functionHTTPFixture(t)
	f := registerHTTPFunction(t, h, c[0])
	if f.AppID != c[0].AppID || !f.Enabled {
		t.Fatalf("owner %#v", f)
	}
	for _, tc := range []struct {
		body   string
		status int
	}{{`{"name":"calculate","timeout_seconds":1}`, 409}, {`{"name":"bad name","timeout_seconds":1}`, 400}, {`{"name":"calc","timeout_seconds":31}`, 400}, {`{"name":"calc","timeout_seconds":1,"app_id":"victim"}`, 400}} {
		res := signedEventRequest(t, h, c[0], "POST", "/api/v1/functions", []byte(tc.body), "")
		if res.Code != tc.status {
			t.Fatalf("management %d %s", res.Code, res.Body.String())
		}
	}
	if res := signedEventRequest(t, h, c[1], "GET", "/api/v1/functions", nil, ""); res.Code != 200 || strings.TrimSpace(res.Body.String()) != "[]" {
		t.Fatalf("owner list %d %s", res.Code, res.Body.String())
	}
	res := signedEventRequest(t, h, c[0], "GET", "/api/v1/functions", nil, "")
	if res.Code != 200 || !strings.Contains(res.Body.String(), f.ID) {
		t.Fatal("missing own function")
	}
	if res := signedEventRequest(t, h, c[1], "DELETE", "/api/v1/functions/"+f.ID, nil, ""); res.Code != 404 {
		t.Fatalf("delete owner %d", res.Code)
	}
	if res := signedEventRequest(t, h, c[0], "DELETE", "/api/v1/functions/"+f.ID, nil, ""); res.Code != 204 {
		t.Fatalf("delete %d", res.Code)
	}
	for _, route := range []struct{ method, path string }{{"GET", "/api/v1/functions"}, {"POST", "/api/v1/functions"}, {"DELETE", "/api/v1/functions/" + f.ID}, {"POST", "/api/v1/functions/" + f.ID + "/invoke"}} {
		if res := requestJSON(t, h, route.method, route.path, nil, nil); res.Code != 401 {
			t.Fatalf("unsigned route %s %d", route.path, res.Code)
		}
	}
}
func TestFunctionHTTPWebSocketRoundTripAndReplay(t *testing.T) {
	for _, ok := range []bool{true, false} {
		t.Run(fmt.Sprint(ok), func(t *testing.T) {
			h, srv, issuer, c := functionHTTPFixture(t)
			f := registerHTTPFunction(t, h, c[0])
			owner := wsDial(t, srv, wsToken(t, issuer, c[0].AppID, "ws:connect"), "")
			wsRead(t, owner)
			_ = owner.WriteJSON(map[string]any{"type": "subscribe", "topics": []string{"functions"}})
			if wsRead(t, owner).Type != "subscribed" {
				t.Fatal("functions rejected")
			}
			outsider := wsDial(t, srv, wsToken(t, issuer, c[2].AppID, "ws:connect"), "")
			wsRead(t, outsider)
			path := "/api/v1/functions/" + f.ID + "/invoke"
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				done <- signedEventRequest(t, h, c[1], "POST", path, []byte(`{"input":{"n":9007199254740993}}`), "call")
			}()
			frame := wsRead(t, owner)
			if frame.Type != "rpc.invoke" || frame.Function != "calculate" || string(frame.Input) != `{"n":9007199254740993}` || frame.AppID != "" || frame.ConnectionID != "" {
				t.Fatalf("invoke frame %#v", frame)
			}
			if deadline, e := time.Parse(time.RFC3339Nano, frame.Deadline); e != nil || time.Until(deadline) > time.Second {
				t.Fatalf("deadline %v %v", deadline, e)
			}
			reply := map[string]any{"type": "rpc.result", "invocation_id": frame.InvocationID, "ok": ok}
			if ok {
				reply["result"] = map[string]any{"value": 42}
			} else {
				reply["error"] = map[string]any{"code": "declined", "message": "Cannot calculate"}
			}
			_ = outsider.WriteJSON(reply)
			if denied := wsRead(t, outsider); denied.Type != "error" || denied.Code != "invalid_rpc_result" {
				t.Fatalf("owner guard %#v", denied)
			}
			_ = owner.WriteJSON(reply)
			res := <-done
			want := fmt.Sprintf(`{"invocation_id":%q,"ok":true,"result":{"value":42}}`, frame.InvocationID)
			if !ok {
				want = fmt.Sprintf(`{"invocation_id":%q,"ok":false,"error":{"code":"declined","message":"Cannot calculate"}}`, frame.InvocationID)
			}
			assertStatusAndJSON(t, res, 200, want)
			replay := signedEventRequest(t, h, c[1], "POST", path, []byte(`{"input":{}}`), "call")
			if replay.Code != 200 || replay.Body.String() != res.Body.String() || replay.Header().Get("Idempotent-Replayed") != "true" {
				t.Fatal("replay differs")
			}
			_ = owner.WriteJSON(reply)
			if duplicate := wsRead(t, owner); duplicate.Code != "invalid_rpc_result" {
				t.Fatalf("duplicate %#v", duplicate)
			}
			_ = owner.WriteJSON(map[string]string{"type": "ping"})
			if wsRead(t, owner).Type != "pong" {
				t.Fatal("replay redispatched or accepted result emitted error")
			}
		})
	}
}
func TestFunctionHTTPOfflineTimeoutAndLimits(t *testing.T) {
	h, srv, issuer, c := functionHTTPFixture(t)
	f := registerHTTPFunction(t, h, c[0])
	path := "/api/v1/functions/" + f.ID + "/invoke"
	for _, tc := range []struct {
		body, key string
		status    int
	}{{`{"input":{}}`, "", 400}, {`{"input":[]}`, "k", 400}, {`{"input":{},"app_id":"evil"}`, "k", 400}, {`{"input":{"x":"` + strings.Repeat("x", 65536) + `"}}`, "big", 400}, {strings.Repeat("x", 1<<20+1), "huge", 413}} {
		res := signedEventRequest(t, h, c[1], "POST", path, []byte(tc.body), tc.key)
		if res.Code != tc.status {
			t.Fatalf("validation %d %s", res.Code, res.Body.String())
		}
	}
	offline := signedEventRequest(t, h, c[1], "POST", path, []byte(`{"input":{}}`), "offline")
	if offline.Code != 503 || !strings.Contains(offline.Body.String(), `"code":"function_unavailable"`) {
		t.Fatalf("offline %d %s", offline.Code, offline.Body.String())
	}
	replay := signedEventRequest(t, h, c[1], "POST", path, []byte(`{"input":{}}`), "offline")
	if replay.Code != 503 || replay.Body.String() != offline.Body.String() || replay.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatal("offline replay")
	}
	owner := wsDial(t, srv, wsToken(t, issuer, c[0].AppID, "ws:connect"), "")
	wsRead(t, owner)
	_ = owner.WriteJSON(map[string]any{"type": "subscribe", "topics": []string{"functions"}})
	wsRead(t, owner)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- signedEventRequest(t, h, c[1], "POST", path, []byte(`{"input":{}}`), "timeout") }()
	frame := wsRead(t, owner)
	res := <-done
	if res.Code != 504 || !strings.Contains(res.Body.String(), `"code":"function_timeout"`) {
		t.Fatalf("timeout %d %s", res.Code, res.Body.String())
	}
	_ = owner.WriteJSON(map[string]any{"type": "rpc.result", "invocation_id": frame.InvocationID, "ok": true, "result": map[string]any{}})
	if wsRead(t, owner).Code != "invalid_rpc_result" {
		t.Fatal("late result accepted")
	}
}

func TestFunctionHTTPRejectsInvalidUTF8BeforeWebSocketDispatch(t *testing.T) {
	h, srv, issuer, creds := functionHTTPFixture(t)
	fn := registerHTTPFunction(t, h, creds[0])
	owner := wsDial(t, srv, wsToken(t, issuer, creds[0].AppID, "ws:connect"), "")
	wsRead(t, owner)
	if e := owner.WriteJSON(map[string]any{"type": "subscribe", "topics": []string{"functions"}}); e != nil {
		t.Fatal(e)
	}
	if wsRead(t, owner).Type != "subscribed" {
		t.Fatal("function subscription failed")
	}
	path := "/api/v1/functions/" + fn.ID + "/invoke"
	body := append([]byte(`{"input":{"text":"`), 0xff)
	body = append(body, []byte(`"}}`)...)
	rejected := signedEventRequest(t, h, creds[1], "POST", path, body, "utf8-retry")
	if rejected.Code != 400 || !strings.Contains(rejected.Body.String(), `"code":"invalid_request"`) {
		t.Errorf("invalid UTF-8 input must be rejected: status=%d body=%s", rejected.Code, rejected.Body.String())
	}
	// Enforce RFC 6455 text validity explicitly on the real Gorilla peer: its JSON
	// decoder alone would silently replace invalid bytes. A ping barrier proves
	// no invocation frame was enqueued and the same connection remains usable.
	if e := owner.WriteJSON(map[string]string{"type": "ping"}); e != nil {
		t.Fatal(e)
	}
	_ = owner.SetReadDeadline(time.Now().Add(time.Second))
	_, raw, e := owner.ReadMessage()
	if e != nil {
		t.Fatal(e)
	}
	if !utf8.Valid(raw) {
		t.Fatalf("invalid UTF-8 escaped into WebSocket text: %q", raw)
	}
	var frame realtime.ServerFrame
	if e = json.Unmarshal(raw, &frame); e != nil {
		t.Fatal(e)
	}
	if frame.Type != "pong" {
		t.Fatalf("rejected input dispatched to handler: %#v", frame)
	}
	// A rejected input must not reserve its idempotency key or create an invocation.
	completed := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		completed <- signedEventRequest(t, h, creds[1], "POST", path, []byte(`{"input":{"text":"€"}}`), "utf8-retry")
	}()
	frame = wsRead(t, owner)
	if frame.Type != "rpc.invoke" || string(frame.Input) != `{"text":"€"}` {
		t.Fatalf("valid Unicode changed: %#v", frame)
	}
	if e = owner.WriteJSON(map[string]any{"type": "rpc.result", "invocation_id": frame.InvocationID, "ok": true, "result": map[string]string{"text": "€"}}); e != nil {
		t.Fatal(e)
	}
	response := <-completed
	if response.Code != 200 || response.Header().Get("Idempotent-Replayed") != "" {
		t.Fatalf("rejected input consumed key: %d %s", response.Code, response.Body.String())
	}
}
