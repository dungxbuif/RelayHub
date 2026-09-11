//go:build integration

package redisstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dungxbuif/RelayHub/internal/auth"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/realtime"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func seedFunctionOwner(t *testing.T, c *Client, owner string) {
	t.Helper()
	e := c.CreateApplication(context.Background(), domain.App{ID: owner, Name: owner, Enabled: true, DeliveryMode: domain.DeliveryQueue}, store.AppCredential{AppID: owner, APIKeyHash: owner, HMACSecret: []byte("secret")})
	if e != nil {
		t.Fatal(e)
	}
}
func TestFunctionsRedisTwoInstanceClaimAndFastConcurrentReplay(t *testing.T) {
	c := integrationRedisClient(t)
	seedFunctionOwner(t, c, "owner")
	ctx := context.Background()
	h1, h2 := realtime.NewHub(), realtime.NewHub()
	defer h1.Close()
	defer h2.Close()
	b1, e := NewBridge(ctx, c, h1)
	if e != nil {
		t.Fatal(e)
	}
	defer b1.Close()
	b2, e := NewBridge(ctx, c, h2)
	if e != nil {
		t.Fatal(e)
	}
	defer b2.Close()
	s1 := service.NewFunctionService(c, service.FunctionOptions{Notifier: b1})
	s2 := service.NewFunctionService(c, service.FunctionOptions{Notifier: b2})
	h1.SetFunctions(s1)
	h2.SetFunctions(s2)
	a, b := h1.Register("owner"), h2.Register("owner")
	_ = h1.Subscribe(a, []string{"functions"})
	_ = h2.Subscribe(b, []string{"functions"})
	f, e := s1.Register(ctx, "owner", service.RegisterFunction{Name: "calculate", TimeoutSeconds: 2})
	if e != nil {
		t.Fatal(e)
	}
	var dispatched atomic.Int32
	var handlers sync.WaitGroup
	handlerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for _, peer := range []struct {
		h *realtime.Hub
		s *realtime.Session
	}{{h1, a}, {h2, b}} {
		handlers.Add(1)
		go func(h *realtime.Hub, s *realtime.Session) {
			defer handlers.Done()
			for {
				select {
				case <-handlerCtx.Done():
					return
				case raw := <-s.Frames():
					var frame realtime.ServerFrame
					if e := json.Unmarshal(raw, &frame); e != nil {
						t.Error(e)
						return
					}
					dispatched.Add(1)
					if frame.Function != "calculate" || string(frame.Input) != `{"n":9007199254740993}` {
						t.Errorf("frame %s", raw)
					}
					ok := true
					if e := h.HandleResult(s, realtime.ClientFrame{Type: "rpc.result", InvocationID: frame.InvocationID, OK: &ok, Result: json.RawMessage(`{"n":9007199254740993}`)}); e != nil {
						t.Error(e)
					}
				}
			}
		}(peer.h, peer.s)
	}
	results := make(chan domain.RPCResult, 30)
	var wg sync.WaitGroup
	for i := range 30 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := s1
			if i%2 == 0 {
				s = s2
			}
			r, _, e := s.Invoke(ctx, "caller", f.ID, "same", json.RawMessage(`{"n":9007199254740993}`))
			if e != nil {
				t.Error(e)
			}
			results <- r
		}(i)
	}
	wg.Wait()
	close(results)
	cancel()
	handlers.Wait()
	var id string
	for r := range results {
		if !r.OK || string(r.Result) != `{"n":9007199254740993}` || id != "" && id != r.InvocationID {
			t.Fatalf("divergent result %#v", r)
		}
		id = r.InvocationID
	}
	if dispatched.Load() != 1 {
		t.Fatalf("dispatch count %d", dispatched.Load())
	}
	stored, e := c.GetInvocation(ctx, id)
	if e != nil || stored.State != domain.InvocationSuccess {
		t.Fatalf("state %#v %v", stored, e)
	}
	if e = c.CompleteInvocation(ctx, "owner", stored.ConnectionID, *stored.Reply); !errors.Is(e, store.ErrInvalidResult) {
		t.Fatalf("duplicate %v", e)
	}
	keys, e := c.client.Keys(ctx, c.prefix+":*").Result()
	if e != nil {
		t.Fatal(e)
	}
	ephemeral := 0
	for _, key := range keys {
		if strings.Contains(key, ":invocation:") || strings.Contains(key, ":function-idem:") {
			ephemeral++
			ttl, e := c.client.PTTL(ctx, key).Result()
			if e != nil || ttl <= 23*time.Hour || ttl > 24*time.Hour {
				t.Fatalf("unbounded key %s ttl %s %v", key, ttl, e)
			}
		}
	}
	if ephemeral != 2 {
		t.Fatalf("invocation/key TTLs %d", ephemeral)
	}
	if e = c.DeleteFunction(ctx, "owner", f.ID); e != nil {
		t.Fatal(e)
	}
	if r, replay, e := s2.Invoke(ctx, "caller", f.ID, "same", json.RawMessage(`{}`)); e != nil || !replay || r.InvocationID != id {
		t.Fatalf("deleted replay %#v %v %v", r, replay, e)
	}
}
func TestFunctionsRedisRegistrationPrefixAndExpiry(t *testing.T) {
	base := integrationRedisClient(t)
	ctx := context.Background()
	url := "redis://" + base.client.Options().Addr + "/0"
	one, e := NewClientWithPrefix(url, "functions_one")
	if e != nil {
		t.Fatal(e)
	}
	defer one.Close()
	two, e := NewClientWithPrefix(url, "functions_two")
	if e != nil {
		t.Fatal(e)
	}
	defer two.Close()
	for _, c := range []*Client{one, two} {
		seedFunctionOwner(t, c, "owner")
		seedFunctionOwner(t, c, "other")
	}
	now := time.Now().UTC()
	f := domain.Function{ID: "fn_same", AppID: "owner", Name: "calculate", TimeoutSeconds: 1, Enabled: true, CreatedAt: now, UpdatedAt: now}
	for _, c := range []*Client{one, two} {
		if e = c.CreateFunction(ctx, f); e != nil {
			t.Fatal(e)
		}
	}
	if e = one.CreateFunction(ctx, domain.Function{ID: "fn_dup", AppID: "owner", Name: "calculate"}); !errors.Is(e, store.ErrConflict) {
		t.Fatalf("name conflict %v", e)
	}
	if e = one.DeleteFunction(ctx, "other", f.ID); !errors.Is(e, store.ErrNotFound) {
		t.Fatalf("cross-owner delete %v", e)
	}
	if list, e := one.ListFunctions(ctx, "other"); e != nil || len(list) != 0 {
		t.Fatal("cross-owner list")
	}
	if e = one.DeleteFunction(ctx, "owner", f.ID); e != nil {
		t.Fatal(e)
	}
	if _, e = two.GetFunction(ctx, f.ID); e != nil {
		t.Fatal("prefix collision")
	}
	if e = one.CreateFunction(ctx, f); e != nil {
		t.Fatal("name index not removed", e)
	}
	v := domain.Invocation{ID: "inv_same", OwnerAppID: "owner", CallerAppID: "caller", FunctionID: f.ID, Name: f.Name, Input: json.RawMessage(`{}`), State: domain.InvocationPending, CreatedAt: now, ClaimBy: now.Add(time.Second), Deadline: now.Add(time.Second)}
	for _, c := range []*Client{one, two} {
		if _, replay, e := c.CreateInvocation(ctx, v, "key"); e != nil || replay {
			t.Fatalf("create %v %v", replay, e)
		}
	}
	if e = one.ClaimInvocation(ctx, "other", "conn", v.ID); !errors.Is(e, store.ErrInvalidResult) {
		t.Fatalf("cross-owner claim %v", e)
	}
	if e = one.ClaimInvocation(ctx, "owner", "conn", v.ID); e != nil {
		t.Fatal(e)
	}
	reply := domain.RPCResult{InvocationID: v.ID, OK: true, Result: json.RawMessage(`{}`)}
	if e = one.CompleteInvocation(ctx, "owner", "conn", reply); !errors.Is(e, store.ErrInvalidResult) {
		t.Fatalf("result before ack %v", e)
	}
	if e = one.AcknowledgeInvocation(ctx, "owner", "conn", v.ID); e != nil {
		t.Fatal(e)
	}
	if e = one.CompleteInvocation(ctx, "owner", "wrong", reply); !errors.Is(e, store.ErrInvalidResult) {
		t.Fatalf("wrong connection %v", e)
	}
	if e = one.CompleteInvocation(ctx, "owner", "conn", reply); e != nil {
		t.Fatal(e)
	}
	if v, e := two.GetInvocation(ctx, v.ID); e != nil || v.State != domain.InvocationPending {
		t.Fatalf("invocation prefix leak %#v %v", v, e)
	}
	keys, e := one.client.Keys(ctx, one.prefix+":*").Result()
	if e != nil {
		t.Fatal(e)
	}
	for _, key := range keys {
		if strings.Contains(key, ":invocation:") || strings.Contains(key, ":function-idem:") {
			if e = one.client.PExpire(ctx, key, 5*time.Millisecond).Err(); e != nil {
				t.Fatal(e)
			}
		}
	}
	time.Sleep(10 * time.Millisecond)
	if _, e = one.FindInvocation(ctx, "caller", "key"); !errors.Is(e, store.ErrNotFound) {
		t.Fatalf("expired key %v", e)
	}
	v.ID = "inv_new"
	v.CreatedAt = time.Now()
	v.ClaimBy = v.CreatedAt.Add(50 * time.Millisecond)
	v.Deadline = v.CreatedAt.Add(time.Second)
	if _, replay, e := one.CreateInvocation(ctx, v, "key"); e != nil || replay {
		t.Fatalf("expired reuse %v %v", replay, e)
	}
	time.Sleep(60 * time.Millisecond)
	if got, e := one.GetInvocation(ctx, v.ID); e != nil || got.State != domain.InvocationUnavailable {
		t.Fatalf("no claim %#v %v", got, e)
	}
	if e = one.ClaimInvocation(ctx, "owner", "conn", v.ID); !errors.Is(e, store.ErrInvalidResult) {
		t.Fatalf("late claim %v", e)
	}
}
func TestFunctionsRedisAcknowledgedOwnerDisconnectNeverRedispatches(t *testing.T) {
	c := integrationRedisClient(t)
	seedFunctionOwner(t, c, "owner")
	ctx := context.Background()
	h := realtime.NewHub()
	defer h.Close()
	b, err := NewBridge(ctx, c, h)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	svc := service.NewFunctionService(c, service.FunctionOptions{Notifier: b})
	h.SetFunctions(svc)
	f, err := svc.Register(ctx, "owner", service.RegisterFunction{Name: "calculate", TimeoutSeconds: 1})
	if err != nil {
		t.Fatal(err)
	}
	one, two := h.Register("owner"), h.Register("owner")
	_ = h.Subscribe(one, []string{"functions"})
	_ = h.Subscribe(two, []string{"functions"})
	done := make(chan error, 1)
	go func() {
		_, _, err := svc.Invoke(ctx, "caller", f.ID, "disconnect-after-ack", json.RawMessage(`{}`))
		done <- err
	}()
	var raw []byte
	var selected, other *realtime.Session
	select {
	case raw = <-one.Frames():
		selected, other = one, two
	case raw = <-two.Frames():
		selected, other = two, one
	case <-time.After(2 * time.Second):
		t.Fatal("invocation not dispatched")
	}
	var frame realtime.ServerFrame
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatal(err)
	}
	v, err := c.GetInvocation(ctx, frame.InvocationID)
	if err != nil || v.State != domain.InvocationClaimed || v.ConnectionID != selected.ID() {
		t.Fatalf("delivery not acknowledged: %#v %v", v, err)
	}
	selected.Close()
	// Even a duplicate Pub/Sub hint cannot send a claimed invocation elsewhere.
	if err := b.PublishInvocation(ctx, v); err != nil {
		t.Fatal(err)
	}
	select {
	case extra := <-other.Frames():
		t.Fatalf("redispatched acknowledged invocation: %s", extra)
	case err := <-done:
		if !errors.Is(err, service.ErrFunctionTimeout) {
			t.Fatalf("expected timeout: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout missing")
	}
	if _, replay, err := svc.Invoke(ctx, "caller", f.ID, "disconnect-after-ack", json.RawMessage(`{}`)); !replay || !errors.Is(err, service.ErrFunctionTimeout) {
		t.Fatalf("terminal replay: %v %v", replay, err)
	}
	select {
	case extra := <-other.Frames():
		t.Fatalf("late redispatch: %s", extra)
	default:
	}
}

func TestFunctionsRedisTimeoutAndOwnerDisconnectBeforeClaim(t *testing.T) {
	c := integrationRedisClient(t)
	ctx := context.Background()
	seedFunctionOwner(t, c, "owner")
	hub := realtime.NewHub()
	defer hub.Close()
	bridge, e := NewBridge(ctx, c, hub)
	if e != nil {
		t.Fatal(e)
	}
	defer bridge.Close()
	svc := service.NewFunctionService(c, service.FunctionOptions{Notifier: bridge})
	hub.SetFunctions(svc)
	f, e := svc.Register(ctx, "owner", service.RegisterFunction{Name: "calculate", TimeoutSeconds: 1})
	if e != nil {
		t.Fatal(e)
	}
	gone := hub.Register("owner")
	_ = hub.Subscribe(gone, []string{"functions"})
	gone.Close()
	if _, _, e = svc.Invoke(ctx, "caller", f.ID, "offline", json.RawMessage(`{}`)); !errors.Is(e, service.ErrFunctionUnavailable) {
		t.Fatalf("disconnected %v", e)
	}
	session := hub.Register("owner")
	_ = hub.Subscribe(session, []string{"functions"})
	done := make(chan error, 1)
	start := time.Now()
	go func() { _, _, e := svc.Invoke(ctx, "caller", f.ID, "timeout", json.RawMessage(`{}`)); done <- e }()
	frame := bridgeRead(t, session)
	if e = <-done; !errors.Is(e, service.ErrFunctionTimeout) || time.Since(start) > 1500*time.Millisecond {
		t.Fatalf("timeout %v", e)
	}
	ok := true
	if e := hub.HandleResult(session, realtime.ClientFrame{Type: "rpc.result", InvocationID: frame.InvocationID, OK: &ok, Result: json.RawMessage(`{}`)}); e == nil || e.Code != "invalid_rpc_result" {
		t.Fatalf("late result %v", e)
	}
	if _, replay, e := svc.Invoke(ctx, "caller", f.ID, "timeout", json.RawMessage(`{}`)); !errors.Is(e, service.ErrFunctionTimeout) || !replay {
		t.Fatalf("timeout replay %v %v", replay, e)
	}
	// Owner disconnect after an acknowledged delivery is never redispatched.
	if e = c.ClaimInvocation(ctx, "owner", "conn", "inv_unknown"); !errors.Is(e, store.ErrInvalidResult) {
		t.Fatalf("unknown %v", e)
	}
}
func TestFunctionsRedisWaiterAlreadyTerminalAndCancellation(t *testing.T) {
	c := integrationRedisClient(t)
	ctx := context.Background()
	seedFunctionOwner(t, c, "owner")
	s := service.NewFunctionService(c, service.FunctionOptions{})
	f, e := s.Register(ctx, "owner", service.RegisterFunction{Name: "calculate", TimeoutSeconds: 1})
	if e != nil {
		t.Fatal(e)
	}
	now := time.Now().UTC()
	v := domain.Invocation{ID: "inv_watch", OwnerAppID: "owner", CallerAppID: "caller", FunctionID: f.ID, Name: f.Name, Input: json.RawMessage(`{}`), CreatedAt: now, Deadline: now.Add(time.Second), ClaimBy: now.Add(250 * time.Millisecond), State: domain.InvocationPending}
	if _, _, e = c.CreateInvocation(ctx, v, "key"); e != nil {
		t.Fatal(e)
	}
	watch, e := c.WatchInvocation(ctx, v.ID)
	if e != nil {
		t.Fatal(e)
	}
	defer watch.Close()
	if e = c.ClaimInvocation(ctx, "owner", "conn", v.ID); e != nil {
		t.Fatal(e)
	}
	if e = c.AcknowledgeInvocation(ctx, "owner", "conn", v.ID); e != nil {
		t.Fatal(e)
	}
	if e = c.CompleteInvocation(ctx, "owner", "conn", domain.RPCResult{InvocationID: v.ID, OK: false, Error: json.RawMessage(`{"code":"declined","message":"Try later"}`)}); e != nil {
		t.Fatal(e)
	}
	select {
	case <-watch.Updates():
	case <-time.After(time.Second):
		t.Fatal("missing waiter wakeup")
	}
	if r, replay, e := s.Invoke(ctx, "caller", f.ID, "key", json.RawMessage(`{}`)); e != nil || !replay || r.OK || string(r.Error) != `{"code":"declined","message":"Try later"}` {
		t.Fatalf("terminal %#v %v %v", r, replay, e)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	start := time.Now()
	if _, e = c.WatchInvocation(cancelled, "inv_cancel"); e == nil {
		t.Fatal("cancelled watcher created")
	}
	if time.Since(start) > time.Second {
		t.Fatal("cancel not bounded")
	}
}

func TestFunctionsBinaryTwoAPIs(t *testing.T) {
	c := integrationRedisClient(t)
	binary := t.TempDir() + "/relayhub"
	if out, e := exec.Command("go", "build", "-o", binary, "../../../cmd/relayhub").CombinedOutput(); e != nil {
		t.Fatalf("build %s %v", out, e)
	}
	apps := service.NewAppService(c, service.AppOptions{})
	creds := []service.AppCredentials{}
	for _, name := range []string{"owner", "caller"} {
		_, cred, e := apps.Create(context.Background(), service.CreateApp{Name: name, DeliveryMode: domain.DeliveryQueue})
		if e != nil {
			t.Fatal(e)
		}
		creds = append(creds, cred)
	}
	addresses := []string{}
	for range 2 {
		listener, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		address := listener.Addr().String()
		_ = listener.Close()
		command := exec.Command(binary, "api")
		command.Env = append(os.Environ(), "RELAYHUB_ADMIN_TOKEN=smoke-admin", "RELAYHUB_SIGNING_SECRET=smoke-signing", "RELAYHUB_REDIS_URL=redis://"+c.client.Options().Addr+"/0", "RELAYHUB_HTTP_ADDR="+address, "RELAYHUB_REDIS_KEY_PREFIX=relayhub")
		if e = command.Start(); e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() {
			_ = command.Process.Signal(os.Interrupt)
			done := make(chan error, 1)
			go func() { done <- command.Wait() }()
			select {
			case e := <-done:
				if e != nil {
					t.Error(e)
				}
			case <-time.After(3 * time.Second):
				_ = command.Process.Kill()
				<-done
				t.Error("shutdown blocked")
			}
		})
		addresses = append(addresses, address)
		ready := false
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			resp, e := http.Get("http://" + address + "/readyz")
			if e == nil {
				resp.Body.Close()
				if resp.StatusCode == 200 {
					ready = true
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !ready {
			t.Fatal("binary did not become ready")
		}
	}
	signed := func(address string, c service.AppCredentials, path string, body []byte, key string) (int, []byte) {
		stamp := fmt.Sprint(time.Now().Unix())
		req, _ := http.NewRequest("POST", "http://"+address+path, bytes.NewReader(body))
		req.Header.Set("X-RelayHub-Api-Key", c.APIKey)
		req.Header.Set("X-RelayHub-Timestamp", stamp)
		req.Header.Set("X-RelayHub-Signature", auth.Sign([]byte(c.HMACSecret), stamp, "POST", path, body))
		req.Header.Set("Idempotency-Key", key)
		resp, e := (&http.Client{Timeout: 3 * time.Second}).Do(req)
		if e != nil {
			t.Error(e)
			return 0, nil
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, raw
	}
	status, raw := signed(addresses[0], creds[0], "/api/v1/functions", []byte(`{"name":"calculate","timeout_seconds":1}`), "")
	if status != 201 {
		t.Fatalf("binary register %d %s", status, raw)
	}
	var f domain.Function
	if e := json.Unmarshal(raw, &f); e != nil {
		t.Fatal(e)
	}
	issuer := auth.NewTokenIssuer([]byte("smoke-signing"), time.Now)
	token, e := issuer.Issue(creds[0].AppID, []string{"ws:connect"}, time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	conn, _, e := websocket.DefaultDialer.Dial("ws://"+addresses[1]+"/ws?token="+token, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var frame realtime.ServerFrame
	if e = conn.ReadJSON(&frame); e != nil {
		t.Fatal(e)
	}
	if e = conn.WriteJSON(map[string]any{"type": "subscribe", "topics": []string{"functions"}}); e != nil {
		t.Fatal(e)
	}
	if e = conn.ReadJSON(&frame); e != nil || frame.Type != "subscribed" {
		t.Fatal("subscription failed", e)
	}
	response := make(chan []byte, 1)
	go func() {
		status, raw := signed(addresses[0], creds[1], "/api/v1/functions/"+f.ID+"/invoke", []byte(`{"input":{"a":20,"b":22}}`), "runtime")
		if status != 200 {
			t.Errorf("invoke %d %s", status, raw)
		}
		response <- raw
	}()
	if e = conn.ReadJSON(&frame); e != nil || frame.Type != "rpc.invoke" {
		t.Fatal("no runtime dispatch", e)
	}
	if e = conn.WriteJSON(map[string]any{"type": "rpc.result", "invocation_id": frame.InvocationID, "ok": true, "result": map[string]int{"value": 42}}); e != nil {
		t.Fatal(e)
	}
	first := <-response
	status, raw = signed(addresses[1], creds[1], "/api/v1/functions/"+f.ID+"/invoke", []byte(`{"input":{}}`), "runtime")
	if status != 200 || !bytes.Equal(first, raw) || !bytes.Contains(raw, []byte(`"value":42`)) {
		t.Fatalf("runtime replay %d %s", status, raw)
	}
	resp, e := http.Get("http://" + addresses[0] + "/metrics")
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	metrics, _ := io.ReadAll(resp.Body)
	if !bytes.Contains(metrics, []byte(`relayhub_function_outcomes_total{outcome="success"} 1`)) {
		t.Fatal("runtime function metrics missing")
	}
}

type closeBeforeFunctionClaim struct {
	store.FunctionStore
	session *realtime.Session
	once    sync.Once
}

func (c *closeBeforeFunctionClaim) ClaimInvocation(ctx context.Context, owner, conn, id string) error {
	e := c.FunctionStore.ClaimInvocation(ctx, owner, conn, id)
	if e == nil {
		c.once.Do(c.session.Close)
	}
	return e
}
func TestFunctionsRedisDisconnectDuringClaimReleasesToOtherInstance(t *testing.T) {
	c := integrationRedisClient(t)
	ctx := context.Background()
	seedFunctionOwner(t, c, "owner")
	h1, h2 := realtime.NewHub(), realtime.NewHub()
	defer h1.Close()
	defer h2.Close()
	a, b := h1.Register("owner"), h2.Register("owner")
	_ = h1.Subscribe(a, []string{"functions"})
	_ = h2.Subscribe(b, []string{"functions"})
	bridge, e := NewBridge(ctx, c, h2)
	if e != nil {
		t.Fatal(e)
	}
	defer bridge.Close()
	s1 := service.NewFunctionService(&closeBeforeFunctionClaim{FunctionStore: c, session: a}, service.FunctionOptions{})
	s2 := service.NewFunctionService(c, service.FunctionOptions{})
	h1.SetFunctions(s1)
	h2.SetFunctions(s2)
	f, e := s2.Register(ctx, "owner", service.RegisterFunction{Name: "calculate", TimeoutSeconds: 1})
	if e != nil {
		t.Fatal(e)
	}
	now := time.Now().UTC()
	v := domain.Invocation{ID: "inv_release", FunctionID: f.ID, OwnerAppID: "owner", CallerAppID: "caller", Name: f.Name, Input: json.RawMessage(`{}`), State: domain.InvocationPending, CreatedAt: now, ClaimBy: now.Add(250 * time.Millisecond), Deadline: now.Add(time.Second)}
	if _, _, e = c.CreateInvocation(ctx, v, "release"); e != nil {
		t.Fatal(e)
	}
	if e = h1.InvokeFunction(ctx, "owner", realtime.InvocationFrame(v)); e == nil {
		t.Fatal("closed owner delivered")
	}
	frame := bridgeRead(t, b)
	if frame.InvocationID != v.ID {
		t.Fatal("failed claim did not move to eligible instance")
	}
	select {
	case raw := <-a.Frames():
		t.Fatalf("closed owner received %s", raw)
	default:
	}
	ok := true
	if e := h2.HandleResult(b, realtime.ClientFrame{Type: "rpc.result", InvocationID: v.ID, OK: &ok, Result: json.RawMessage(`{}`)}); e != nil {
		t.Fatal(e)
	}
	stored, e := c.GetInvocation(ctx, v.ID)
	if e != nil || stored.ConnectionID != b.ID() || stored.State != domain.InvocationSuccess {
		t.Fatalf("fallback %#v %v", stored, e)
	}
	if e = c.ReleaseInvocation(ctx, "owner", a.ID(), v.ID); !errors.Is(e, store.ErrInvalidResult) {
		t.Fatal("stale owner reopened terminal call")
	}
}
func TestFunctionsRedisPendingCrashExpiryAndDisabledOwner(t *testing.T) {
	c := integrationRedisClient(t)
	ctx := context.Background()
	seedFunctionOwner(t, c, "owner")
	s := service.NewFunctionService(c, service.FunctionOptions{})
	f, e := s.Register(ctx, "owner", service.RegisterFunction{Name: "calculate", TimeoutSeconds: 1})
	if e != nil {
		t.Fatal(e)
	}
	now := time.Now().UTC()
	v := domain.Invocation{ID: "inv_crash", FunctionID: f.ID, OwnerAppID: "owner", CallerAppID: "caller", Name: f.Name, Input: json.RawMessage(`{}`), State: domain.InvocationPending, CreatedAt: now, ClaimBy: now.Add(30 * time.Millisecond), Deadline: now.Add(time.Second)}
	if _, _, e = c.CreateInvocation(ctx, v, "crash"); e != nil {
		t.Fatal(e)
	}
	time.Sleep(40 * time.Millisecond)
	if _, replay, e := s.Invoke(ctx, "caller", f.ID, "crash", json.RawMessage(`{}`)); !replay || !errors.Is(e, service.ErrFunctionUnavailable) {
		t.Fatalf("crashed publisher left unbounded pending state %v %v", replay, e)
	}
	_, e = c.DisableApplication(ctx, "owner", time.Now())
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = s.Invoke(ctx, "caller", f.ID, "disabled", json.RawMessage(`{}`)); !errors.Is(e, service.ErrNotFound) {
		t.Fatalf("disabled target accepted %v", e)
	}
}

// slowFunctionSubscriptionClient leaves the first normal command connection
// untouched, then stalls responses during the dedicated Pub/Sub connection's
// real Redis HELLO/init exchange. net.Pipe honors the driver's socket deadlines.
func slowFunctionSubscriptionClient(t *testing.T, base *Client) (*Client, <-chan struct{}) {
	t.Helper()
	options := *base.client.Options()
	options.MinIdleConns = 0
	options.MaxIdleConns = 0
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var connections atomic.Int32
	var workers sync.WaitGroup
	var peersMu sync.Mutex
	var peers []net.Conn
	options.Dialer = func(ctx context.Context, network, address string) (net.Conn, error) {
		upstream, e := (&net.Dialer{}).DialContext(ctx, network, address)
		if e != nil {
			return nil, e
		}
		if connections.Add(1) == 1 {
			return upstream, nil
		}
		client, proxy := net.Pipe()
		peersMu.Lock()
		peers = append(peers, client, proxy, upstream)
		peersMu.Unlock()
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer upstream.Close()
			defer proxy.Close()
			first := make([]byte, 4096)
			n, e := proxy.Read(first)
			if e != nil {
				return
			}
			if _, e = upstream.Write(first[:n]); e != nil {
				return
			}
			select {
			case started <- struct{}{}:
			default:
			}
			copied := make(chan struct{})
			go func() { _, _ = io.Copy(upstream, proxy); _ = upstream.Close(); _ = proxy.Close(); close(copied) }()
			// The old Subscribe(ctx) path waits through this 1.5s stall before it even
			// creates its 250ms Receive context. The fixed path expires during init.
			timer := time.NewTimer(1500 * time.Millisecond)
			select {
			case <-release:
			case <-timer.C:
			}
			timer.Stop()
			_, _ = io.Copy(proxy, upstream)
			_ = proxy.Close()
			_ = upstream.Close()
			<-copied
		}()
		return client, nil
	}
	client := &Client{prefix: base.prefix, client: redis.NewClient(&options), jobRetention: base.jobRetention}
	t.Cleanup(func() {
		close(release)
		_ = client.Close()
		peersMu.Lock()
		for _, peer := range peers {
			_ = peer.Close()
		}
		peersMu.Unlock()
		workers.Wait()
	})
	if e := client.Ping(context.Background()); e != nil {
		t.Fatal(e)
	}
	return client, started
}
func TestFunctionsRedisSubscriptionSetupHonorsInvocationBudget(t *testing.T) {
	base := integrationRedisClient(t)
	seedFunctionOwner(t, base, "owner")
	registered := service.NewFunctionService(base, service.FunctionOptions{})
	fn, e := registered.Register(context.Background(), "owner", service.RegisterFunction{Name: "calculate", TimeoutSeconds: 1})
	if e != nil {
		t.Fatal(e)
	}
	for _, scenario := range []string{"claim_window", "caller_cancel", "caller_deadline"} {
		t.Run(scenario, func(t *testing.T) {
			client, started := slowFunctionSubscriptionClient(t, base)
			svc := service.NewFunctionService(client, service.FunctionOptions{})
			ctx, cancel := context.WithCancel(context.Background())
			if scenario == "caller_deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
			}
			defer cancel()
			done := make(chan error, 1)
			begin := time.Now()
			go func() { _, _, err := svc.Invoke(ctx, "caller", fn.ID, scenario, json.RawMessage(`{}`)); done <- err }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("subscription never entered its real Redis connection-init stall")
			}
			if scenario == "caller_cancel" {
				cancel()
			}
			select {
			case err := <-done:
				elapsed := time.Since(begin)
				if err == nil {
					t.Fatal("stalled subscription unexpectedly succeeded")
				}
				if elapsed > 500*time.Millisecond {
					t.Errorf("subscription setup exceeded 250ms claim budget with scheduling margin: %s (%v)", elapsed, err)
				}
				invocation, e := base.FindInvocation(context.Background(), "caller", scenario)
				if e != nil {
					t.Fatal(e)
				}
				if !begin.Add(elapsed).Before(invocation.Deadline) {
					t.Errorf("subscription setup exceeded persisted 1-second function deadline: %s", elapsed)
				}
				if scenario == "caller_cancel" && !errors.Is(err, context.Canceled) {
					t.Errorf("caller cancellation lost: %v", err)
				}
				if scenario == "caller_deadline" && !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("caller deadline lost: %v", err)
				}
				t.Logf("%s returned in %s with %v", scenario, elapsed, err)
			case <-time.After(4 * time.Second):
				t.Fatal("subscription initialization did not return")
			}
		})
	}
}
