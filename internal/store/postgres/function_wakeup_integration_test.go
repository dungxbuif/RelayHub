//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/realtime"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/dungxbuif/RelayHub/internal/store"
	server "github.com/nats-io/nats-server/v2/server"
	gonats "github.com/nats-io/nats.go"
)

type observedInvocationReads struct {
	*Client
	reads chan struct{}
}

func (repository *observedInvocationReads) GetInvocation(ctx context.Context, id string) (domain.Invocation, error) {
	v, err := repository.Client.GetInvocation(ctx, id)
	repository.reads <- struct{}{}
	return v, err
}

type persistedResultTransport struct {
	*realtime.NATSBridge
	repository *Client
	t          *testing.T
	drop       bool
	hints      atomic.Int32
}

func (transport *persistedResultTransport) PublishInvocationResult(ctx context.Context, id string) error {
	v, err := transport.repository.GetInvocation(ctx, id)
	if err != nil || !v.Terminal() {
		transport.t.Errorf("hint before PostgreSQL commit: %#v %v", v, err)
	}
	transport.hints.Add(1)
	if transport.drop {
		return errors.New("simulated lost Core NATS result hint")
	}
	return transport.NATSBridge.PublishInvocationResult(ctx, id)
}

func TestPostgresCoreNATSResultsWakeConcurrentIdempotentWaiters(t *testing.T) {
	client := integrationPostgresClient(t)
	resetControlTables(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	now := time.Now().UTC()
	for _, id := range []string{"owner", "caller"} {
		if err := client.CreateApplication(ctx, domain.App{ID: id, Name: id, DeliveryMode: domain.DeliveryWebSocket, Enabled: true, CreatedAt: now, UpdatedAt: now}, store.AppCredential{AppID: id, APIKeyHash: "wakeup-" + id, HMACSecret: []byte("secret")}); err != nil {
			t.Fatal(err)
		}
	}
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("Core NATS not ready")
	}
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	newBridge := func(id string) (*realtime.NATSBridge, *realtime.Hub) {
		connection, err := gonats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(connection.Close)
		hub := realtime.NewHub()
		t.Cleanup(hub.Close)
		bridge, err := realtime.NewNATSBridge(ctx, connection, hub, id)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(bridge.Close)
		return bridge, hub
	}
	ownerBridge, ownerHub := newBridge("owner-gateway")
	ownerTransport := &persistedResultTransport{NATSBridge: ownerBridge, repository: client, t: t}
	ownerService := service.NewFunctionService(client, service.FunctionOptions{Notifier: ownerTransport})
	ownerHub.SetFunctions(ownerService)
	session := ownerHub.Register("owner")
	if err := ownerHub.Subscribe(session, []string{"functions"}); err != nil {
		t.Fatal(err)
	}
	function, err := ownerService.Register(ctx, "owner", service.RegisterFunction{Name: "calculate", TimeoutSeconds: 1})
	if err != nil {
		t.Fatal(err)
	}
	reads := &observedInvocationReads{Client: client, reads: make(chan struct{}, 1000)}
	first, _ := newBridge("caller-one")
	second, _ := newBridge("caller-two")
	callers := []*service.FunctionService{service.NewFunctionService(reads, service.FunctionOptions{Notifier: first}), service.NewFunctionService(reads, service.FunctionOptions{Notifier: second})}
	type response struct {
		result domain.RPCResult
		replay bool
		err    error
	}
	done := make(chan response, 8)
	invoke := func(caller *service.FunctionService, key string) {
		r, replay, err := caller.Invoke(ctx, "caller", function.ID, key, json.RawMessage(`{"n":9007199254740993}`))
		done <- response{r, replay, err}
	}
	go invoke(callers[0], "shared-key")
	var frame realtime.ServerFrame
	select {
	case raw := <-session.Frames():
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if frame.Function != "calculate" || string(frame.Input) != `{"n":9007199254740993}` {
		t.Fatalf("noncanonical PostgreSQL frame: %#v", frame)
	}
	for i := 1; i < 8; i++ {
		go invoke(callers[i%2], "shared-key")
	}
	for range 8 {
		select {
		case <-reads.reads:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	select {
	case <-reads.reads:
		t.Fatal("idle waiters polled PostgreSQL")
	case <-time.After(100 * time.Millisecond):
	}
	ok := true
	if err := ownerHub.HandleResult(session, realtime.ClientFrame{Type: "rpc.result", InvocationID: frame.InvocationID, OK: &ok, Result: json.RawMessage(`{"value":9007199254740993}`)}); err != nil {
		t.Fatal(err)
	}
	replays := 0
	for range 8 {
		select {
		case got := <-done:
			if got.err != nil || got.result.InvocationID != frame.InvocationID || string(got.result.Result) != `{"value":9007199254740993}` {
				t.Fatalf("waiter result=%#v", got)
			}
			if got.replay {
				replays++
			}
		case <-time.After(300 * time.Millisecond):
			t.Fatal("persisted result did not wake every Core NATS caller")
		}
	}
	if replays != 7 || ownerTransport.hints.Load() != 1 {
		t.Fatalf("replays=%d result hints=%d", replays, ownerTransport.hints.Load())
	}
	select {
	case raw := <-session.Frames():
		t.Fatalf("idempotent waiter redispatched: %s", raw)
	default:
	}

	// A lost hint delays observation until the persisted deadline; it cannot
	// turn an already committed success into a timeout or a second dispatch.
	ownerTransport.drop = true
	for len(reads.reads) > 0 {
		<-reads.reads
	}
	go invoke(callers[0], "lost-hint")
	select {
	case raw := <-session.Frames():
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	<-reads.reads
	started := time.Now()
	if err := ownerHub.HandleResult(session, realtime.ClientFrame{Type: "rpc.result", InvocationID: frame.InvocationID, OK: &ok, Result: json.RawMessage(`{"value":9007199254740993}`)}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-reads.reads:
		t.Fatal("lost hint triggered polling before persisted deadline")
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case got := <-done:
		if got.err != nil || !got.result.OK || time.Since(started) < 500*time.Millisecond {
			t.Fatalf("deadline recovery=%#v elapsed=%s", got, time.Since(started))
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
