//go:build integration

package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	server "github.com/nats-io/nats-server/v2/server"
	gonats "github.com/nats-io/nats.go"
)

type fencedInvocationBackend struct {
	mu         sync.Mutex
	invocation domain.Invocation
}

func (backend *fencedInvocationBackend) ClaimInvocation(_ context.Context, app, connection, id string) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.invocation.ID != id || backend.invocation.OwnerAppID != app || backend.invocation.State != domain.InvocationPending || !time.Now().Before(backend.invocation.ClaimBy) {
		return store.ErrInvalidResult
	}
	backend.invocation.State, backend.invocation.ConnectionID = domain.InvocationReserved, connection
	return nil
}
func (backend *fencedInvocationBackend) AcknowledgeInvocation(_ context.Context, app, connection, id string) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.invocation.ID != id || backend.invocation.OwnerAppID != app || backend.invocation.ConnectionID != connection || backend.invocation.State != domain.InvocationReserved {
		return store.ErrInvalidResult
	}
	backend.invocation.State = domain.InvocationClaimed
	return nil
}
func (backend *fencedInvocationBackend) ReleaseInvocation(_ context.Context, app, connection, id string) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.invocation.ID != id || backend.invocation.OwnerAppID != app || backend.invocation.ConnectionID != connection {
		return store.ErrInvalidResult
	}
	backend.invocation.State, backend.invocation.ConnectionID = domain.InvocationPending, ""
	return nil
}
func (backend *fencedInvocationBackend) CompleteResult(_ context.Context, app, connection string, result domain.RPCResult) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.invocation.ID != result.InvocationID || backend.invocation.OwnerAppID != app || backend.invocation.ConnectionID != connection || backend.invocation.State != domain.InvocationClaimed || !time.Now().Before(backend.invocation.Deadline) {
		return store.ErrInvalidResult
	}
	backend.invocation.State = domain.InvocationSuccess
	backend.invocation.Reply = &result
	return nil
}

func TestCoreNATSBridgeAcrossTwoGateways(t *testing.T) {
	url := startRealtimeNATSServer(t)
	invokeConn, firstConn, secondConn := connectRealtimeNATS(t, url), connectRealtimeNATS(t, url), connectRealtimeNATS(t, url)
	invokeHub, firstHub, secondHub := NewHub(), NewHub(), NewHub()
	backend := &fencedInvocationBackend{}
	firstHub.SetFunctions(backend)
	secondHub.SetFunctions(backend)
	invokeBridge, err := NewNATSBridge(context.Background(), invokeConn, invokeHub, "invoke-instance")
	if err != nil {
		t.Fatal(err)
	}
	firstBridge, err := NewNATSBridge(context.Background(), firstConn, firstHub, "first-instance")
	if err != nil {
		t.Fatal(err)
	}
	secondBridge, err := NewNATSBridge(context.Background(), secondConn, secondHub, "second-instance")
	if err != nil {
		t.Fatal(err)
	}
	defer invokeBridge.Close()
	defer firstBridge.Close()
	defer secondBridge.Close()
	first, second := firstHub.Register("owner"), secondHub.Register("owner")
	if err := firstHub.Subscribe(first, []string{"functions", "events"}); err != nil {
		t.Fatal(err)
	}
	if err := secondHub.Subscribe(second, []string{"functions", "events"}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	invocation := domain.Invocation{ID: "inv_nats", FunctionID: "fn_nats", OwnerAppID: "owner", CallerAppID: "caller", Name: "calculate", Input: json.RawMessage(`{"a":20,"b":22}`), CreatedAt: now, ClaimBy: now.Add(500 * time.Millisecond), Deadline: now.Add(time.Second), State: domain.InvocationPending}
	backend.invocation = invocation
	if err := invokeBridge.PublishInvocation(context.Background(), invocation); err != nil {
		t.Fatal(err)
	}
	var selected *Session
	select {
	case <-first.Frames():
		selected = first
	case <-second.Frames():
		selected = second
	case <-time.After(time.Second):
		t.Fatal("no function gateway selected")
	}
	other := first
	if selected == first {
		other = second
	}
	select {
	case frame := <-other.Frames():
		t.Fatalf("second owner received invocation: %s", frame)
	case <-time.After(50 * time.Millisecond):
	}
	if err := invokeBridge.PublishInvocation(context.Background(), invocation); !errors.Is(err, ErrNATSFunctionUnavailable) {
		t.Fatalf("accepted invocation redispatch=%v", err)
	}
	for _, session := range []*Session{first, second} {
		select {
		case frame := <-session.Frames():
			t.Fatalf("accepted invocation reached handler twice: %s", frame)
		default:
		}
	}
	ok := true
	if protocolErr := selected.hub.HandleResult(selected, ClientFrame{Type: "rpc.result", InvocationID: invocation.ID, OK: &ok, Result: json.RawMessage(`{"value":42}`)}); protocolErr != nil {
		t.Fatal(protocolErr)
	}
	backend.mu.Lock()
	if backend.invocation.State != domain.InvocationSuccess {
		t.Fatalf("terminal invocation=%#v", backend.invocation)
	}
	backend.mu.Unlock()
	event := domain.Event{ID: "evt_nats", TargetAppIDs: []string{"owner"}}
	if err := invokeBridge.PublishEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	for _, session := range []*Session{first, second} {
		frame := receive(t, session)
		if frame.Event == nil || frame.Event.ID != event.ID {
			t.Fatalf("observation=%#v", frame)
		}
	}
}

func TestCoreNATSFunctionNoResponderIsBounded(t *testing.T) {
	url := startRealtimeNATSServer(t)
	bridge, err := NewNATSBridge(context.Background(), connectRealtimeNATS(t, url), NewHub(), "caller-instance")
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.Close()
	now := time.Now().UTC()
	invocation := domain.Invocation{ID: "inv_offline", FunctionID: "fn_offline", OwnerAppID: "offline", Name: "wait", Input: json.RawMessage(`{}`), CreatedAt: now, ClaimBy: now.Add(60 * time.Millisecond), Deadline: now.Add(time.Second), State: domain.InvocationPending}
	started := time.Now()
	if err := bridge.PublishInvocation(context.Background(), invocation); !errors.Is(err, ErrNATSFunctionUnavailable) {
		t.Fatalf("PublishInvocation()=%v", err)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("no responder took %s", elapsed)
	}
}

func startRealtimeNATSServer(t *testing.T) string {
	t.Helper()
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server not ready")
	}
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	return srv.ClientURL()
}

func connectRealtimeNATS(t *testing.T, url string) *gonats.Conn {
	t.Helper()
	connection, err := gonats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(connection.Close)
	return connection
}
