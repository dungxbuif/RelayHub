//go:build integration

package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsbroker "github.com/dungxbuif/RelayHub/internal/broker/nats"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	server "github.com/nats-io/nats-server/v2/server"
	gonats "github.com/nats-io/nats.go"
)

type fencedInvocationBackend struct {
	mu         sync.Mutex
	invocation domain.Invocation
}

func (backend *fencedInvocationBackend) GetInvocation(_ context.Context, id string) (domain.Invocation, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.invocation.ID != id {
		return domain.Invocation{}, store.ErrNotFound
	}
	return backend.invocation, nil
}

func TestCoreNATSRejectsForgedInvocationFramesBeforeClaim(t *testing.T) {
	url := startRealtimeNATSServer(t)
	for _, field := range []string{"function_id", "name", "input", "deadline"} {
		t.Run(field, func(t *testing.T) {
			now := time.Now().UTC()
			invocation := domain.Invocation{ID: "inv_canonical", FunctionID: "fn_canonical", OwnerAppID: "owner", Name: "calculate", Input: json.RawMessage(`{"n":9007199254740993}`), CreatedAt: now, ClaimBy: now.Add(time.Second), Deadline: now.Add(2 * time.Second), State: domain.InvocationPending}
			backend := &fencedInvocationBackend{invocation: invocation}
			hub := NewHub()
			defer hub.Close()
			hub.SetFunctions(backend)
			bridge, err := NewNATSBridge(context.Background(), connectRealtimeNATS(t, url), hub, "canonical-instance")
			if err != nil {
				t.Fatal(err)
			}
			defer bridge.Close()
			session := hub.Register("owner")
			if err := hub.Subscribe(session, []string{"functions"}); err != nil {
				t.Fatal(err)
			}
			request := natsInvocationRequest{RequestID: "forged", OwnerAppID: "owner", FunctionID: "fn_canonical", Frame: InvocationFrame(invocation)}
			switch field {
			case "function_id":
				request.FunctionID = "fn_other"
			case "name":
				request.Frame.Function = "erase_all"
			case "input":
				request.Frame.Input = json.RawMessage(`{"n":0}`)
			case "deadline":
				request.Frame.Deadline = now.Add(time.Hour).Format(time.RFC3339Nano)
			}
			subject, _ := natsbroker.FunctionSubject(request.OwnerAppID, request.FunctionID)
			reply, _ := natsbroker.ReplySubject("canonical-caller")
			raw, _ := json.Marshal(request)
			bridge.handleInvocation(&gonats.Msg{Subject: subject, Reply: reply, Data: raw})
			select {
			case frame := <-session.Frames():
				t.Errorf("forged %s reached handler: %s", field, frame)
			default:
			}
			stored, _ := backend.GetInvocation(context.Background(), invocation.ID)
			if stored.State != domain.InvocationPending {
				t.Fatalf("forged request changed reservation to %s", stored.State)
			}
		})
	}
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

func TestNATSBridgeRealtimeChannelAcrossGateways(t *testing.T) {
	url := startRealtimeNATSServer(t)
	publisherConn, subscriberConn := connectRealtimeNATS(t, url), connectRealtimeNATS(t, url)
	publisherHub, subscriberHub := NewHub(), NewHub()
	publisherBridge, err := NewNATSBridge(context.Background(), publisherConn, publisherHub, "channel-publisher")
	if err != nil {
		t.Fatal(err)
	}
	subscriberBridge, err := NewNATSBridge(context.Background(), subscriberConn, subscriberHub, "channel-subscriber")
	if err != nil {
		t.Fatal(err)
	}
	defer publisherBridge.Close()
	defer subscriberBridge.Close()
	defer publisherHub.Close()
	defer subscriberHub.Close()
	session := subscriberHub.Register("subscriber")
	if err := subscriberHub.Subscribe(session, []string{"channel:orders.live"}); err != nil {
		t.Fatal(err)
	}
	publisherBridge.PublishChannel(context.Background(), domain.ChannelMessage{Channel: "orders.live", PublisherAppID: "publisher", Data: json.RawMessage(`{"id":"ord_1"}`)})
	frame := receive(t, session)
	if frame.Type != "channel.message" || frame.Channel != "orders.live" || frame.PublisherAppID != "publisher" || string(frame.Data) != `{"id":"ord_1"}` {
		t.Fatalf("unexpected channel frame: %#v", frame)
	}
}

func TestNATSBridgeRealtimeV2IsolatesAppsAcrossGateways(t *testing.T) {
	url := startRealtimeNATSServer(t)
	firstHub, secondHub := NewHub(), NewHub()
	defer firstHub.Close()
	defer secondHub.Close()
	firstBridge, err := NewNATSBridge(context.Background(), connectRealtimeNATS(t, url), firstHub, "v2-first")
	if err != nil {
		t.Fatal(err)
	}
	defer firstBridge.Close()
	secondBridge, err := NewNATSBridge(context.Background(), connectRealtimeNATS(t, url), secondHub, "v2-second")
	if err != nil {
		t.Fatal(err)
	}
	defer secondBridge.Close()

	capabilities := map[string][]string{"room": {"subscribe", "publish"}}
	source := firstHub.RegisterV2("app_a", "publisher", capabilities)
	local := firstHub.RegisterV2("app_a", "local", capabilities)
	remote := secondHub.RegisterV2("app_a", "remote", capabilities)
	outsider := secondHub.RegisterV2("app_b", "outsider", capabilities)
	for _, pair := range []struct {
		hub     *Hub
		session *Session
	}{{firstHub, source}, {firstHub, local}, {secondHub, remote}, {secondHub, outsider}} {
		if err := pair.hub.SubscribeV2(pair.session, []string{"room"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := firstHub.PublishV2(source, ClientFrame{Type: "channel.publish", Channel: "room", Audience: &Audience{Type: "others"}, Data: json.RawMessage(`{"text":"hello"}`)}); err != nil {
		t.Fatal(err)
	}
	if frame := receive(t, local); frame.PublisherClientID != "publisher" || frame.MessageID == "" {
		t.Fatalf("local frame=%#v", frame)
	}
	if frame := receive(t, remote); frame.AppID != "app_a" || string(frame.Data) != `{"text":"hello"}` {
		t.Fatalf("remote frame=%#v", frame)
	}
	if len(source.outbound) != 0 || len(outsider.outbound) != 0 {
		t.Fatal("v2 fanout leaked to publisher or another app")
	}
}

func TestNATSBridgeRealtimeV2RoutesDisconnectToOwningGateway(t *testing.T) {
	url := startRealtimeNATSServer(t)
	firstHub, secondHub := NewHub(), NewHub()
	defer firstHub.Close()
	defer secondHub.Close()
	first, err := NewNATSBridge(context.Background(), connectRealtimeNATS(t, url), firstHub, "owner-first")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := NewNATSBridge(context.Background(), connectRealtimeNATS(t, url), secondHub, "owner-second")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	target := secondHub.RegisterV2("app_a", "client_1", map[string][]string{"room": {"subscribe"}})
	if err := first.DisconnectRealtimeV2(context.Background(), "owner-second", "app_a", target.ID()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-target.Done():
	case <-time.After(time.Second):
		t.Fatal("remote owner did not disconnect target")
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

func TestCoreNATSResultWatchesAreScopedAndCleaned(t *testing.T) {
	url := startRealtimeNATSServer(t)
	connection := connectRealtimeNATS(t, url)
	bridge, err := NewNATSBridge(context.Background(), connection, NewHub(), "watch-instance")
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.Close()
	baseline := connection.NumSubscriptions()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first, err := bridge.WatchInvocation(ctx, "inv_first")
	if err != nil {
		t.Fatal(err)
	}
	second, err := bridge.WatchInvocation(context.Background(), "inv_second")
	if err != nil {
		t.Fatal(err)
	}
	if err := bridge.PublishInvocationResult(context.Background(), "inv_first"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-first.Updates():
	case <-time.After(time.Second):
		t.Fatal("matching invocation missed hint")
	}
	select {
	case <-second.Updates():
		t.Fatal("cross-invocation result hint")
	default:
	}
	cancel()
	select {
	case _, open := <-first.Updates():
		if open {
			t.Fatal("cancelled watch remained open")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled watch leaked")
	}
	first.Close()
	second.Close()
	if got := connection.NumSubscriptions(); got != baseline {
		t.Fatalf("subscriptions after close=%d want=%d", got, baseline)
	}
	last, err := bridge.WatchInvocation(context.Background(), "inv_last")
	if err != nil {
		t.Fatal(err)
	}
	bridge.Close()
	select {
	case _, open := <-last.Updates():
		if open {
			t.Fatal("bridge shutdown left watch open")
		}
	case <-time.After(time.Second):
		t.Fatal("bridge shutdown leaked watch")
	}
	if _, err := bridge.WatchInvocation(context.Background(), "inv_closed"); err == nil {
		t.Fatal("closed bridge accepted new watch")
	}
}

func TestCoreNATSAcceptanceBurstStopsRequestPublication(t *testing.T) {
	url := startRealtimeNATSServer(t)
	connection := connectRealtimeNATS(t, url)
	bridge, err := NewNATSBridge(context.Background(), connection, NewHub(), "acceptance-instance")
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.Close()
	var requests atomic.Int32
	subject, _ := natsbroker.FunctionSubject("owner", "fn_burst")
	subscription, err := connection.Subscribe(subject, func(message *gonats.Msg) {
		requests.Add(1)
		var request natsInvocationRequest
		if json.Unmarshal(message.Data, &request) != nil {
			return
		}
		// Drive the real reply handler synchronously so rejection hints can fill
		// the wakeup slot before success, followed by a stale rejection.
		for _, accepted := range []bool{false, false, true, false} {
			raw, _ := json.Marshal(natsInvocationAcceptance{RequestID: request.RequestID, InvocationID: request.Frame.InvocationID, Accepted: accepted})
			bridge.handleAcceptance(&gonats.Msg{Data: raw})
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Unsubscribe()
	if err := connection.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	invocation := domain.Invocation{ID: "inv_burst", FunctionID: "fn_burst", OwnerAppID: "owner", Name: "calculate", Input: json.RawMessage(`{}`), ClaimBy: now.Add(200 * time.Millisecond), Deadline: now.Add(time.Second)}
	if err := bridge.PublishInvocation(context.Background(), invocation); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if got := requests.Load(); got != 1 {
		t.Fatalf("acceptance burst caused %d request publications, want 1", got)
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
