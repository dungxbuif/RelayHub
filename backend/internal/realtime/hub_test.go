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
	"github.com/dungxbuif/RelayHub/internal/redisstate"
	"github.com/gorilla/websocket"
)

type historyMemory struct {
	entries map[string][]redisstate.RealtimeHistoryEntry
	err     error
}

type publishLimitMemory struct {
	calls  []string
	denied map[string]bool
	err    error
}

func (limit *publishLimitMemory) Allow(_ context.Context, appID, connectionID, channel string) (bool, error) {
	limit.calls = append(limit.calls, appID+"/"+connectionID+"/"+channel)
	return !limit.denied[channel], limit.err
}

func (history *historyMemory) Append(_ context.Context, appID, channel string, payload json.RawMessage) (string, error) {
	if history.err != nil {
		return "", history.err
	}
	key := appID + "/" + channel
	cursor := fmt.Sprintf("cursor_%d", len(history.entries[key])+1)
	history.entries[key] = append(history.entries[key], redisstate.RealtimeHistoryEntry{Cursor: cursor, Payload: append([]byte(nil), payload...)})
	return cursor, nil
}

func (history *historyMemory) Read(_ context.Context, appID, channel, _ string, limit int64) ([]redisstate.RealtimeHistoryEntry, string, error) {
	if history.err != nil {
		return nil, "", history.err
	}
	items := history.entries[appID+"/"+channel]
	if int64(len(items)) > limit {
		items = items[len(items)-int(limit):]
	}
	return append([]redisstate.RealtimeHistoryEntry(nil), items...), "", nil
}

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

func TestHubRealtimeChannelSubscriptionsAreIsolated(t *testing.T) {
	h := NewHub()
	defer h.Close()
	a := h.Register("a")
	a2 := h.Register("a")
	b := h.Register("b")
	if err := h.Subscribe(a, []string{"channel:orders.live"}); err != nil {
		t.Fatal(err)
	}
	if err := h.Subscribe(a2, []string{"channel:orders.audit"}); err != nil {
		t.Fatal(err)
	}
	if err := h.Subscribe(b, []string{"channel:orders.live"}); err != nil {
		t.Fatal(err)
	}
	h.PublishChannel(context.Background(), ChannelMessage{Channel: "orders.live", PublisherAppID: "source", Data: []byte(`{"id":"ord_1"}`)})
	if frame := receive(t, a); frame.Type != "channel.message" || frame.Channel != "orders.live" || frame.PublisherAppID != "source" || string(frame.Data) != `{"id":"ord_1"}` {
		t.Fatalf("channel frame %#v", frame)
	}
	if len(a2.outbound) != 0 {
		t.Fatal("cross-channel delivery")
	}
	if frame := receive(t, b); frame.Type != "channel.message" || frame.Channel != "orders.live" {
		t.Fatalf("same channel app delivery %#v", frame)
	}
}

func TestHubRealtimeV2ACLIsolationUnsubscribeAndAudiences(t *testing.T) {
	h := NewHub()
	defer h.Close()
	capabilities := map[string][]string{"room": {"subscribe", "publish"}}
	publisher := h.RegisterV2("app_a", "client_1", capabilities)
	sameClient := h.RegisterV2("app_a", "client_1", map[string][]string{"room": {"subscribe"}})
	otherClient := h.RegisterV2("app_a", "client_2", map[string][]string{"room": {"subscribe"}})
	otherApp := h.RegisterV2("app_b", "client_2", map[string][]string{"room": {"subscribe"}})
	denied := h.RegisterV2("app_a", "client_3", map[string][]string{"other": {"subscribe"}})
	for _, session := range []*Session{publisher, sameClient, otherClient, otherApp} {
		if err := h.SubscribeV2(session, []string{"room"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.SubscribeV2(denied, []string{"room"}); err == nil || err.Code != "forbidden" {
		t.Fatalf("unauthorized subscribe error=%v", err)
	}
	if err := h.PublishV2(publisher, ClientFrame{Type: "channel.publish", Channel: "room", Data: json.RawMessage(`{"n":1}`), Audience: &Audience{Type: "others"}}); err != nil {
		t.Fatal(err)
	}
	for _, session := range []*Session{sameClient, otherClient} {
		frame := receive(t, session)
		if frame.AppID != "app_a" || frame.PublisherClientID != "client_1" || frame.PublisherConnectionID != publisher.ID() || frame.MessageID == "" || frame.PublishedAt == "" {
			t.Fatalf("untrusted/missing envelope %#v", frame)
		}
	}
	if len(publisher.outbound) != 0 || len(otherApp.outbound) != 0 {
		t.Fatal("others audience or app isolation failed")
	}

	if err := h.PublishV2(publisher, ClientFrame{Type: "channel.publish", Channel: "room", Data: json.RawMessage(`{}`), Audience: &Audience{Type: "client", ClientID: "client_1"}}); err != nil {
		t.Fatal(err)
	}
	if receive(t, publisher).Audience.Type != "client" || receive(t, sameClient).Audience.ClientID != "client_1" {
		t.Fatal("client targeting failed")
	}
	if len(otherClient.outbound) != 0 {
		t.Fatal("client target leaked")
	}

	if err := h.UnsubscribeV2(sameClient, []string{"room"}); err != nil {
		t.Fatal(err)
	}
	if err := h.PublishV2(publisher, ClientFrame{Type: "channel.publish", Channel: "room", Data: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	_ = receive(t, publisher)
	_ = receive(t, otherClient)
	if len(sameClient.outbound) != 0 {
		t.Fatal("unsubscribed connection received message")
	}
}

func TestHubRealtimeV2RejectsUnauthorizedPublishAndCrossAppTarget(t *testing.T) {
	h := NewHub()
	defer h.Close()
	readOnly := h.RegisterV2("app_a", "reader", map[string][]string{"room": {"subscribe"}})
	if err := h.PublishV2(readOnly, ClientFrame{Type: "channel.publish", Channel: "room", Data: json.RawMessage(`{}`)}); err == nil || err.Code != "forbidden" {
		t.Fatalf("unauthorized publish error=%v", err)
	}
	publisher := h.RegisterV2("app_a", "writer", map[string][]string{"room": {"publish"}})
	target := h.RegisterV2("app_b", "target", map[string][]string{"room": {"subscribe"}})
	if err := h.SubscribeV2(target, []string{"room"}); err != nil {
		t.Fatal(err)
	}
	if err := h.PublishV2(publisher, ClientFrame{Type: "channel.publish", Channel: "room", Data: json.RawMessage(`{}`), Audience: &Audience{Type: "connection", ConnectionID: target.ID()}}); err == nil || err.Code != "target_not_found" {
		t.Fatalf("cross-app target error=%v", err)
	}
}

func TestHubRealtimeV2NamespaceGrantMatchesOneResolvedChannelSegment(t *testing.T) {
	h := NewHub()
	defer h.Close()
	session := h.RegisterV2("app_a", "client_1", map[string][]string{"tenant:42:*": {"subscribe", "publish"}})
	if err := h.SubscribeV2(session, []string{"tenant:42:orders"}); err != nil {
		t.Fatalf("bounded namespace subscribe error=%v", err)
	}
	if err := h.PublishV2(session, ClientFrame{Type: "channel.publish", Channel: "tenant:42:orders", Data: json.RawMessage(`{"ok":true}`)}); err != nil {
		t.Fatalf("bounded namespace publish error=%v", err)
	}
	if err := h.SubscribeV2(session, []string{"tenant:42:orders:created"}); err == nil || err.Code != "forbidden" {
		t.Fatalf("multi-segment expansion error=%v, want forbidden", err)
	}
	if err := h.SubscribeV2(session, []string{"tenant:43:orders"}); err == nil || err.Code != "forbidden" {
		t.Fatalf("cross-namespace expansion error=%v, want forbidden", err)
	}
}

func TestHubRealtimeV2HistoryStoresBroadcastOnceAndEnforcesCapability(t *testing.T) {
	h := NewHub()
	defer h.Close()
	history := &historyMemory{entries: map[string][]redisstate.RealtimeHistoryEntry{}}
	h.SetHistoryStore(history)
	publisher := h.RegisterV2("app_a", "publisher", map[string][]string{"room": {"publish", "history"}})
	reader := h.RegisterV2("app_a", "reader", map[string][]string{"room": {"history"}})
	denied := h.RegisterV2("app_a", "denied", map[string][]string{"room": {"subscribe"}})
	if err := h.PublishV2(publisher, ClientFrame{Type: "channel.publish", Channel: "room", Data: json.RawMessage(`{"n":1}`)}); err != nil {
		t.Fatal(err)
	}
	if err := h.PublishV2(publisher, ClientFrame{Type: "channel.publish", Channel: "room", Audience: &Audience{Type: "client", ClientID: "reader"}, Data: json.RawMessage(`{"secret":true}`)}); err == nil || err.Code != "target_not_found" {
		t.Fatalf("targeted publish error=%v", err)
	}
	result, err := h.HistoryV2(reader, "room", "", 10)
	if err != nil || len(result.Items) != 1 || string(result.Items[0].Data) != `{"n":1}` || result.Items[0].Cursor == "" {
		t.Fatalf("history=%#v error=%v", result, err)
	}
	if _, err := h.HistoryV2(denied, "room", "", 10); err == nil || err.Code != "forbidden" {
		t.Fatalf("unauthorized history error=%v", err)
	}
	if _, err := h.HistoryV2(h.RegisterV2("app_b", "reader", map[string][]string{"room": {"history"}}), "room", "", 10); err != nil {
		t.Fatal(err)
	}
}

func TestHubRealtimeV2HistoryRetentionIsDisabledWithoutPublisherHistoryCapability(t *testing.T) {
	h := NewHub()
	defer h.Close()
	history := &historyMemory{entries: map[string][]redisstate.RealtimeHistoryEntry{}}
	h.SetHistoryStore(history)
	publisher := h.RegisterV2("app_a", "publisher", map[string][]string{"room": {"publish"}})
	if err := h.PublishV2(publisher, ClientFrame{Type: "channel.publish", Channel: "room", Data: json.RawMessage(`{"n":1}`)}); err != nil {
		t.Fatal(err)
	}
	if len(history.entries["app_a/room"]) != 0 {
		t.Fatal("history retained without an explicit history grant")
	}
}

func TestHubRealtimeV2HistoryFailurePreventsUnrecordedBroadcast(t *testing.T) {
	h := NewHub()
	defer h.Close()
	h.SetHistoryStore(&historyMemory{err: errors.New("redis unavailable")})
	publisher := h.RegisterV2("app_a", "publisher", map[string][]string{"room": {"publish", "history"}})
	reader := h.RegisterV2("app_a", "reader", map[string][]string{"room": {"subscribe"}})
	if err := h.SubscribeV2(reader, []string{"room"}); err != nil {
		t.Fatal(err)
	}
	if err := h.PublishV2(publisher, ClientFrame{Type: "channel.publish", Channel: "room", Data: json.RawMessage(`{"n":1}`)}); err == nil || err.Code != "history_unavailable" {
		t.Fatalf("publish error=%v", err)
	}
	if len(reader.outbound) != 0 {
		t.Fatal("unrecorded broadcast was delivered")
	}
}

func TestHubRealtimeV2HistoryMapsStoreRejectedCursorToClientError(t *testing.T) {
	h := NewHub()
	defer h.Close()
	h.SetHistoryStore(&historyMemory{err: redisstate.ErrInvalidRecord})
	reader := h.RegisterV2("app_a", "reader", map[string][]string{"room": {"history"}})
	if _, err := h.HistoryV2(reader, "room", "abc", 10); err == nil || err.Code != "invalid_history" {
		t.Fatalf("history error=%v, want invalid_history", err)
	}
}

func TestHubRealtimeV2RewindBuffersLiveFramesUntilContinuityBoundary(t *testing.T) {
	h := NewHub()
	defer h.Close()
	history := &historyMemory{entries: map[string][]redisstate.RealtimeHistoryEntry{}}
	h.SetHistoryStore(history)
	publisher := h.RegisterV2("app_a", "publisher", map[string][]string{"room": {"publish", "history"}})
	reader := h.RegisterV2("app_a", "reader", map[string][]string{"room": {"subscribe", "history"}})
	if err := h.SubscribeRewindV2(reader, []string{"room"}); err != nil {
		t.Fatal(err)
	}
	page, err := h.HistoryV2(reader, "room", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	reader.Send(page)
	reader.Send(ServerFrame{Type: "subscribed", Channels: []string{"room"}})
	if err := h.PublishV2(publisher, ClientFrame{Type: "channel.publish", Channel: "room", Audience: &Audience{Type: "connection", ConnectionID: reader.ID()}, Data: json.RawMessage(`{"during":"rewind"}`)}); err != nil {
		t.Fatal(err)
	}
	if len(reader.outbound) != 2 {
		t.Fatalf("live frame escaped rewind barrier: queued=%d", len(reader.outbound))
	}
	h.FinishRewindV2(reader, []ServerFrame{page})
	if first, second, live := receive(t, reader), receive(t, reader), receive(t, reader); first.Type != "history.result" || second.Type != "subscribed" || live.Type != "channel.message" || string(live.Data) != `{"during":"rewind"}` {
		t.Fatalf("frames=%#v %#v %#v", first, second, live)
	}
}

func TestHubRealtimeV2RewindDoesNotRedeliverMessageAlreadyInHistory(t *testing.T) {
	h := NewHub()
	defer h.Close()
	history := &historyMemory{entries: map[string][]redisstate.RealtimeHistoryEntry{}}
	h.SetHistoryStore(history)
	publisher := h.RegisterV2("app_a", "publisher", map[string][]string{"room": {"publish", "history"}})
	reader := h.RegisterV2("app_a", "reader", map[string][]string{"room": {"subscribe", "history"}})
	if err := h.SubscribeRewindV2(reader, []string{"room"}); err != nil {
		t.Fatal(err)
	}
	if err := h.PublishV2(publisher, ClientFrame{Type: "channel.publish", Channel: "room", Data: json.RawMessage(`{"n":1}`)}); err != nil {
		t.Fatal(err)
	}
	page, err := h.HistoryV2(reader, "room", "", 10)
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("page=%#v error=%v", page, err)
	}
	reader.Send(page)
	reader.Send(ServerFrame{Type: "subscribed", Channels: []string{"room"}})
	h.FinishRewindV2(reader, []ServerFrame{page})
	_ = receive(t, reader)
	_ = receive(t, reader)
	if len(reader.outbound) != 0 {
		t.Fatal("history message was duplicated from live rewind buffer")
	}
	h.DeliverV2("app_a", page.Items[0])
	if len(reader.outbound) != 0 {
		t.Fatal("delayed cross-replica frame duplicated a completed rewind")
	}
}

func TestHubRealtimeV2PreservesOpaqueEncryptedEnvelope(t *testing.T) {
	h := NewHub()
	defer h.Close()
	publisher := h.RegisterV2("app_a", "publisher", map[string][]string{"private:room": {"publish"}})
	reader := h.RegisterV2("app_a", "reader", map[string][]string{"private:room": {"subscribe"}})
	if err := h.SubscribeV2(reader, []string{"private:room"}); err != nil {
		t.Fatal(err)
	}
	envelope := &EncryptionEnvelope{Algorithm: "aes-256-gcm", KeyID: "key-1", Nonce: "AAAAAAAAAAAAAAAA", Ciphertext: "AAAAAAAAAAAAAAAAAAAAAA"}
	if err := h.PublishV2(publisher, ClientFrame{Type: "channel.publish", Channel: "private:room", Encryption: envelope}); err != nil {
		t.Fatal(err)
	}
	frame := receive(t, reader)
	if frame.Encryption == nil || frame.Encryption.Ciphertext != envelope.Ciphertext || len(frame.Data) != 0 {
		t.Fatalf("encrypted frame=%#v", frame)
	}
}

func TestHubRealtimeV2BatchPublishReturnsStablePerItemOutcomes(t *testing.T) {
	h := NewHub()
	defer h.Close()
	publisher := h.RegisterV2("app_a", "publisher", map[string][]string{"tenant:42:*": {"publish"}})
	reader := h.RegisterV2("app_a", "reader", map[string][]string{"tenant:42:orders": {"subscribe"}})
	if err := h.SubscribeV2(reader, []string{"tenant:42:orders"}); err != nil {
		t.Fatal(err)
	}
	result := h.PublishBatchV2(publisher, []PublishItem{
		{ID: "accepted", Channel: "tenant:42:orders", Data: json.RawMessage(`{"n":1}`)},
		{ID: "second", Channel: "tenant:42:updates", Data: json.RawMessage(`{"n":2}`)},
	})
	if result.Type != "channel.publish.batch.result" || len(result.Outcomes) != 2 || !result.Outcomes[0].Accepted || result.Outcomes[0].MessageID == "" || !result.Outcomes[1].Accepted || result.Outcomes[1].MessageID == "" {
		t.Fatalf("result=%#v", result)
	}
	if frame := receive(t, reader); string(frame.Data) != `{"n":1}` {
		t.Fatalf("delivered=%#v", frame)
	}
}

func TestHubRealtimeV2BatchAuthorizationIsAtomicBeforeDispatch(t *testing.T) {
	h := NewHub()
	defer h.Close()
	limit := &publishLimitMemory{denied: map[string]bool{}}
	h.SetPublishLimiter(limit)
	publisher := h.RegisterV2("app_a", "publisher", map[string][]string{"tenant:42:*": {"publish"}})
	reader := h.RegisterV2("app_a", "reader", map[string][]string{"tenant:42:orders": {"subscribe"}})
	if err := h.SubscribeV2(reader, []string{"tenant:42:orders"}); err != nil {
		t.Fatal(err)
	}
	result := h.PublishBatchV2(publisher, []PublishItem{
		{ID: "otherwise-valid", Channel: "tenant:42:orders", Data: json.RawMessage(`{"n":1}`)},
		{ID: "denied", Channel: "tenant:43:orders", Data: json.RawMessage(`{"n":2}`)},
	})
	if len(result.Outcomes) != 2 || result.Outcomes[0].Accepted || result.Outcomes[0].Code != "batch_rejected" || result.Outcomes[1].Accepted || result.Outcomes[1].Code != "forbidden" {
		t.Fatalf("result=%#v", result)
	}
	if len(reader.outbound) != 0 {
		t.Fatal("batch dispatched before authorization preflight completed")
	}
	if len(limit.calls) != 0 {
		t.Fatalf("authorization-rejected batch consumed rate quota: %v", limit.calls)
	}
}

func TestHubRealtimeV2BatchTakesOneRateDecisionPerItemBeforeDispatch(t *testing.T) {
	h := NewHub()
	defer h.Close()
	limit := &publishLimitMemory{denied: map[string]bool{"tenant:42:updates": true}}
	h.SetPublishLimiter(limit)
	publisher := h.RegisterV2("app_a", "publisher", map[string][]string{"tenant:42:*": {"publish"}})
	reader := h.RegisterV2("app_a", "reader", map[string][]string{"tenant:42:orders": {"subscribe"}})
	if err := h.SubscribeV2(reader, []string{"tenant:42:orders"}); err != nil {
		t.Fatal(err)
	}
	result := h.PublishBatchV2(publisher, []PublishItem{
		{ID: "one", Channel: "tenant:42:orders", Data: json.RawMessage(`{"n":1}`)},
		{ID: "two", Channel: "tenant:42:updates", Data: json.RawMessage(`{"n":2}`)},
	})
	if len(limit.calls) != 2 || len(result.Outcomes) != 2 || result.Outcomes[0].Code != "batch_rejected" || result.Outcomes[1].Code != "rate_limited" {
		t.Fatalf("calls=%v result=%#v", limit.calls, result)
	}
	if len(reader.outbound) != 0 {
		t.Fatal("batch dispatched before rate preflight completed")
	}
}

func TestHubRealtimeV2SinglePublishFailsClosedWhenRateStoreUnavailable(t *testing.T) {
	h := NewHub()
	defer h.Close()
	h.SetPublishLimiter(&publishLimitMemory{err: errors.New("redis unavailable")})
	publisher := h.RegisterV2("app_a", "publisher", map[string][]string{"room": {"publish"}})
	if err := h.PublishV2(publisher, ClientFrame{Type: "channel.publish", Channel: "room", Data: json.RawMessage(`{}`)}); err == nil || err.Code != "realtime_unavailable" {
		t.Fatalf("publish error=%v", err)
	}
}

func TestHubRejectsInvalidRealtimeChannelTopics(t *testing.T) {
	h := NewHub()
	defer h.Close()
	session := h.Register("a")
	for _, topics := range [][]string{{"channel:"}, {"channel:Bad"}, {"channel:" + strings.Repeat("x", 97)}, {"channel:orders.live", "channel:orders.live"}} {
		if err := h.Subscribe(session, topics); err == nil || err.Code != "invalid_topics" {
			t.Fatalf("Subscribe(%v) error=%v, want invalid_topics", topics, err)
		}
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

func TestHubRealtimeV2PresenceLifecycleAndACL(t *testing.T) {
	h := NewHub()
	defer h.Close()
	member := h.RegisterV2("app_a", "client_1", map[string][]string{"room": {"subscribe", "presence"}})
	observer := h.RegisterV2("app_a", "client_2", map[string][]string{"room": {"subscribe"}})
	denied := h.RegisterV2("app_a", "client_3", map[string][]string{"room": {"subscribe"}})
	for _, session := range []*Session{member, observer, denied} {
		if err := h.SubscribeV2(session, []string{"room"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.UpdatePresence(denied, ClientFrame{Type: "presence.update", Channel: "room", Data: json.RawMessage(`{}`)}); err == nil || err.Code != "forbidden" {
		t.Fatalf("presence ACL error=%v", err)
	}
	if err := h.UpdatePresence(member, ClientFrame{Type: "presence.update", Channel: "room", Data: json.RawMessage(`{"status":"online"}`)}); err != nil {
		t.Fatal(err)
	}
	for _, session := range []*Session{member, observer, denied} {
		frame := receive(t, session)
		if frame.Type != "presence.join" || frame.PublisherClientID != "client_1" || frame.Occupancy != 1 {
			t.Fatalf("join=%#v", frame)
		}
	}
	if err := h.UpdatePresence(member, ClientFrame{Type: "presence.update", Channel: "room", Data: json.RawMessage(`{"status":"away"}`)}); err != nil {
		t.Fatal(err)
	}
	for _, session := range []*Session{member, observer, denied} {
		if frame := receive(t, session); frame.Type != "presence.update" || string(frame.Data) != `{"status":"away"}` {
			t.Fatalf("update=%#v", frame)
		}
	}
	member.Close()
	for _, session := range []*Session{observer, denied} {
		if frame := receive(t, session); frame.Type != "presence.leave" || frame.Occupancy != 0 {
			t.Fatalf("leave=%#v", frame)
		}
	}
}
