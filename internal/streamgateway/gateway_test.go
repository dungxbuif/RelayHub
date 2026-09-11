package streamgateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/broker"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/dungxbuif/RelayHub/internal/streamprotocol"
	"github.com/gorilla/websocket"
)

func TestSessionConsumesAssignsAndAcknowledges(t *testing.T) {
	gateway, consumer, assignments := testGateway(t, 4, 1<<20)
	session, err := gateway.open("app_target")
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(context.Background())
	assertFrameType(t, session.Frames(), "ready")
	if protocolErr := session.Handle(context.Background(), []byte(`{"type":"consumer.start","protocol_version":1,"consumer":"default","max_in_flight":2}`)); protocolErr != nil {
		t.Fatal(protocolErr)
	}
	assertFrameType(t, session.Frames(), "consumer.started")
	if consumer.config.Stream != "RH_DELIVERIES" || consumer.config.MaxPending != 4 || consumer.config.DurableName != durableName("app_target") || !strings.HasPrefix(consumer.config.Filter, "rh.v1.delivery.") {
		t.Fatalf("consumer config leaked/drifted: %#v", consumer.config)
	}
	message := &fakeMessage{data: envelope(t, "dlv_one", "order.created")}
	consumer.deliver(message)
	delivery := readFrame(t, session.Frames())
	if delivery["type"] != "event.delivery" || delivery["delivery_id"] != "dlv_one" || delivery["attempt"] != float64(1) {
		t.Fatalf("delivery=%v", delivery)
	}
	if protocolErr := session.Handle(context.Background(), []byte(`{"type":"delivery.ack","delivery_id":"dlv_one"}`)); protocolErr != nil {
		t.Fatal(protocolErr)
	}
	assertFrameType(t, session.Frames(), "delivery.accepted")
	if message.acks != 1 || assignments.acked != 1 {
		t.Fatalf("broker acks=%d store acks=%d", message.acks, assignments.acked)
	}
	if assignments.lastApp != "app_target" || assignments.lastConnection != session.ID() || assignments.lastToken == "" {
		t.Fatalf("assignment fence=%#v", assignments)
	}
}

func TestSessionNackProgressAndAssignmentFence(t *testing.T) {
	gateway, consumer, assignments := testGateway(t, 4, 1<<20)
	session, _ := gateway.open("app_target")
	defer session.Close(context.Background())
	<-session.Frames()
	if err := session.Handle(context.Background(), []byte(`{"type":"consumer.start","protocol_version":1,"consumer":"default","max_in_flight":3}`)); err != nil {
		t.Fatal(err)
	}
	<-session.Frames()
	progress := &fakeMessage{data: envelope(t, "dlv_progress", "order.created")}
	consumer.deliver(progress)
	<-session.Frames()
	if err := session.Handle(context.Background(), []byte(`{"type":"delivery.progress","delivery_id":"dlv_progress"}`)); err != nil {
		t.Fatal(err)
	}
	<-session.Frames()
	if progress.progress != 1 || assignments.progressed != 1 {
		t.Fatalf("progress broker=%d store=%d", progress.progress, assignments.progressed)
	}
	nack := &fakeMessage{data: envelope(t, "dlv_nack", "order.created")}
	consumer.deliver(nack)
	<-session.Frames()
	if err := session.Handle(context.Background(), []byte(`{"type":"delivery.nack","delivery_id":"dlv_nack","delay_ms":1234}`)); err != nil {
		t.Fatal(err)
	}
	<-session.Frames()
	if nack.nacks != 1 || nack.delay != 1234*time.Millisecond || assignments.released != 1 {
		t.Fatalf("nack=%#v released=%d", nack, assignments.released)
	}
	for _, raw := range []string{`{"type":"delivery.ack","delivery_id":"dlv_missing"}`, `{"type":"delivery.progress","delivery_id":"dlv_nack"}`} {
		if got := session.Handle(context.Background(), []byte(raw)); got == nil || got.Code != "delivery_not_assigned" {
			t.Fatalf("frame=%s error=%v", raw, got)
		}
	}
}

func TestDuplicatePhysicalMessagesAndBackpressure(t *testing.T) {
	gateway, consumer, assignments := testGateway(t, 1, 1024)
	session, _ := gateway.open("app_target")
	defer session.Close(context.Background())
	<-session.Frames()
	_ = session.Handle(context.Background(), []byte(`{"type":"consumer.start","protocol_version":1,"consumer":"default","max_in_flight":1}`))
	<-session.Frames()
	first := &fakeMessage{data: envelope(t, "dlv_one", "order.created")}
	consumer.deliver(first)
	<-session.Frames()
	second := &fakeMessage{data: envelope(t, "dlv_two", "order.created")}
	consumer.deliver(second)
	if second.nacks != 1 || assignments.assignCalls != 1 {
		t.Fatalf("capacity assigned=%d nacks=%d", assignments.assignCalls, second.nacks)
	}
	if err := session.Handle(context.Background(), []byte(`{"type":"delivery.ack","delivery_id":"dlv_one"}`)); err != nil {
		t.Fatal(err)
	}
	<-session.Frames()
	assignments.disposition = store.DeliveryAlreadyComplete
	duplicate := &fakeMessage{data: envelope(t, "dlv_done", "order.created")}
	consumer.deliver(duplicate)
	if duplicate.acks != 1 {
		t.Fatal("completed physical duplicate was not suppressed")
	}
	assignments.disposition = store.DeliveryAlreadyAssigned
	activeDuplicate := &fakeMessage{data: envelope(t, "dlv_active", "order.created")}
	consumer.deliver(activeDuplicate)
	if activeDuplicate.nacks != 1 {
		t.Fatal("active physical duplicate was not deferred")
	}
}

func TestByteBackpressureAndOversizeBrokerFrames(t *testing.T) {
	gateway, consumer, assignments := testGateway(t, 4, 100)
	session, _ := gateway.open("app_target")
	defer session.Close(context.Background())
	<-session.Frames()
	_ = session.Handle(context.Background(), []byte(`{"type":"consumer.start","protocol_version":1,"consumer":"default","max_in_flight":4}`))
	<-session.Frames()
	bounded := &fakeMessage{data: envelope(t, "dlv_bytes", "order.created")}
	consumer.deliver(bounded)
	if bounded.nacks != 1 || assignments.assignCalls != 0 || assignments.released != 0 {
		t.Fatalf("byte pressure nack=%d assign=%d release=%d", bounded.nacks, assignments.assignCalls, assignments.released)
	}
	oversize := &fakeMessage{data: []byte(`{"delivery_id":"dlv_large","event":{"id":"evt_large","type":"order.created","source_app_id":"app_source","target_app_ids":["app_target"],"data":{"value":"` + strings.Repeat("x", streamprotocol.MaxMessageBytes) + `"},"created_at":"2026-09-12T10:00:00Z"}}`)}
	consumer.deliver(oversize)
	if oversize.nacks != 1 || assignments.assignCalls != 0 {
		t.Fatalf("oversize nack=%d assignments=%d", oversize.nacks, assignments.assignCalls)
	}
}

func TestDurableIdentityIsStableAndApplicationScoped(t *testing.T) {
	if durableName("app_one") != durableName("app_one") || durableName("app_one") == durableName("app_two") || !strings.HasSuffix(durableName("app_one"), "_default") {
		t.Fatal("durable identity is not stable and app scoped")
	}
}

func TestMalformedUnicodeFilterAndDrainRedelivery(t *testing.T) {
	gateway, consumer, assignments := testGateway(t, 2, 1<<20)
	session, _ := gateway.open("app_target")
	<-session.Frames()
	longUnicode := strings.Repeat("đơn.hàng.", 100)
	start, _ := json.Marshal(map[string]any{"type": "consumer.start", "protocol_version": 1, "consumer": "default", "max_in_flight": 2})
	if err := session.Handle(context.Background(), start); err != nil {
		t.Fatal(err)
	}
	<-session.Frames()
	bad := &fakeMessage{data: []byte(`{"delivery_id":"dlv_bad","event":null}`)}
	consumer.deliver(bad)
	if bad.nacks != 1 || assignments.assignCalls != 0 {
		t.Fatal("malformed broker payload reached assignment")
	}
	message := &fakeMessage{data: envelope(t, "dlv_unicode", longUnicode)}
	consumer.deliver(message)
	assertFrameType(t, session.Frames(), "event.delivery")
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if assignments.released != 1 || message.nacks != 1 || consumer.subscription.drains != 1 {
		t.Fatalf("release=%d nack=%d drains=%d", assignments.released, message.nacks, consumer.subscription.drains)
	}
}

func TestCloseRejectsAssignmentThatCommitsAfterAdmissionCloses(t *testing.T) {
	gateway, consumer, assignments := testGateway(t, 2, 1<<20)
	assignEntered := make(chan struct{})
	assignments.assignEntered = assignEntered
	assignments.continueAssign = make(chan struct{})
	session, _ := gateway.open("app_target")
	<-session.Frames()
	if err := session.Handle(context.Background(), []byte(`{"type":"consumer.start","protocol_version":1,"consumer":"default","max_in_flight":2}`)); err != nil {
		t.Fatal(err)
	}
	<-session.Frames()
	message := &fakeMessage{data: envelope(t, "dlv_close_race", "order.created")}
	delivered := make(chan struct{})
	go func() {
		consumer.deliver(message)
		close(delivered)
	}()
	<-assignEntered
	closed := make(chan error, 1)
	go func() { closed <- session.Close(context.Background()) }()
	for {
		session.mu.Lock()
		closing := session.closing
		session.mu.Unlock()
		if closing {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(assignments.continueAssign)
	<-delivered
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if message.nacks != 1 || assignments.released != 1 {
		t.Fatalf("nacks=%d releases=%d", message.nacks, assignments.released)
	}
	select {
	case frame := <-session.Frames():
		t.Fatalf("delivery admitted after close: %s", frame)
	default:
	}
}

func TestPendingDatabaseAssignmentReservesCapacity(t *testing.T) {
	gateway, consumer, assignments := testGateway(t, 1, 1<<20)
	assignEntered := make(chan struct{})
	assignments.assignEntered = assignEntered
	assignments.continueAssign = make(chan struct{})
	session, _ := gateway.open("app_target")
	defer session.Close(context.Background())
	<-session.Frames()
	if err := session.Handle(context.Background(), []byte(`{"type":"consumer.start","protocol_version":1,"consumer":"default","max_in_flight":1}`)); err != nil {
		t.Fatal(err)
	}
	<-session.Frames()
	first := &fakeMessage{data: envelope(t, "dlv_pending", "order.created")}
	finished := make(chan struct{})
	go func() {
		consumer.deliver(first)
		close(finished)
	}()
	<-assignEntered
	second := &fakeMessage{data: envelope(t, "dlv_overbooked", "order.created")}
	consumer.deliver(second)
	if second.nacks != 1 {
		t.Fatalf("second nack=%d", second.nacks)
	}
	assignments.mu.Lock()
	assignCalls := assignments.assignCalls
	assignments.mu.Unlock()
	if assignCalls != 1 {
		t.Fatalf("database assignments=%d", assignCalls)
	}
	close(assignments.continueAssign)
	<-finished
	assertFrameType(t, session.Frames(), "event.delivery")
}

func TestClientProtocolErrorsDoNotStartAnotherConsumer(t *testing.T) {
	gateway, consumer, _ := testGateway(t, 2, 1<<20)
	session, _ := gateway.open("app_target")
	defer session.Close(context.Background())
	<-session.Frames()
	for _, tc := range []struct{ raw, code string }{
		{`{"type":"consumer.start","protocol_version":2,"consumer":"default","max_in_flight":1}`, "unsupported_version"},
		{`{"type":"consumer.start","protocol_version":1,"consumer":"custom","max_in_flight":1}`, "invalid_frame"},
		{`{"type":"delivery.ack","delivery_id":"dlv_missing"}`, "delivery_not_assigned"},
	} {
		if got := session.Handle(context.Background(), []byte(tc.raw)); got == nil || got.Code != tc.code {
			t.Fatalf("%s got=%v", tc.raw, got)
		}
	}
	if err := session.Handle(context.Background(), []byte(`{"type":"consumer.start","protocol_version":1,"consumer":"default","max_in_flight":1}`)); err != nil {
		t.Fatal(err)
	}
	<-session.Frames()
	if got := session.Handle(context.Background(), []byte(`{"type":"consumer.start","protocol_version":1,"consumer":"default","max_in_flight":1}`)); got == nil || got.Code != "consumer_already_started" {
		t.Fatalf("got=%v", got)
	}
	if consumer.starts != 1 {
		t.Fatalf("consumer starts=%d", consumer.starts)
	}
}

func TestRealWebSocketDeliveryAndMalformedFrame(t *testing.T) {
	gateway, consumer, _ := testGateway(t, 2, 1<<20)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		upgrader := websocket.Upgrader{Subprotocols: []string{streamprotocol.Subprotocol}}
		connection, err := upgrader.Upgrade(response, request, nil)
		if err == nil {
			gateway.Serve(request.Context(), "app_target", connection)
		}
	}))
	defer server.Close()
	endpoint, _ := url.Parse(server.URL)
	endpoint.Scheme = "ws"
	dialer := *websocket.DefaultDialer
	dialer.Subprotocols = []string{streamprotocol.Subprotocol}
	connection, _, err := dialer.Dial(endpoint.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	var frame map[string]any
	_ = connection.SetReadDeadline(time.Now().Add(2 * time.Second))
	if connection.ReadJSON(&frame) != nil || frame["type"] != "ready" {
		t.Fatalf("ready=%v", frame)
	}
	if connection.WriteJSON(map[string]any{"type": "consumer.start", "protocol_version": 1, "consumer": "default", "max_in_flight": 2}) != nil {
		t.Fatal("write start")
	}
	if connection.ReadJSON(&frame) != nil || frame["type"] != "consumer.started" {
		t.Fatalf("started=%v", frame)
	}
	message := &fakeMessage{data: envelope(t, "dlv_socket", "đơn.hàng.tạo")}
	consumer.deliver(message)
	if connection.ReadJSON(&frame) != nil || frame["type"] != "event.delivery" {
		t.Fatalf("delivery=%v", frame)
	}
	if connection.WriteMessage(websocket.TextMessage, []byte(`{"type":"delivery.ack","delivery_id":"dlv_missing"}`)) != nil {
		t.Fatal("write malformed ack")
	}
	if connection.ReadJSON(&frame) != nil || frame["type"] != "error" || frame["code"] != "delivery_not_assigned" {
		t.Fatalf("error=%v", frame)
	}
	if connection.WriteJSON(map[string]any{"type": "delivery.ack", "delivery_id": "dlv_socket"}) != nil {
		t.Fatal("write ack")
	}
	if connection.ReadJSON(&frame) != nil || frame["type"] != "delivery.accepted" || message.acks != 1 {
		t.Fatalf("accepted=%v acks=%d", frame, message.acks)
	}
}

func TestRealWebSocketRejectsBinaryFrames(t *testing.T) {
	gateway, _, _ := testGateway(t, 1, 1<<20)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		connection, err := (&websocket.Upgrader{}).Upgrade(response, request, nil)
		if err == nil {
			gateway.Serve(request.Context(), "app_target", connection)
		}
	}))
	defer server.Close()
	endpoint, _ := url.Parse(server.URL)
	endpoint.Scheme = "ws"
	connection, _, err := websocket.DefaultDialer.Dial(endpoint.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	var ready map[string]any
	_ = connection.SetReadDeadline(time.Now().Add(2 * time.Second))
	if connection.ReadJSON(&ready) != nil {
		t.Fatal("missing ready")
	}
	if connection.WriteMessage(websocket.BinaryMessage, []byte{0xff}) != nil {
		t.Fatal("write binary")
	}
	_, _, err = connection.ReadMessage()
	var closeErr *websocket.CloseError
	if !errors.As(err, &closeErr) || closeErr.Code != websocket.CloseUnsupportedData {
		t.Fatalf("close=%v", err)
	}
}

func TestRealWebSocketHeartbeatTimeoutClosesWith4408(t *testing.T) {
	gateway, _, _ := testGateway(t, 1, 1<<20)
	gateway.options.PongWait = 50 * time.Millisecond
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		connection, err := (&websocket.Upgrader{}).Upgrade(response, request, nil)
		if err == nil {
			gateway.Serve(request.Context(), "app_target", connection)
		}
	}))
	defer server.Close()
	endpoint, _ := url.Parse(server.URL)
	endpoint.Scheme = "ws"
	connection, _, err := websocket.DefaultDialer.Dial(endpoint.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	var ready map[string]any
	if connection.ReadJSON(&ready) != nil {
		t.Fatal("missing ready")
	}
	_, _, err = connection.ReadMessage()
	var closeErr *websocket.CloseError
	if !errors.As(err, &closeErr) || closeErr.Code != streamprotocol.CloseTimeout {
		t.Fatalf("close=%v", err)
	}
}

func testGateway(t *testing.T, maxInFlight, maxBytes int) (*Gateway, *fakeConsumer, *fakeAssignments) {
	t.Helper()
	consumer := &fakeConsumer{subscription: &fakeSubscription{}}
	assignments := &fakeAssignments{disposition: store.DeliveryAssigned}
	sequence := 0
	gateway, err := New(Options{Consumer: consumer, Assignments: assignments, Now: func() time.Time { return time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC) }, NewID: func(prefix string) (string, error) { sequence++; return prefix + string(rune('a'+sequence)), nil }, AssignmentLease: time.Minute, DrainTimeout: time.Second, MaxInFlight: maxInFlight, MaxInFlightBytes: maxBytes, OutboundQueue: 16, RetryDelay: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return gateway, consumer, assignments
}

func envelope(t *testing.T, deliveryID, eventType string) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"delivery_id": deliveryID, "event": domain.Event{ID: "evt_" + deliveryID, Type: eventType, SourceAppID: "app_source", TargetAppIDs: []string{"app_target"}, Data: json.RawMessage(`{"n":1}`), CreatedAt: time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC)}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func readFrame(t *testing.T, frames <-chan []byte) map[string]any {
	t.Helper()
	select {
	case raw := <-frames:
		var value map[string]any
		if json.Unmarshal(raw, &value) != nil {
			t.Fatalf("bad frame %s", raw)
		}
		return value
	case <-time.After(time.Second):
		t.Fatal("frame timeout")
	}
	return nil
}
func assertFrameType(t *testing.T, frames <-chan []byte, typ string) {
	t.Helper()
	if got := readFrame(t, frames)["type"]; got != typ {
		t.Fatalf("type=%v want=%s", got, typ)
	}
}

type fakeSubscription struct{ drains int }

func (s *fakeSubscription) Drain(context.Context) error { s.drains++; return nil }

type fakeConsumer struct {
	config       broker.ConsumerConfig
	handler      broker.Handler
	subscription *fakeSubscription
	starts       int
}

func (c *fakeConsumer) Consume(_ context.Context, config broker.ConsumerConfig, handler broker.Handler) (broker.Subscription, error) {
	c.config = config
	c.handler = handler
	c.starts++
	return c.subscription, nil
}
func (c *fakeConsumer) deliver(message broker.Message) { c.handler(context.Background(), message) }

type fakeMessage struct {
	data                  []byte
	acks, nacks, progress int
	delay                 time.Duration
}

func (m *fakeMessage) Data() []byte                   { return m.data }
func (m *fakeMessage) Ack(context.Context) error      { m.acks++; return nil }
func (m *fakeMessage) Nack(delay time.Duration) error { m.nacks++; m.delay = delay; return nil }
func (m *fakeMessage) Progress() error                { m.progress++; return nil }

type fakeAssignments struct {
	mu                                       sync.Mutex
	disposition                              store.DeliveryAssignmentDisposition
	assignCalls, acked, released, progressed int
	lastApp, lastConnection, lastToken       string
	assignEntered, continueAssign            chan struct{}
}

func (s *fakeAssignments) AssignStreamDelivery(_ context.Context, id, app, conn, token string, now time.Time, lease, _ time.Duration) (store.DeliveryAssignment, store.DeliveryAssignmentDisposition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.assignCalls++
	s.lastApp, s.lastConnection, s.lastToken = app, conn, token
	entered, proceed := s.assignEntered, s.continueAssign
	if entered != nil {
		close(entered)
		s.assignEntered = nil
	}
	s.mu.Unlock()
	if proceed != nil {
		<-proceed
	}
	s.mu.Lock()
	if s.disposition != store.DeliveryAssigned {
		return store.DeliveryAssignment{}, s.disposition, nil
	}
	return store.DeliveryAssignment{DeliveryID: id, TargetAppID: app, ConnectionID: conn, Token: token, Attempt: 1, ExpiresAt: now.Add(lease)}, s.disposition, nil
}
func (s *fakeAssignments) AcknowledgeStreamDelivery(context.Context, string, string, string, string, time.Time) error {
	s.acked++
	return nil
}
func (s *fakeAssignments) ReleaseStreamDelivery(context.Context, string, string, string, string, time.Time) error {
	s.released++
	return nil
}
func (s *fakeAssignments) ProgressStreamDelivery(context.Context, string, string, string, string, time.Time, time.Duration) error {
	s.progressed++
	return nil
}
