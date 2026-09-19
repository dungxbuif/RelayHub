package streamgateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/dungxbuif/RelayHub/internal/broker"
	natsbroker "github.com/dungxbuif/RelayHub/internal/broker/nats"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/dungxbuif/RelayHub/internal/streamprotocol"
)

type outboxEnvelope struct {
	DeliveryID string       `json:"delivery_id"`
	Event      domain.Event `json:"event"`
}

type inflightDelivery struct {
	assignment store.DeliveryAssignment
	message    broker.Message
	bytes      int
}

type Session struct {
	gateway      *Gateway
	appID        string
	connectionID string
	ctx          context.Context
	cancel       context.CancelFunc
	frames       chan []byte
	done         chan struct{}
	closeOnce    sync.Once
	closeErr     error
	assignments  sync.WaitGroup

	mu            sync.Mutex
	started       bool
	maxInFlight   int
	topics        map[string]bool
	subscription  broker.Subscription
	inflight      map[string]*inflightDelivery
	inflightBytes int
	reserved      int
	reservedBytes int
	closing       bool
}

func (session *Session) ID() string            { return session.connectionID }
func (session *Session) Frames() <-chan []byte { return session.frames }
func (session *Session) Done() <-chan struct{} { return session.done }

func (session *Session) Handle(ctx context.Context, raw []byte) *streamprotocol.ProtocolError {
	frame, protocolErr := streamprotocol.DecodeClientFrame(raw)
	if protocolErr != nil {
		return protocolErr
	}
	switch frame.Type {
	case "consumer.start":
		return session.start(ctx, frame)
	case "delivery.ack", "delivery.nack", "delivery.progress":
		return session.control(ctx, frame)
	case "function.result":
		return protocolError("function_not_assigned", "Function invocation is not assigned to this connection.", false)
	case "ping":
		if !session.enqueue(map[string]any{"type": "pong"}) {
			return protocolError("backpressure", "Session output is full.", true)
		}
	}
	return nil
}

func (session *Session) start(ctx context.Context, frame streamprotocol.ClientFrame) *streamprotocol.ProtocolError {
	session.mu.Lock()
	if session.started {
		session.mu.Unlock()
		return protocolError("consumer_already_started", "Consumer is already started.", false)
	}
	session.started = true
	session.maxInFlight = min(frame.MaxInFlight, session.gateway.options.MaxInFlight)
	session.mu.Unlock()
	subjects, err := natsbroker.SubjectsForApp(session.appID)
	if err != nil {
		session.resetStarted()
		return protocolError("internal_error", "Consumer could not start.", true)
	}
	subscription, err := session.gateway.options.Consumer.Consume(session.ctx, broker.ConsumerConfig{Stream: "RH_DELIVERIES", DurableName: durableName(session.appID), Filter: subjects.Deliveries, MaxPending: session.gateway.options.MaxInFlight}, session.deliver)
	if err != nil {
		session.resetStarted()
		return protocolError("consumer_unavailable", "Consumer is temporarily unavailable.", true)
	}
	session.mu.Lock()
	session.subscription = subscription
	session.mu.Unlock()
	if !session.enqueue(map[string]any{"type": "consumer.started", "consumer": "default", "max_in_flight": session.maxInFlight}) {
		return protocolError("backpressure", "Session output is full.", true)
	}
	return nil
}

func (session *Session) resetStarted() {
	session.mu.Lock()
	session.started = false
	session.mu.Unlock()
}

func (session *Session) deliver(ctx context.Context, message broker.Message) {
	var envelope outboxEnvelope
	decoder := json.NewDecoder(bytes.NewReader(message.Data()))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&envelope) != nil || decoder.Decode(new(any)) != io.EOF || !validEnvelope(envelope, session.appID) {
		_ = message.Nack(session.gateway.options.RetryDelay)
		return
	}
	wire, err := json.Marshal(map[string]any{"type": "event.delivery", "delivery_id": envelope.DeliveryID, "attempt": 1, "event": envelope.Event})
	if err != nil || len(wire) > streamprotocol.MaxMessageBytes || streamprotocol.ValidateServerFrame(wire) != nil {
		_ = message.Nack(session.gateway.options.RetryDelay)
		return
	}
	session.mu.Lock()
	if session.closedLocked() || len(session.inflight)+session.reserved >= session.maxInFlight || session.inflightBytes+session.reservedBytes+len(wire) > session.gateway.options.MaxInFlightBytes {
		session.mu.Unlock()
		_ = message.Nack(session.gateway.options.RetryDelay)
		return
	}
	session.reserved++
	session.reservedBytes += len(wire)
	session.assignments.Add(1)
	session.mu.Unlock()
	defer session.assignments.Done()
	token, tokenErr := session.gateway.options.NewID("asn_")
	if tokenErr != nil {
		session.releaseReservation(len(wire))
		_ = message.Nack(session.gateway.options.RetryDelay)
		return
	}
	assignment, disposition, assignErr := session.gateway.options.Assignments.AssignStreamDelivery(ctx, envelope.DeliveryID, session.appID, session.connectionID, token, session.gateway.options.Now(), session.gateway.options.AssignmentLease, session.gateway.options.MaxProcessing)
	if assignErr != nil {
		session.releaseReservation(len(wire))
		_ = message.Nack(session.gateway.options.RetryDelay)
		return
	}
	switch disposition {
	case store.DeliveryAlreadyComplete:
		session.releaseReservation(len(wire))
		_ = message.Ack(ctx)
		return
	case store.DeliveryAlreadyAssigned:
		session.releaseReservation(len(wire))
		_ = message.Nack(session.gateway.options.RetryDelay)
		return
	case store.DeliveryAssigned:
	default:
		session.releaseReservation(len(wire))
		_ = message.Nack(session.gateway.options.RetryDelay)
		return
	}
	session.mu.Lock()
	session.reserved--
	session.reservedBytes -= len(wire)
	if session.closedLocked() {
		session.mu.Unlock()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), session.gateway.options.DrainTimeout)
		_ = session.gateway.options.Assignments.ReleaseStreamDelivery(cleanupCtx, envelope.DeliveryID, session.appID, session.connectionID, assignment.Token, session.gateway.options.Now())
		cancel()
		_ = message.Nack(session.gateway.options.RetryDelay)
		return
	}
	wire, err = json.Marshal(map[string]any{"type": "event.delivery", "delivery_id": envelope.DeliveryID, "attempt": assignment.Attempt, "event": envelope.Event})
	if err != nil || len(wire) > streamprotocol.MaxMessageBytes {
		session.mu.Unlock()
		_ = session.gateway.options.Assignments.ReleaseStreamDelivery(ctx, envelope.DeliveryID, session.appID, session.connectionID, assignment.Token, session.gateway.options.Now())
		_ = message.Nack(session.gateway.options.RetryDelay)
		return
	}
	session.inflight[envelope.DeliveryID] = &inflightDelivery{assignment: assignment, message: message, bytes: len(wire)}
	session.inflightBytes += len(wire)
	session.mu.Unlock()
	if !session.enqueueRaw(wire) {
		session.release(ctx, envelope.DeliveryID, 0)
	}
}

func (session *Session) releaseReservation(bytes int) {
	session.mu.Lock()
	session.reserved--
	session.reservedBytes -= bytes
	session.mu.Unlock()
}

func validEnvelope(envelope outboxEnvelope, appID string) bool {
	if envelope.DeliveryID == "" || envelope.Event.ID == "" || envelope.Event.Type == "" || envelope.Event.SourceAppID == "" || len(envelope.Event.TargetAppIDs) == 0 || !domain.JSONObject(envelope.Event.Data) {
		return false
	}
	for _, target := range envelope.Event.TargetAppIDs {
		if target == appID {
			return true
		}
	}
	return false
}

func (session *Session) control(ctx context.Context, frame streamprotocol.ClientFrame) *streamprotocol.ProtocolError {
	session.mu.Lock()
	active, ok := session.inflight[frame.DeliveryID]
	session.mu.Unlock()
	if !ok {
		return protocolError("delivery_not_assigned", "Delivery is not assigned to this connection.", false)
	}
	now := session.gateway.options.Now()
	var err error
	state := "acked"
	switch frame.Type {
	case "delivery.ack":
		err = session.gateway.options.Assignments.AcknowledgeStreamDelivery(ctx, frame.DeliveryID, session.appID, session.connectionID, active.assignment.Token, now)
		if err == nil {
			err = active.message.Ack(ctx)
		}
	case "delivery.nack":
		state = "retrying"
		err = session.gateway.options.Assignments.ReleaseStreamDelivery(ctx, frame.DeliveryID, session.appID, session.connectionID, active.assignment.Token, now)
		if err == nil {
			err = active.message.Nack(time.Duration(frame.DelayMS) * time.Millisecond)
		}
	case "delivery.progress":
		state = "progress"
		err = session.gateway.options.Assignments.ProgressStreamDelivery(ctx, frame.DeliveryID, session.appID, session.connectionID, active.assignment.Token, now, session.gateway.options.AssignmentLease)
		if err == nil {
			err = active.message.Progress()
		}
	}
	if err != nil {
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrConflict) {
			return protocolError("delivery_not_assigned", "Delivery is not assigned to this connection.", false)
		}
		return protocolError("internal_error", "Delivery state could not be updated.", true)
	}
	if frame.Type != "delivery.progress" {
		session.remove(frame.DeliveryID)
	}
	if !session.enqueue(map[string]any{"type": "delivery.accepted", "delivery_id": frame.DeliveryID, "state": state}) {
		return protocolError("backpressure", "Session output is full.", true)
	}
	return nil
}

func (session *Session) remove(id string) {
	session.mu.Lock()
	if active := session.inflight[id]; active != nil {
		delete(session.inflight, id)
		session.inflightBytes -= active.bytes
	}
	session.mu.Unlock()
}

func (session *Session) release(ctx context.Context, id string, delay time.Duration) {
	session.mu.Lock()
	active := session.inflight[id]
	if active != nil {
		delete(session.inflight, id)
		session.inflightBytes -= active.bytes
	}
	session.mu.Unlock()
	if active == nil {
		return
	}
	_ = session.gateway.options.Assignments.ReleaseStreamDelivery(ctx, id, session.appID, session.connectionID, active.assignment.Token, session.gateway.options.Now())
	_ = active.message.Nack(delay)
}

func (session *Session) enqueue(frame any) bool {
	raw, err := json.Marshal(frame)
	return err == nil && session.enqueueRaw(raw)
}
func (session *Session) enqueueRaw(raw []byte) bool {
	select {
	case <-session.done:
		return false
	default:
	}
	select {
	case session.frames <- append([]byte(nil), raw...):
		return true
	default:
		return false
	}
}
func (session *Session) closedLocked() bool {
	if session.closing {
		return true
	}
	select {
	case <-session.done:
		return true
	default:
		return false
	}
}

func (session *Session) Close(ctx context.Context) error {
	session.closeOnce.Do(func() {
		session.mu.Lock()
		session.closing = true
		session.mu.Unlock()
		session.cancel()
		assignmentsDone := make(chan struct{})
		go func() {
			session.assignments.Wait()
			close(assignmentsDone)
		}()
		select {
		case <-assignmentsDone:
		case <-ctx.Done():
			session.closeErr = errors.Join(session.closeErr, ctx.Err())
		}
		session.mu.Lock()
		subscription := session.subscription
		ids := make([]string, 0, len(session.inflight))
		for id := range session.inflight {
			ids = append(ids, id)
		}
		session.mu.Unlock()
		if subscription != nil {
			drainCtx, cancel := context.WithTimeout(ctx, session.gateway.options.DrainTimeout)
			session.closeErr = errors.Join(session.closeErr, subscription.Drain(drainCtx))
			cancel()
		}
		for _, id := range ids {
			session.release(ctx, id, 0)
		}
		close(session.done)
		session.gateway.unregister(session)
	})
	return session.closeErr
}

func protocolError(code, message string, retryable bool) *streamprotocol.ProtocolError {
	return &streamprotocol.ProtocolError{Code: code, Message: message, Retryable: retryable}
}
