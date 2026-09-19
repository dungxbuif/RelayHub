package realtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	natsbroker "github.com/dungxbuif/RelayHub/internal/broker/nats"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/google/uuid"
	gonats "github.com/nats-io/nats.go"
)

var ErrNATSFunctionUnavailable = errors.New("NATS function responder unavailable")

type natsObservation struct {
	AppID string      `json:"app_id"`
	Frame ServerFrame `json:"frame"`
}

type natsChannelObservation struct {
	Channel        string          `json:"channel"`
	PublisherAppID string          `json:"publisher_app_id"`
	Data           json.RawMessage `json:"data"`
}

type natsInvocationRequest struct {
	RequestID  string      `json:"request_id"`
	OwnerAppID string      `json:"owner_app_id"`
	FunctionID string      `json:"function_id"`
	Frame      ServerFrame `json:"frame"`
}

type natsInvocationAcceptance struct {
	RequestID    string `json:"request_id"`
	InvocationID string `json:"invocation_id"`
	Accepted     bool   `json:"accepted"`
}

// Acceptance is monotonic state, independent of the coalescing wakeup channel.
// bridge.mu orders setting accepted against publishing another request.
type pendingInvocationAcceptance struct {
	invocationID string
	accepted     bool
	updates      chan struct{}
}

// NATSBridge transports best-effort observations and live function dispatch.
// PostgreSQL remains authoritative for delivery eligibility and terminal state.
type NATSBridge struct {
	connection   *gonats.Conn
	hub          *Hub
	replySubject string
	observations *gonats.Subscription
	replies      *gonats.Subscription

	mu      sync.Mutex
	routes  map[string]*gonats.Subscription
	pending map[string]*pendingInvocationAcceptance
	watches map[*natsInvocationWatch]struct{}
	closed  bool
	once    sync.Once
}

func NewNATSBridge(ctx context.Context, connection *gonats.Conn, hub *Hub, instanceID string) (*NATSBridge, error) {
	if connection == nil || hub == nil || instanceID == "" || ctx.Err() != nil {
		return nil, ErrNATSFunctionUnavailable
	}
	replySubject, err := natsbroker.ReplySubject(instanceID)
	if err != nil {
		return nil, err
	}
	bridge := &NATSBridge{connection: connection, hub: hub, replySubject: replySubject, routes: make(map[string]*gonats.Subscription), pending: make(map[string]*pendingInvocationAcceptance), watches: make(map[*natsInvocationWatch]struct{})}
	bridge.observations, err = connection.Subscribe("rh.v1.realtime.>", bridge.handleRealtimeMessage)
	if err != nil {
		return nil, ErrNATSFunctionUnavailable
	}
	bridge.replies, err = connection.Subscribe(replySubject, bridge.handleAcceptance)
	if err != nil {
		_ = bridge.observations.Unsubscribe()
		return nil, ErrNATSFunctionUnavailable
	}
	if err := flushNATS(ctx, connection); err != nil {
		_ = bridge.observations.Unsubscribe()
		_ = bridge.replies.Unsubscribe()
		return nil, ErrNATSFunctionUnavailable
	}
	hub.SetFunctionRoutes(bridge)
	if ctx.Done() != nil {
		go func() {
			<-ctx.Done()
			bridge.Close()
		}()
	}
	return bridge, nil
}

func (bridge *NATSBridge) PublishEvent(ctx context.Context, event domain.Event) error {
	for _, appID := range event.TargetAppIDs {
		if err := bridge.publishObservation(ctx, appID, ServerFrame{Type: "event", Event: &event}); err != nil {
			return err
		}
	}
	return nil
}

func (bridge *NATSBridge) PublishJob(ctx context.Context, job domain.Job) error {
	return bridge.publishObservation(ctx, job.TargetAppID, ServerFrame{Type: "job.updated", Job: &job})
}

func (bridge *NATSBridge) PublishChannel(ctx context.Context, message domain.ChannelMessage) {
	if !domain.ValidRealtimeChannel(message.Channel) {
		return
	}
	subject, err := realtimeChannelSubject(message.Channel)
	if err != nil {
		return
	}
	raw, err := json.Marshal(natsChannelObservation{Channel: message.Channel, PublisherAppID: message.PublisherAppID, Data: append(json.RawMessage(nil), message.Data...)})
	if err != nil {
		return
	}
	if err := bridge.connection.Publish(subject, raw); err != nil {
		return
	}
	_ = flushNATS(ctx, bridge.connection)
}

func (bridge *NATSBridge) publishObservation(ctx context.Context, appID string, frame ServerFrame) error {
	subjects, err := natsbroker.SubjectsForApp(appID)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(natsObservation{AppID: appID, Frame: frame})
	if err != nil {
		return err
	}
	if err := bridge.connection.Publish(subjects.Realtime, raw); err != nil {
		return ErrNATSFunctionUnavailable
	}
	if err := flushNATS(ctx, bridge.connection); err != nil {
		return ErrNATSFunctionUnavailable
	}
	return nil
}

func flushNATS(ctx context.Context, connection *gonats.Conn) error {
	flushContext, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	return connection.FlushWithContext(flushContext)
}

func (bridge *NATSBridge) handleRealtimeMessage(message *gonats.Msg) {
	if strings.HasPrefix(message.Subject, "rh.v1.realtime.channel.") {
		bridge.handleChannelObservation(message)
		return
	}
	bridge.handleObservation(message)
}

func (bridge *NATSBridge) handleObservation(message *gonats.Msg) {
	var observation natsObservation
	if json.Unmarshal(message.Data, &observation) != nil {
		return
	}
	subjects, err := natsbroker.SubjectsForApp(observation.AppID)
	if err != nil || subjects.Realtime != message.Subject {
		return
	}
	bridge.hub.Deliver(observation.AppID, observation.Frame)
}

func (bridge *NATSBridge) handleChannelObservation(message *gonats.Msg) {
	var observation natsChannelObservation
	if json.Unmarshal(message.Data, &observation) != nil || !domain.ValidRealtimeChannel(observation.Channel) || !domain.JSONObject(observation.Data) {
		return
	}
	expected, err := realtimeChannelSubject(observation.Channel)
	if err != nil || expected != message.Subject {
		return
	}
	bridge.hub.PublishChannel(context.Background(), domain.ChannelMessage{Channel: observation.Channel, PublisherAppID: observation.PublisherAppID, Data: append(json.RawMessage(nil), observation.Data...)})
}

func realtimeChannelSubject(channel string) (string, error) {
	if !domain.ValidRealtimeChannel(channel) {
		return "", errors.New("invalid realtime channel")
	}
	token, err := natsbroker.AppToken("channel:" + channel)
	if err != nil {
		return "", err
	}
	return "rh.v1.realtime.channel." + token, nil
}

func (bridge *NATSBridge) PublishInvocation(ctx context.Context, invocation domain.Invocation) error {
	subject, err := natsbroker.FunctionSubject(invocation.OwnerAppID, invocation.FunctionID)
	if err != nil {
		return err
	}
	requestID := uuid.NewString()
	request := natsInvocationRequest{RequestID: requestID, OwnerAppID: invocation.OwnerAppID, FunctionID: invocation.FunctionID, Frame: InvocationFrame(invocation)}
	raw, err := json.Marshal(request)
	if err != nil {
		return ErrNATSFunctionUnavailable
	}
	pending := &pendingInvocationAcceptance{invocationID: invocation.ID, updates: make(chan struct{}, 1)}
	bridge.mu.Lock()
	if bridge.closed {
		bridge.mu.Unlock()
		return ErrNATSFunctionUnavailable
	}
	bridge.pending[requestID] = pending
	bridge.mu.Unlock()
	defer func() {
		bridge.mu.Lock()
		delete(bridge.pending, requestID)
		bridge.mu.Unlock()
	}()
	deadline := invocation.ClaimBy
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	for {
		// This check and publication share the acceptance handler's lock. A
		// successful reply already received cannot be overtaken by redispatch.
		bridge.mu.Lock()
		if pending.accepted {
			bridge.mu.Unlock()
			return nil
		}
		if bridge.closed || !time.Now().Before(deadline) {
			bridge.mu.Unlock()
			return ErrNATSFunctionUnavailable
		}
		if err := ctx.Err(); err != nil {
			bridge.mu.Unlock()
			return err
		}
		message := gonats.NewMsg(subject)
		message.Reply = bridge.replySubject
		message.Data = raw
		err := bridge.connection.PublishMsg(message)
		bridge.mu.Unlock()
		if err != nil {
			return ErrNATSFunctionUnavailable
		}
		remaining := time.Until(deadline)
		wait := 20 * time.Millisecond
		if remaining < wait {
			wait = remaining
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-pending.updates:
			timer.Stop()
			bridge.mu.Lock()
			accepted := pending.accepted
			bridge.mu.Unlock()
			if accepted {
				return nil
			}
			backoff := time.NewTimer(min(5*time.Millisecond, time.Until(deadline)))
			select {
			case <-ctx.Done():
				backoff.Stop()
				return ctx.Err()
			case <-backoff.C:
			}
		case <-timer.C:
		}
	}
}

func (bridge *NATSBridge) handleAcceptance(message *gonats.Msg) {
	var acceptance natsInvocationAcceptance
	if json.Unmarshal(message.Data, &acceptance) != nil || acceptance.RequestID == "" {
		return
	}
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	pending := bridge.pending[acceptance.RequestID]
	if pending != nil && pending.invocationID == acceptance.InvocationID {
		pending.accepted = pending.accepted || acceptance.Accepted
		select {
		case pending.updates <- struct{}{}:
		default:
		}
	}
}

func (bridge *NATSBridge) EnsureFunctionRoute(appID string) error {
	appToken, err := natsbroker.AppToken(appID)
	if err != nil {
		return err
	}
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	if bridge.closed {
		return ErrNATSFunctionUnavailable
	}
	if bridge.routes[appID] != nil {
		return nil
	}
	subject := "rh.v1.rpc." + appToken + ".*"
	subscription, err := bridge.connection.QueueSubscribe(subject, "rh_v1_rpc_"+appToken, bridge.handleInvocation)
	if err != nil {
		return ErrNATSFunctionUnavailable
	}
	if err := bridge.connection.FlushTimeout(250 * time.Millisecond); err != nil {
		_ = subscription.Unsubscribe()
		return ErrNATSFunctionUnavailable
	}
	bridge.routes[appID] = subscription
	return nil
}

func (bridge *NATSBridge) ReleaseFunctionRoute(appID string) {
	bridge.mu.Lock()
	subscription := bridge.routes[appID]
	delete(bridge.routes, appID)
	bridge.mu.Unlock()
	if subscription != nil {
		_ = subscription.Unsubscribe()
	}
}

func (bridge *NATSBridge) handleInvocation(message *gonats.Msg) {
	var request natsInvocationRequest
	if json.Unmarshal(message.Data, &request) != nil || request.RequestID == "" || request.Frame.Type != "rpc.invoke" || request.Frame.InvocationID == "" {
		return
	}
	expected, err := natsbroker.FunctionSubject(request.OwnerAppID, request.FunctionID)
	if err != nil || expected != message.Subject || !validRPCReplySubject(message.Reply) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	bridge.hub.mu.RLock()
	reader, ok := bridge.hub.functions.(interface {
		GetInvocation(context.Context, string) (domain.Invocation, error)
	})
	bridge.hub.mu.RUnlock()
	if !ok {
		return
	}
	canonical, err := reader.GetInvocation(ctx, request.Frame.InvocationID)
	if err != nil || canonical.OwnerAppID != request.OwnerAppID || canonical.FunctionID != request.FunctionID {
		return
	}
	frame := InvocationFrame(canonical)
	canonicalWire, marshalErr := json.Marshal(frame)
	requestWire, requestErr := json.Marshal(request.Frame)
	if marshalErr != nil || requestErr != nil || !bytes.Equal(canonicalWire, requestWire) {
		return
	}
	err = bridge.hub.InvokeFunction(ctx, canonical.OwnerAppID, frame)
	cancel()
	response, marshalErr := json.Marshal(natsInvocationAcceptance{RequestID: request.RequestID, InvocationID: request.Frame.InvocationID, Accepted: err == nil})
	if marshalErr == nil {
		_ = bridge.connection.Publish(message.Reply, response)
	}
}

// Result subjects contain only an opaque hash, and messages contain no result
// data. Every waiter independently reads the authoritative persisted invocation.
func invocationResultSubject(id string) (string, error) {
	token, err := natsbroker.AppToken(id)
	if err != nil {
		return "", err
	}
	return "rh.v1.rpc.result." + token, nil
}

func (bridge *NATSBridge) PublishInvocationResult(ctx context.Context, id string) error {
	subject, err := invocationResultSubject(id)
	if err != nil {
		return err
	}
	if err := bridge.connection.Publish(subject, nil); err != nil {
		return err
	}
	return flushNATS(ctx, bridge.connection)
}

type natsInvocationWatch struct {
	bridge       *NATSBridge
	subscription *gonats.Subscription
	updates      chan struct{}
	mu           sync.Mutex
	closed       bool
	done         chan struct{}
	stop         func() bool
}

func (watch *natsInvocationWatch) Updates() <-chan struct{} { return watch.updates }
func (watch *natsInvocationWatch) signal() {
	watch.mu.Lock()
	defer watch.mu.Unlock()
	if !watch.closed {
		select {
		case watch.updates <- struct{}{}:
		default:
		}
	}
}
func (watch *natsInvocationWatch) Close() {
	watch.mu.Lock()
	if watch.closed {
		done := watch.done
		watch.mu.Unlock()
		<-done
		return
	}
	watch.closed = true
	if watch.stop != nil {
		watch.stop()
	}
	watch.mu.Unlock()
	_ = watch.subscription.Unsubscribe()
	watch.bridge.mu.Lock()
	delete(watch.bridge.watches, watch)
	watch.bridge.mu.Unlock()
	watch.mu.Lock()
	close(watch.updates)
	close(watch.done)
	watch.mu.Unlock()
}

func (bridge *NATSBridge) WatchInvocation(ctx context.Context, id string) (store.InvocationWatch, error) {
	subject, err := invocationResultSubject(id)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	watch := &natsInvocationWatch{bridge: bridge, updates: make(chan struct{}, 1), done: make(chan struct{})}
	bridge.mu.Lock()
	if bridge.closed {
		bridge.mu.Unlock()
		return nil, ErrNATSFunctionUnavailable
	}
	watch.subscription, err = bridge.connection.Subscribe(subject, func(_ *gonats.Msg) { watch.signal() })
	if err == nil {
		bridge.watches[watch] = struct{}{}
	}
	bridge.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if err := flushNATS(ctx, bridge.connection); err != nil {
		watch.Close()
		return nil, err
	}
	watch.mu.Lock()
	watch.stop = context.AfterFunc(ctx, watch.Close)
	watch.mu.Unlock()
	return watch, nil
}

func validRPCReplySubject(subject string) bool {
	const prefix = "rh.v1.rpc.reply."
	if !strings.HasPrefix(subject, prefix) || len(subject) != len(prefix)+32 {
		return false
	}
	for _, character := range subject[len(prefix):] {
		if (character < 'a' || character > 'z') && (character < '2' || character > '7') {
			return false
		}
	}
	return true
}

func (bridge *NATSBridge) Close() {
	bridge.once.Do(func() {
		bridge.mu.Lock()
		bridge.closed = true
		routes := make([]*gonats.Subscription, 0, len(bridge.routes))
		for _, subscription := range bridge.routes {
			routes = append(routes, subscription)
		}
		bridge.routes = make(map[string]*gonats.Subscription)
		watches := make([]*natsInvocationWatch, 0, len(bridge.watches))
		for watch := range bridge.watches {
			watches = append(watches, watch)
		}
		for _, pending := range bridge.pending {
			select {
			case pending.updates <- struct{}{}:
			default:
			}
		}
		bridge.mu.Unlock()
		for _, watch := range watches {
			watch.Close()
		}
		for _, subscription := range routes {
			_ = subscription.Unsubscribe()
		}
		if bridge.observations != nil {
			_ = bridge.observations.Unsubscribe()
		}
		if bridge.replies != nil {
			_ = bridge.replies.Unsubscribe()
		}
	})
}
