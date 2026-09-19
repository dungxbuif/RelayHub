package realtime

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/observability"
	"github.com/dungxbuif/RelayHub/internal/redisstate"
	"github.com/google/uuid"
)

// Hub owns local fan-out. Identity is supplied by authenticated server code only.
type Hub struct {
	mu            sync.RWMutex
	sessions      map[*Session]map[string]bool
	closed        bool
	functions     FunctionBackend
	routes        FunctionRouteManager
	v2            V2Publisher
	registry      *redisstate.RealtimeConnectionStore
	presenceStore *redisstate.RealtimePresenceStore
	instanceID    string
	generation    uint64
}

const connectionRegistryTTL = 75 * time.Second

func NewHub() *Hub { return &Hub{sessions: make(map[*Session]map[string]bool)} }
func (h *Hub) ConnectionCount() int64 {
	if h == nil {
		return 0
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return int64(len(h.sessions))
}
func (h *Hub) Register(appID string) *Session {
	return h.register(appID, "", "", nil)
}

func (h *Hub) RegisterV2(appID, clientID string, capabilities map[string][]string) *Session {
	return h.register(appID, ProtocolV2, clientID, capabilities)
}

func (h *Hub) register(appID, protocol, clientID string, capabilities map[string][]string) *Session {
	now := time.Now().UTC()
	s := &Session{appID: appID, clientID: clientID, protocol: protocol, capabilities: copyCapabilities(capabilities), presence: make(map[string]json.RawMessage), id: "conn_" + uuid.NewString(), connectedAt: now, hub: h, outbound: make(chan []byte, OutboundQueueSize), controls: make(chan controlFrame, OutboundQueueSize), closeRequests: make(chan controlFrame, 1), done: make(chan struct{})}
	h.mu.Lock()
	if h.closed || appID == "" || protocol == ProtocolV2 && clientID == "" {
		h.mu.Unlock()
		s.Close()
		return s
	}
	h.sessions[s] = map[string]bool{}
	observability.WebSocketConnections.Inc()
	registry, instanceID, generation := h.registry, h.instanceID, h.generation
	h.mu.Unlock()
	if protocol == ProtocolV2 && registry != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := registry.Put(ctx, s.registryRecord(instanceID, generation, nil), connectionRegistryTTL)
		cancel()
		if err != nil {
			s.Close()
			return s
		}
		go s.refreshRegistry(registry, instanceID, generation)
	}
	return s
}

func (h *Hub) SetConnectionRegistry(registry *redisstate.RealtimeConnectionStore, instanceID string, generation uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.registry, h.instanceID, h.generation = registry, instanceID, generation
}

func (h *Hub) SetPresenceStore(store *redisstate.RealtimePresenceStore) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.presenceStore = store
}

func copyCapabilities(source map[string][]string) map[string]map[string]bool {
	if len(source) == 0 {
		return nil
	}
	result := make(map[string]map[string]bool, len(source))
	for channel, actions := range source {
		result[channel] = make(map[string]bool, len(actions))
		for _, action := range actions {
			result[channel][action] = true
		}
	}
	return result
}
func (h *Hub) Disconnect(s *Session) { s.Close() }
func (h *Hub) DisconnectConnection(appID, connectionID string) bool {
	h.mu.RLock()
	var target *Session
	for session := range h.sessions {
		if session.appID == appID && session.id == connectionID {
			target = session
			break
		}
	}
	h.mu.RUnlock()
	if target == nil {
		return false
	}
	target.Close()
	return true
}
func (h *Hub) Subscribe(s *Session, topics []string) *ProtocolError {
	if err := validateTopics(topics); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	subscribed, ok := h.sessions[s]
	if !ok {
		return protocolError("connection_closed", "Connection is closed.")
	}
	needsFunctionRoute := false
	for _, topic := range topics {
		if topic == "functions" && !subscribed[topic] {
			needsFunctionRoute = true
			for candidate, candidateTopics := range h.sessions {
				if candidate != s && candidate.appID == s.appID && candidateTopics["functions"] {
					needsFunctionRoute = false
					break
				}
			}
		}
	}
	if needsFunctionRoute {
		if h.routes != nil && h.routes.EnsureFunctionRoute(s.appID) != nil {
			return protocolError("function_unavailable", "Function routing is unavailable.")
		}
	}
	for _, topic := range topics {
		subscribed[topic] = true
	}
	return nil
}

func (h *Hub) SubscribeV2(s *Session, channels []string) *ProtocolError {
	if err := validateChannels(channels); err != nil {
		return err
	}
	h.mu.Lock()
	subscribed, ok := h.sessions[s]
	if !ok || s.protocol != ProtocolV2 {
		h.mu.Unlock()
		return protocolError("connection_closed", "Connection is closed.")
	}
	for _, channel := range channels {
		if !s.allowed(channel, "subscribe") {
			h.mu.Unlock()
			return protocolError("forbidden", "The token does not allow subscribing to this channel.")
		}
	}
	for _, channel := range channels {
		subscribed["channel:"+channel] = true
	}
	h.mu.Unlock()
	if !h.persistChannels(s) {
		return protocolError("realtime_unavailable", "Connection registry is temporarily unavailable.")
	}
	return nil
}

func (h *Hub) UnsubscribeV2(s *Session, channels []string) *ProtocolError {
	if err := validateChannels(channels); err != nil {
		return err
	}
	h.mu.Lock()
	subscribed, ok := h.sessions[s]
	if !ok || s.protocol != ProtocolV2 {
		h.mu.Unlock()
		return protocolError("connection_closed", "Connection is closed.")
	}
	for _, channel := range channels {
		if !s.allowed(channel, "subscribe") {
			h.mu.Unlock()
			return protocolError("forbidden", "The token does not allow this channel.")
		}
	}
	for _, channel := range channels {
		delete(subscribed, "channel:"+channel)
	}
	h.mu.Unlock()
	if !h.persistChannels(s) {
		return protocolError("realtime_unavailable", "Connection registry is temporarily unavailable.")
	}
	return nil
}

func (h *Hub) persistChannels(s *Session) bool {
	h.mu.RLock()
	topics, active := h.sessions[s]
	registry, instanceID, generation := h.registry, h.instanceID, h.generation
	channels := make([]string, 0, len(topics))
	for topic := range topics {
		if channel, ok := strings.CutPrefix(topic, "channel:"); ok {
			channels = append(channels, channel)
		}
	}
	h.mu.RUnlock()
	if !active || registry == nil || s.protocol != ProtocolV2 {
		return active
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err := registry.UpdateChannels(ctx, s.registryRecord(instanceID, generation, channels), channels, connectionRegistryTTL)
	cancel()
	if err != nil {
		s.Close()
		return false
	}
	return true
}

func (h *Hub) PublishV2(source *Session, request ClientFrame) *ProtocolError {
	if request.Type != "channel.publish" || !domain.ValidRealtimeChannel(request.Channel) || !domain.JSONObject(request.Data) {
		return protocolError("invalid_publish", "Publish requires a valid channel and JSON object data.")
	}
	if err := validateAudience(request.Audience); err != nil {
		return err
	}
	if !source.allowed(request.Channel, "publish") {
		return protocolError("forbidden", "The token does not allow publishing to this channel.")
	}
	audience := Audience{Type: "all"}
	if request.Audience != nil {
		audience = *request.Audience
	}
	h.mu.RLock()
	if _, active := h.sessions[source]; !active || source.protocol != ProtocolV2 {
		h.mu.RUnlock()
		return protocolError("connection_closed", "Connection is closed.")
	}
	publisher := h.v2
	h.mu.RUnlock()
	frameAudience := audience
	frame := ServerFrame{
		Type:                  "channel.message",
		AppID:                 source.appID,
		Channel:               request.Channel,
		PublisherAppID:        source.appID,
		PublisherClientID:     source.clientID,
		PublisherConnectionID: source.id,
		MessageID:             "msg_" + uuid.NewString(),
		PublishedAt:           time.Now().UTC().Format(time.RFC3339Nano),
		Audience:              &frameAudience,
		Data:                  append([]byte(nil), request.Data...),
	}
	if publisher != nil {
		if err := publisher.PublishRealtimeV2(context.Background(), source.appID, frame); err != nil {
			return protocolError("realtime_unavailable", "Realtime routing is temporarily unavailable.")
		}
		return nil
	}
	delivered := h.DeliverV2(source.appID, frame)
	if (audience.Type == "connection" || audience.Type == "client") && delivered == 0 {
		return protocolError("target_not_found", "No subscribed target exists in this application.")
	}
	return nil
}

func (h *Hub) UpdatePresence(source *Session, request ClientFrame) *ProtocolError {
	if request.Type != "presence.update" || !domain.ValidRealtimeChannel(request.Channel) || !domain.JSONObject(request.Data) {
		return protocolError("invalid_presence", "Presence requires a valid channel and JSON object data.")
	}
	if !source.allowed(request.Channel, "presence") {
		return protocolError("forbidden", "The token does not allow presence on this channel.")
	}
	h.mu.Lock()
	topics, active := h.sessions[source]
	if !active || source.protocol != ProtocolV2 {
		h.mu.Unlock()
		return protocolError("connection_closed", "Connection is closed.")
	}
	if !topics["channel:"+request.Channel] {
		h.mu.Unlock()
		return protocolError("not_subscribed", "Subscribe to the channel before updating presence.")
	}
	_, joined := source.presence[request.Channel]
	source.presence[request.Channel] = append(json.RawMessage(nil), request.Data...)
	occupancy := 0
	for session := range h.sessions {
		if session.appID == source.appID {
			if _, present := session.presence[request.Channel]; present {
				occupancy++
			}
		}
	}
	presenceStore := h.presenceStore
	h.mu.Unlock()
	if presenceStore != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		redisJoined, redisOccupancy, err := presenceStore.Upsert(ctx, source.presenceRecord(request.Channel, request.Data), connectionRegistryTTL)
		cancel()
		if err != nil {
			source.Close()
			return protocolError("realtime_unavailable", "Presence state is temporarily unavailable.")
		}
		joined, occupancy = !redisJoined, redisOccupancy
	}
	frameType := "presence.join"
	if joined {
		frameType = "presence.update"
	}
	frame := ServerFrame{Type: frameType, AppID: source.appID, Channel: request.Channel, PublisherClientID: source.clientID, PublisherConnectionID: source.id, MessageID: "msg_" + uuid.NewString(), PublishedAt: time.Now().UTC().Format(time.RFC3339Nano), Occupancy: occupancy, Data: append(json.RawMessage(nil), request.Data...)}
	if err := h.dispatchV2(source.appID, frame); err != nil {
		return protocolError("realtime_unavailable", "Realtime routing is temporarily unavailable.")
	}
	return nil
}

func (h *Hub) dispatchV2(appID string, frame ServerFrame) error {
	h.mu.RLock()
	publisher := h.v2
	h.mu.RUnlock()
	if publisher != nil {
		return publisher.PublishRealtimeV2(context.Background(), appID, frame)
	}
	h.DeliverV2(appID, frame)
	return nil
}

// V2Publisher routes a server-authenticated envelope across gateway replicas.
type V2Publisher interface {
	PublishRealtimeV2(context.Context, string, ServerFrame) error
}

func (h *Hub) SetV2Publisher(publisher V2Publisher) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.v2 = publisher
}

// DeliverV2 performs only local fan-out. The app argument is derived from the
// authenticated NATS subject and must match the envelope before delivery.
func (h *Hub) DeliverV2(app string, frame ServerFrame) int {
	if !validV2Delivery(app, frame) {
		return 0
	}
	topic := "channel:" + frame.Channel
	h.mu.RLock()
	targets := make([]*Session, 0)
	for target, topics := range h.sessions {
		if target.protocol != ProtocolV2 || target.appID != app || !topics[topic] {
			continue
		}
		if frame.Type != "channel.message" {
			targets = append(targets, target)
			continue
		}
		switch frame.Audience.Type {
		case "all":
			targets = append(targets, target)
		case "others":
			if target.id != frame.PublisherConnectionID {
				targets = append(targets, target)
			}
		case "connection":
			if target.id == frame.Audience.ConnectionID {
				targets = append(targets, target)
			}
		case "client":
			if target.clientID == frame.Audience.ClientID {
				targets = append(targets, target)
			}
		}
	}
	h.mu.RUnlock()
	for _, target := range targets {
		target.Send(frame)
	}
	return len(targets)
}

func validV2Delivery(app string, frame ServerFrame) bool {
	if frame.AppID != app || !domain.ValidRealtimeChannel(frame.Channel) {
		return false
	}
	switch frame.Type {
	case "channel.message":
		return frame.Audience != nil && domain.JSONObject(frame.Data)
	case "presence.join", "presence.update":
		return domain.JSONObject(frame.Data) && frame.PublisherClientID != "" && frame.PublisherConnectionID != ""
	case "presence.leave":
		return frame.PublisherClientID != "" && frame.PublisherConnectionID != ""
	default:
		return false
	}
}
func (h *Hub) PublishEvent(_ context.Context, e domain.Event) error {
	for _, target := range e.TargetAppIDs {
		h.publish(target, "events", ServerFrame{Type: "event", Event: &e})
	}
	return nil
}
func (h *Hub) PublishJob(_ context.Context, j domain.Job) error {
	h.publish(j.TargetAppID, "jobs", ServerFrame{Type: "job.updated", Job: &j})
	return nil
}

type ChannelMessage = domain.ChannelMessage

func (h *Hub) PublishChannel(_ context.Context, message ChannelMessage) {
	if !domain.ValidRealtimeChannel(message.Channel) {
		return
	}
	h.publish("", "channel:"+message.Channel, ServerFrame{Type: "channel.message", Channel: message.Channel, PublisherAppID: message.PublisherAppID, Data: append([]byte(nil), message.Data...)})
}

func (h *Hub) publish(app, topic string, frame ServerFrame) {
	h.mu.RLock()
	var targets []*Session
	for s, topics := range h.sessions {
		if (app == "" || s.appID == app) && topics[topic] {
			targets = append(targets, s)
		}
	}
	h.mu.RUnlock()
	for _, s := range targets {
		s.Send(frame)
	}
}

// FunctionBackend persists ownership; no local selection is authoritative until
// the shared store reserves and acknowledges this exact connection.
type FunctionBackend interface {
	ClaimInvocation(context.Context, string, string, string) error
	AcknowledgeInvocation(context.Context, string, string, string) error
	ReleaseInvocation(context.Context, string, string, string) error
	CompleteResult(context.Context, string, string, domain.RPCResult) error
}

// FunctionRouteManager joins Core NATS routing only while this gateway has an
// eligible local function handler for the application.
type FunctionRouteManager interface {
	EnsureFunctionRoute(string) error
	ReleaseFunctionRoute(string)
}

func (h *Hub) SetFunctions(backend FunctionBackend) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.functions = backend
}
func (h *Hub) SetFunctionRoutes(routes FunctionRouteManager) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.routes = routes
}
func (h *Hub) FunctionSessions(app string) []*Session {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := []*Session{}
	for s, topics := range h.sessions {
		if s.appID == app && topics["functions"] {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}
func (h *Hub) PublishInvocation(ctx context.Context, v domain.Invocation) error {
	return h.InvokeFunction(ctx, v.OwnerAppID, InvocationFrame(v))
}
func InvocationFrame(v domain.Invocation) ServerFrame {
	f := v.Frame()
	return ServerFrame{Type: f.Type, InvocationID: f.InvocationID, Function: f.Function, Input: f.Input, Deadline: f.Deadline}
}
func (h *Hub) InvokeFunction(ctx context.Context, app string, frame ServerFrame) error {
	h.mu.RLock()
	backend := h.functions
	h.mu.RUnlock()
	if backend == nil {
		return protocolError("function_unavailable", "No function handler is available.")
	}
	ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	for _, s := range h.FunctionSessions(app) {
		select {
		case <-s.Done():
			continue
		default:
		}
		if backend.ClaimInvocation(ctx, s.AppID(), s.ID(), frame.InvocationID) != nil {
			continue
		}
		release := func() {
			releaseCtx, stop := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer stop()
			_ = backend.ReleaseInvocation(releaseCtx, s.AppID(), s.ID(), frame.InvocationID)
		}
		select {
		case <-s.Done():
			release()
			continue
		default:
		}
		if backend.AcknowledgeInvocation(ctx, s.AppID(), s.ID(), frame.InvocationID) != nil {
			release()
			continue
		}
		// Acknowledgement precedes enqueue so a fast handler can complete immediately.
		// Failed enqueue cannot have reached the handler and may safely release.
		if s.Send(frame) {
			return nil
		}
		release()
	}
	return protocolError("function_unavailable", "No function handler is available.")
}
func (h *Hub) HandleResult(s *Session, frame ClientFrame) *ProtocolError {
	h.mu.RLock()
	backend := h.functions
	_, active := h.sessions[s]
	h.mu.RUnlock()
	if backend == nil {
		return protocolError("rpc_unavailable", "Function result handling is not configured.")
	}
	if !active || frame.OK == nil {
		return protocolError("invalid_rpc_result", "Invocation result was not accepted.")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	r := domain.RPCResult{InvocationID: frame.InvocationID, OK: *frame.OK, Result: frame.Result, Error: frame.Error}
	if backend.CompleteResult(ctx, s.AppID(), s.ID(), r) != nil {
		return protocolError("invalid_rpc_result", "Invocation result was not accepted.")
	}
	return nil
}
func (h *Hub) Close() {
	h.mu.Lock()
	h.closed = true
	var sessions []*Session
	for s := range h.sessions {
		sessions = append(sessions, s)
	}
	h.mu.Unlock()
	for _, s := range sessions {
		s.Close()
	}
}

// Deliver accepts one application-scoped bridge notification. The original event
// payload is preserved; routing never expands to other targets in that payload.
func (h *Hub) Deliver(app string, frame ServerFrame) {
	switch frame.Type {
	case "event":
		if frame.Event != nil {
			for _, target := range frame.Event.TargetAppIDs {
				if target == app {
					h.publish(app, "events", frame)
					return
				}
			}
		}
	case "job.updated":
		if frame.Job != nil && frame.Job.TargetAppID == app {
			h.publish(app, "jobs", frame)
		}
	}
}
