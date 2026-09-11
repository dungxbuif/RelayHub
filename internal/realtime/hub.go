package realtime

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/observability"
	"github.com/google/uuid"
)

// Hub owns local fan-out. Identity is supplied by authenticated server code only.
type Hub struct {
	mu        sync.RWMutex
	sessions  map[*Session]map[string]bool
	closed    bool
	functions FunctionBackend
}

func NewHub() *Hub { return &Hub{sessions: make(map[*Session]map[string]bool)} }
func (h *Hub) Register(appID string) *Session {
	s := &Session{appID: appID, id: "conn_" + uuid.NewString(), hub: h, outbound: make(chan []byte, OutboundQueueSize), controls: make(chan controlFrame, OutboundQueueSize), closeRequests: make(chan controlFrame, 1), done: make(chan struct{})}
	h.mu.Lock()
	if h.closed || appID == "" {
		h.mu.Unlock()
		s.Close()
		return s
	}
	h.sessions[s] = map[string]bool{}
	observability.WebSocketConnections.Inc()
	h.mu.Unlock()
	return s
}
func (h *Hub) Disconnect(s *Session) { s.Close() }
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
	for _, topic := range topics {
		subscribed[topic] = true
	}
	return nil
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
func (h *Hub) publish(app, topic string, frame ServerFrame) {
	h.mu.RLock()
	var targets []*Session
	for s, topics := range h.sessions {
		if s.appID == app && topics[topic] {
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

func (h *Hub) SetFunctions(backend FunctionBackend) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.functions = backend
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
