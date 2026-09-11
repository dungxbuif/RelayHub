package realtime

import (
	"context"
	"sync"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/observability"
	"github.com/google/uuid"
)

// Hub owns local fan-out. Identity is supplied by authenticated server code only.
type Hub struct {
	mu       sync.RWMutex
	sessions map[*Session]map[string]bool
	closed   bool
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

// InvokeFunction and HandleResult reserve the typed extension points for Task 6.
// They deliberately never route requests or results in this release.
func (h *Hub) InvokeFunction(context.Context, string, ServerFrame) error {
	return protocolError("function_unavailable", "Functions are not enabled.")
}
func (h *Hub) HandleResult(*Session, ClientFrame) *ProtocolError {
	return protocolError("rpc_unavailable", "RPC results are not enabled.")
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
