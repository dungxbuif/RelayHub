package realtime

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/observability"
	"github.com/dungxbuif/RelayHub/internal/redisstate"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const WriteTimeout = 10 * time.Second
const PongTimeout = 60 * time.Second
const PingInterval = 25 * time.Second

type controlFrame struct {
	kind    int
	data    []byte
	written chan struct{}
}

// Session has one bounded application queue. Serve owns exactly one reader and
// one writer goroutine; Close may run concurrently and never closes the queues.
type Session struct {
	appID, clientID, id string
	protocol            string
	capabilities        map[string]map[string]bool
	presence            map[string]json.RawMessage
	rewinding           map[string]bool
	rewindBuffer        []ServerFrame
	rewindSeen          map[string]time.Time
	connectedAt         time.Time
	hub                 *Hub
	outbound            chan []byte
	controls            chan controlFrame
	closeRequests       chan controlFrame
	done                chan struct{}
	once                sync.Once
	mu                  sync.Mutex
	conn                *websocket.Conn
}

func (s *Session) AppID() string         { return s.appID }
func (s *Session) ClientID() string      { return s.clientID }
func (s *Session) Protocol() string      { return s.protocol }
func (s *Session) ID() string            { return s.id }
func (s *Session) Frames() <-chan []byte { return s.outbound }
func (s *Session) Done() <-chan struct{} { return s.done }
func (s *Session) allowed(channel, action string) bool {
	if s == nil {
		return false
	}
	for grant, actions := range s.capabilities {
		if actions[action] && domain.RealtimeChannelGrantMatches(grant, channel) {
			return true
		}
	}
	return false
}
func (s *Session) Close() {
	s.once.Do(func() {
		close(s.done)
		s.mu.Lock()
		if s.conn != nil {
			_ = s.conn.Close()
		}
		s.mu.Unlock()
		s.hub.mu.Lock()
		registry, instanceID, generation := s.hub.registry, s.hub.instanceID, s.hub.generation
		presenceStore := s.hub.presenceStore
		leaves := make([]ServerFrame, 0, len(s.presence))
		if topics, ok := s.hub.sessions[s]; ok {
			delete(s.hub.sessions, s)
			for channel := range s.presence {
				occupancy := 0
				for candidate := range s.hub.sessions {
					if candidate.appID == s.appID {
						if _, present := candidate.presence[channel]; present {
							occupancy++
						}
					}
				}
				leaves = append(leaves, ServerFrame{Type: "presence.leave", AppID: s.appID, Channel: channel, PublisherClientID: s.clientID, PublisherConnectionID: s.id, MessageID: "msg_" + uuid.NewString(), PublishedAt: time.Now().UTC().Format(time.RFC3339Nano), Occupancy: occupancy})
			}
			observability.WebSocketConnections.Dec()
			if topics["functions"] {
				releaseRoutes := s.hub.routes
				for candidate, candidateTopics := range s.hub.sessions {
					if candidate.appID == s.appID && candidateTopics["functions"] {
						releaseRoutes = nil
						break
					}
				}
				if releaseRoutes != nil {
					releaseRoutes.ReleaseFunctionRoute(s.appID)
				}
			}
		}
		s.hub.mu.Unlock()
		for _, frame := range leaves {
			if presenceStore != nil {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				occupancy, err := presenceStore.Delete(ctx, s.presenceRecord(frame.Channel, nil))
				cancel()
				if err == nil {
					frame.Occupancy = occupancy
				}
			}
			_ = s.hub.dispatchV2(s.appID, frame)
		}
		if registry != nil && s.protocol == ProtocolV2 {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = registry.Delete(ctx, s.registryRecord(instanceID, generation, nil))
			cancel()
		}
	})
}

func (s *Session) registryRecord(instanceID string, generation uint64, channels []string) redisstate.RealtimeConnection {
	return redisstate.RealtimeConnection{AppID: s.appID, ClientID: s.clientID, ConnectionID: s.id, InstanceID: instanceID, Generation: generation, Protocol: s.protocol, Channels: append([]string(nil), channels...), ConnectedAt: s.connectedAt, LastSeenAt: time.Now().UTC()}
}

func (s *Session) presenceRecord(channel string, data json.RawMessage) redisstate.RealtimePresence {
	return redisstate.RealtimePresence{AppID: s.appID, Channel: channel, ClientID: s.clientID, ConnectionID: s.id, Data: append(json.RawMessage(nil), data...)}
}

func (s *Session) refreshRegistry(registry *redisstate.RealtimeConnectionStore, instanceID string, generation uint64) {
	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.hub.mu.RLock()
			topics, active := s.hub.sessions[s]
			presenceStore := s.hub.presenceStore
			presence := make(map[string]json.RawMessage, len(s.presence))
			for channel, data := range s.presence {
				presence[channel] = append(json.RawMessage(nil), data...)
			}
			channels := make([]string, 0, len(topics))
			for topic := range topics {
				if channel, ok := strings.CutPrefix(topic, "channel:"); ok {
					channels = append(channels, channel)
				}
			}
			s.hub.mu.RUnlock()
			if !active {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			err := registry.Refresh(ctx, s.registryRecord(instanceID, generation, channels), connectionRegistryTTL)
			cancel()
			if err != nil {
				s.Close()
				return
			}
			if presenceStore != nil {
				for channel, data := range presence {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					_, _, err := presenceStore.Upsert(ctx, s.presenceRecord(channel, data), connectionRegistryTTL)
					cancel()
					if err != nil {
						s.Close()
						return
					}
				}
			}
		}
	}
}
func (s *Session) Send(frame ServerFrame) bool {
	raw, err := json.Marshal(frame)
	if err != nil {
		return false
	}
	select {
	case <-s.done:
		return false
	default:
	}
	select {
	case s.outbound <- raw:
		return true
	default:
		observability.WebSocketSlowClients.Inc()
		s.Close()
		return false
	}
}
func (s *Session) Serve(conn *websocket.Conn) {
	s.mu.Lock()
	s.conn = conn
	s.mu.Unlock()
	select {
	case <-s.done:
		_ = conn.Close()
		return
	default:
	}
	conn.SetReadLimit(MaxInboundBytes)
	_ = conn.SetReadDeadline(time.Now().Add(PongTimeout))
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(PongTimeout)) })
	conn.SetPingHandler(func(data string) error {
		select {
		case s.controls <- controlFrame{kind: websocket.PongMessage, data: []byte(data)}:
		default:
			s.Close()
		}
		return nil
	})
	conn.SetCloseHandler(func(code int, text string) error {
		s.writeClose(code)
		return &websocket.CloseError{Code: code}
	})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); defer s.Close(); s.readLoop(conn) }()
	go func() { defer wg.Done(); defer s.Close(); s.writeLoop(conn) }()
	wg.Wait()
}

// writeClose reserves a dedicated slot unaffected by a full Pong queue. The
// sole reader requests a close and waits for the writer or bounded shutdown.
func (s *Session) writeClose(code int) {
	written := make(chan struct{})
	timer := time.NewTimer(WriteTimeout)
	defer timer.Stop()
	select {
	case s.closeRequests <- controlFrame{kind: websocket.CloseMessage, data: websocket.FormatCloseMessage(code, ""), written: written}:
	case <-s.done:
		return
	case <-timer.C:
		return
	}
	select {
	case <-written:
	case <-s.done:
	case <-timer.C:
	}
}

func (s *Session) readLoop(conn *websocket.Conn) {
	for {
		kind, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if kind != websocket.TextMessage {
			s.Send(ErrorFrame(protocolError("invalid_frame", "Use JSON text frames.")))
			continue
		}
		// ReadMessage reassembles fragments. UTF-8 code points may cross frame
		// boundaries, so validate the full text before JSON can replace bad bytes.
		if !utf8.Valid(raw) {
			s.writeClose(websocket.CloseInvalidFramePayloadData)
			return
		}
		var frame ClientFrame
		var pe *ProtocolError
		if s.protocol == ProtocolV2 {
			frame, pe = DecodeClientFrameV2(raw)
		} else {
			frame, pe = DecodeClientFrame(raw)
		}
		if pe != nil {
			s.Send(ErrorFrame(pe))
			continue
		}
		switch frame.Type {
		case "subscribe":
			if s.protocol == ProtocolV2 {
				if frame.Rewind != nil {
					pe = s.hub.SubscribeRewindV2(s, frame.Channels)
				} else {
					pe = s.hub.SubscribeV2(s, frame.Channels)
				}
			} else {
				pe = s.hub.Subscribe(s, frame.Topics)
			}
			if pe != nil {
				s.Send(ErrorFrame(pe))
			} else if frame.Rewind != nil {
				pages := make([]ServerFrame, 0, len(frame.Channels))
				for _, channel := range frame.Channels {
					var history ServerFrame
					history, pe = s.hub.HistoryV2(s, channel, frame.Rewind.Cursor, frame.Rewind.Limit)
					if pe != nil {
						s.Send(ErrorFrame(pe))
						break
					}
					s.Send(history)
					pages = append(pages, history)
				}
				s.Send(ServerFrame{Type: "subscribed", Topics: frame.Topics, Channels: frame.Channels})
				s.hub.FinishRewindV2(s, pages)
			} else {
				s.Send(ServerFrame{Type: "subscribed", Topics: frame.Topics, Channels: frame.Channels})
			}
		case "unsubscribe":
			if pe = s.hub.UnsubscribeV2(s, frame.Channels); pe != nil {
				s.Send(ErrorFrame(pe))
			} else {
				s.Send(ServerFrame{Type: "unsubscribed", Channels: frame.Channels})
			}
		case "channel.publish":
			if pe = s.hub.PublishV2(s, frame); pe != nil {
				s.Send(ErrorFrame(pe))
			}
		case "channel.publish.batch":
			s.Send(s.hub.PublishBatchV2(s, frame.Items))
		case "history.get":
			var history ServerFrame
			if history, pe = s.hub.HistoryV2(s, frame.Channel, frame.Cursor, frame.Limit); pe != nil {
				s.Send(ErrorFrame(pe))
			} else {
				s.Send(history)
			}
		case "presence.update":
			if pe = s.hub.UpdatePresence(s, frame); pe != nil {
				s.Send(ErrorFrame(pe))
			}
		case "message.action.put":
			if _, pe = s.hub.PutActionV2(s, frame); pe != nil {
				s.Send(ErrorFrame(pe))
			}
		case "message.actions.get":
			var result ServerFrame
			if result, pe = s.hub.ListActionsV2(s, frame); pe != nil {
				s.Send(ErrorFrame(pe))
			} else {
				s.Send(result)
			}
		case "message.action.remove":
			if _, pe = s.hub.RemoveActionV2(s, frame); pe != nil {
				s.Send(ErrorFrame(pe))
			}
		case "file.publish":
			if pe = s.hub.PublishFileV2(s, frame); pe != nil {
				s.Send(ErrorFrame(pe))
			}
		case "ping":
			s.Send(ServerFrame{Type: "pong"})
		case "rpc.result":
			if pe := s.hub.HandleResult(s, frame); pe != nil {
				s.Send(ErrorFrame(pe))
			}
		}
	}
}
func (s *Session) writeLoop(conn *websocket.Conn) {
	ticker := time.NewTicker(PingInterval)
	defer ticker.Stop()
	finishClose := func(request controlFrame) {
		_ = conn.WriteControl(websocket.CloseMessage, request.data, time.Now().Add(WriteTimeout))
		close(request.written)
	}
	for {
		// A close already queued when the current write finishes takes precedence
		// over all buffered Pongs and application frames, regardless of queue load.
		select {
		case request := <-s.closeRequests:
			finishClose(request)
			return
		default:
		}
		select {
		case request := <-s.closeRequests:
			finishClose(request)
			return
		case <-s.done:
			return
		case raw := <-s.outbound:
			_ = conn.SetWriteDeadline(time.Now().Add(WriteTimeout))
			if conn.WriteMessage(websocket.TextMessage, raw) != nil {
				return
			}
		case control := <-s.controls:
			err := conn.WriteControl(control.kind, control.data, time.Now().Add(WriteTimeout))
			if control.written != nil {
				close(control.written)
			}
			if err != nil || control.kind == websocket.CloseMessage {
				return
			}
		case <-ticker.C:
			if conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(WriteTimeout)) != nil {
				return
			}
		}
	}
}
