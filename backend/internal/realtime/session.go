package realtime

import (
	"encoding/json"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/dungxbuif/RelayHub/internal/observability"
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
	return s != nil && s.capabilities[channel][action]
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
		if topics, ok := s.hub.sessions[s]; ok {
			delete(s.hub.sessions, s)
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
	})
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
				pe = s.hub.SubscribeV2(s, frame.Channels)
			} else {
				pe = s.hub.Subscribe(s, frame.Topics)
			}
			if pe != nil {
				s.Send(ErrorFrame(pe))
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
