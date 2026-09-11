package realtime

import (
	"encoding/json"
	"sync"
	"time"

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
	appID, id string
	hub       *Hub
	outbound  chan []byte
	controls  chan controlFrame
	done      chan struct{}
	once      sync.Once
	mu        sync.Mutex
	conn      *websocket.Conn
}

func (s *Session) AppID() string         { return s.appID }
func (s *Session) ID() string            { return s.id }
func (s *Session) Frames() <-chan []byte { return s.outbound }
func (s *Session) Done() <-chan struct{} { return s.done }
func (s *Session) Close() {
	s.once.Do(func() {
		close(s.done)
		s.mu.Lock()
		if s.conn != nil {
			_ = s.conn.Close()
		}
		s.mu.Unlock()
		s.hub.mu.Lock()
		if _, ok := s.hub.sessions[s]; ok {
			delete(s.hub.sessions, s)
			observability.WebSocketConnections.Dec()
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
		written := make(chan struct{})
		select {
		case s.controls <- controlFrame{kind: websocket.CloseMessage, data: websocket.FormatCloseMessage(code, ""), written: written}:
		default:
			return &websocket.CloseError{Code: code}
		}
		timer := time.NewTimer(WriteTimeout)
		defer timer.Stop()
		select {
		case <-written:
		case <-s.done:
		case <-timer.C:
		}
		return &websocket.CloseError{Code: code}
	})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); defer s.Close(); s.readLoop(conn) }()
	go func() { defer wg.Done(); defer s.Close(); s.writeLoop(conn) }()
	wg.Wait()
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
		frame, pe := DecodeClientFrame(raw)
		if pe != nil {
			s.Send(ErrorFrame(pe))
			continue
		}
		switch frame.Type {
		case "subscribe":
			if pe = s.hub.Subscribe(s, frame.Topics); pe != nil {
				s.Send(ErrorFrame(pe))
			} else {
				s.Send(ServerFrame{Type: "subscribed", Topics: frame.Topics})
			}
		case "ping":
			s.Send(ServerFrame{Type: "pong"})
		case "rpc.result":
			s.Send(ErrorFrame(s.hub.HandleResult(s, frame)))
		}
	}
}
func (s *Session) writeLoop(conn *websocket.Conn) {
	ticker := time.NewTicker(PingInterval)
	defer ticker.Stop()
	for {
		select {
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
