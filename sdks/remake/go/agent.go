package relayhub

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

func (c *Client) socket(ctx context.Context, path string) (*websocket.Conn, error) {
	u := c.url + path
	if strings.HasPrefix(u, "https:") {
		u = "wss:" + strings.TrimPrefix(u, "https:")
	} else {
		u = "ws:" + strings.TrimPrefix(u, "http:")
	}
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second, Subprotocols: []string{"relayhub.v1"}}
	conn, response, err := dialer.DialContext(ctx, u, http.Header{"Authorization": []string{"Bearer " + c.token}})
	if err != nil {
		if response != nil {
			response.Body.Close()
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if response != nil {
			return nil, &Error{response.StatusCode, "connection_rejected"}
		}
		return nil, unavailable()
	}
	if conn.Subprotocol() != "relayhub.v1" {
		conn.Close()
		return nil, invalid("invalid_subprotocol")
	}
	conn.SetReadLimit(maxFrame)
	return conn, nil
}
func send(conn *websocket.Conn, frame Frame) error {
	b, err := EncodeFrame(frame)
	if err != nil {
		return err
	}
	if err = conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return unavailable()
	}
	if err = conn.WriteMessage(websocket.TextMessage, b); err != nil {
		return unavailable()
	}
	return nil
}

// RunAgent delivers at least once: handlers MUST deduplicate business side effects
// using Delivery.Lease.EventID. All generation/attempt fencing is server-owned.
// Zero ProcessingResult ACKs success; Response sends only a final HTTP Response
// (the host must dispatch it to Responses::complete_response). Errors close the
// socket without settlement: the frozen protocol has no NACK.
//
// Canceling ctx stops local admission/reconnect and cancels handler contexts. The
// current session has at most 30 seconds to settle; RunAgent returns ctx.Err().
// shortcut: Gateway can still claim during drain, until the frozen protocol gains
// a stop-claim signal. Unhandled work remains unacked and recoverable.
// Go cannot forcibly stop a handler that ignores its context. Such a handler may
// still perform side effects after disconnect or return; it cannot settle a new session.
func (c *Client) RunAgent(ctx context.Context, handler func(context.Context, Delivery) (ProcessingResult, error)) error {
	if c.agentID == "" || handler == nil {
		return invalid("agent_id_and_handler_required")
	}
	backoff := time.Second
	connected := false
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		conn, err := c.socket(ctx, "/api/v1/socket")
		if err == nil {
			connected = true
			var delivered bool
			delivered, err = c.runSession(ctx, conn, handler)
			if delivered {
				backoff = time.Second
			}
		} else {
			var e *Error
			if !connected || (errors.As(err, &e) && e.Status >= 400 && e.Status < 500 && e.Status != 429) {
				return err
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if backoff < 30*time.Second {
			backoff = min(2*backoff, 30*time.Second)
		}
	}
}

type received struct {
	frame Frame
	err   error
}
type completed struct {
	delivery Delivery
	result   ProcessingResult
	err      error
}

func (c *Client) runSession(ctx context.Context, conn *websocket.Conn, handler func(context.Context, Delivery) (ProcessingResult, error)) (bool, error) {
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer conn.Close()
	finished := make(chan struct{})
	defer close(finished)
	var admissionMu sync.Mutex
	admissionOpen := true
	stopAdmission := context.AfterFunc(ctx, func() {
		admissionMu.Lock()
		admissionOpen = false
		admissionMu.Unlock()
	})
	defer stopAdmission()
	// The hard deadline includes a blocked write and socket close, not just handlers.
	stopWatch := context.AfterFunc(ctx, func() {
		timer := time.NewTimer(30 * time.Second)
		defer timer.Stop()
		select {
		case <-finished:
		case <-timer.C:
			conn.Close()
		}
	})
	defer stopWatch()
	if err := send(conn, &Hello{Envelope: envelope("hello"), AgentID: c.agentID, MaxInFlight: c.maxInFlight}); err != nil {
		return false, err
	}
	incoming := make(chan received, 1)
	go func() {
		for {
			_, b, err := conn.ReadMessage()
			var f Frame
			if err == nil {
				f, err = DecodeFrame(b)
			}
			select {
			case incoming <- received{f, err}:
			case <-finished:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	results := make(chan completed, c.maxInFlight)
	jobs := make(map[string]Delivery)
	heartbeat := time.NewTicker(20 * time.Second)
	defer heartbeat.Stop()
	stopping := ctx.Done()
	draining := false
	delivered := false
	for {
		if !draining && ctx.Err() != nil {
			draining = true
			stopping = nil
		}
		if draining && len(jobs) == 0 {
			return delivered, ctx.Err()
		}
		select {
		case <-stopping:
			draining = true
			stopping = nil
		case message := <-incoming:
			if message.err != nil {
				return delivered, unavailable()
			}
			if ctx.Err() != nil {
				draining = true
				stopping = nil
				continue
			}
			d, ok := message.frame.(*Delivery)
			if !ok || len(jobs) >= c.maxInFlight {
				return delivered, invalid("unexpected_delivery")
			}
			if c.admissionHook != nil {
				c.admissionHook()
			}
			if _, exists := jobs[d.Lease.DeliveryID]; exists {
				return delivered, invalid("duplicate_delivery")
			}
			admissionMu.Lock()
			if !admissionOpen || ctx.Err() != nil {
				admissionMu.Unlock()
				draining = true
				stopping = nil
				continue
			}
			// Store correlation by value before handing mutable input to business code.
			delivery := *d
			jobs[delivery.Lease.DeliveryID] = delivery
			delivered = true
			start := make(chan struct{})
			go func() {
				<-start
				result, err := handler(sessionCtx, delivery)
				select {
				case results <- completed{delivery, result, err}:
				case <-finished:
				}
			}()
			close(start)
			admissionMu.Unlock()
		case result := <-results:
			if result.err != nil {
				return delivered, unavailable()
			}
			d := result.delivery
			correlation := Ack{Envelope: Envelope{Version: 1, RequestID: d.RequestID, Type: "ack"}, EventID: d.Lease.EventID, DeliveryID: d.Lease.DeliveryID, AttemptToken: d.Lease.AttemptToken}
			var frame Frame = &correlation
			if result.result.Response != nil {
				r := *result.result.Response
				if r.Status < 200 {
					return delivered, invalid("invalid_response")
				}
				correlation.Type = "response"
				frame = &Response{Ack: correlation, Response: r}
			}
			if err := send(conn, frame); err != nil {
				return delivered, err
			}
			delete(jobs, d.Lease.DeliveryID)
		case <-heartbeat.C:
			if err := send(conn, &Heartbeat{Envelope: envelope("heartbeat")}); err != nil {
				return delivered, err
			}
			for _, d := range jobs {
				lease := d.Lease
				if err := send(conn, &Heartbeat{Envelope: envelope("heartbeat"), Lease: &lease}); err != nil {
					return delivered, err
				}
			}
		}
	}
}

// Subscription is live-only. It closes on transport loss or a full 64-frame
// buffer, never replays or reconnects, and sends no processing ACKs. Messages
// already buffered may be read after Close. Close and Err are concurrency safe.
type Subscription struct {
	conn     *websocket.Conn
	messages chan RealtimeFrame
	done     chan struct{}
	cancel   context.CancelFunc
	mu       sync.Mutex
	err      error
}

func (s *Subscription) Messages() <-chan RealtimeFrame { return s.messages }
func (s *Subscription) Done() <-chan struct{}          { return s.done }
func (s *Subscription) Err() error                     { s.mu.Lock(); defer s.mu.Unlock(); return s.err }
func (s *Subscription) Close() error                   { s.cancel(); s.conn.Close(); <-s.done; return nil }

// Subscribe returns a bounded channel so slow consumers cannot accumulate
// unbounded work. Resubscribe explicitly after Done; missed messages are lost.
func (c *Client) Subscribe(ctx context.Context, channel string) (*Subscription, error) {
	if err := validChannel(channel); err != nil {
		return nil, err
	}
	conn, err := c.socket(ctx, "/api/v1/realtime/"+channel)
	if err != nil {
		return nil, err
	}
	subCtx, cancel := context.WithCancel(ctx)
	s := &Subscription{conn: conn, messages: make(chan RealtimeFrame, 64), done: make(chan struct{}), cancel: cancel}
	stop := context.AfterFunc(subCtx, func() { conn.Close() })
	go func() {
		var terminal error
		defer func() {
			stop()
			cancel()
			conn.Close()
			if ctx.Err() != nil {
				terminal = ctx.Err()
			}
			s.mu.Lock()
			s.err = terminal
			s.mu.Unlock()
			close(s.messages)
			close(s.done)
		}()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				if subCtx.Err() == nil {
					terminal = unavailable()
				}
				return
			}
			frame, err := DecodeFrame(data)
			if err != nil {
				terminal = err
				return
			}
			realtime, ok := frame.(*RealtimeFrame)
			if !ok || realtime.Channel != channel {
				terminal = invalid("invalid_realtime")
				return
			}
			if subCtx.Err() != nil {
				return
			}
			select {
			case s.messages <- *realtime:
			default:
				terminal = &Error{429, "slow_consumer"}
				return
			}
		}
	}()
	return s, nil
}
