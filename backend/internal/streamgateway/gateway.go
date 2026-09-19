// Package streamgateway bridges the public RelayHub stream protocol to private
// JetStream consumers and PostgreSQL-fenced delivery assignments.
package streamgateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dungxbuif/RelayHub/internal/broker"
	natsbroker "github.com/dungxbuif/RelayHub/internal/broker/nats"
	"github.com/dungxbuif/RelayHub/internal/store"
)

type Options struct {
	Consumer         broker.Consumer
	Assignments      store.DeliveryAssignmentStore
	Now              func() time.Time
	NewID            func(string) (string, error)
	AssignmentLease  time.Duration
	MaxProcessing    time.Duration
	DrainTimeout     time.Duration
	MaxInFlight      int
	MaxInFlightBytes int
	OutboundQueue    int
	RetryDelay       time.Duration
	PongWait         time.Duration
}

type Gateway struct {
	options  Options
	mu       sync.Mutex
	sessions map[*Session]struct{}
	draining bool
}

func (gateway *Gateway) ConnectionCount() int64 {
	if gateway == nil {
		return 0
	}
	gateway.mu.Lock()
	defer gateway.mu.Unlock()
	return int64(len(gateway.sessions))
}

func New(options Options) (*Gateway, error) {
	if options.Consumer == nil || options.Assignments == nil || options.NewID == nil {
		return nil, errors.New("stream gateway dependencies are required")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.AssignmentLease <= 0 {
		options.AssignmentLease = 60 * time.Second
	}
	if options.MaxProcessing <= 0 {
		options.MaxProcessing = 15 * time.Minute
	}
	if options.MaxProcessing < options.AssignmentLease {
		return nil, errors.New("stream maximum processing time is shorter than assignment lease")
	}
	if options.DrainTimeout <= 0 {
		options.DrainTimeout = 10 * time.Second
	}
	if options.MaxInFlight <= 0 {
		options.MaxInFlight = 256
	}
	if options.MaxInFlight > 256 {
		return nil, errors.New("stream max inflight exceeds protocol limit")
	}
	if options.MaxInFlightBytes <= 0 {
		options.MaxInFlightBytes = 16 << 20
	}
	if options.OutboundQueue <= 0 {
		options.OutboundQueue = options.MaxInFlight + 16
	}
	if options.RetryDelay <= 0 {
		options.RetryDelay = time.Second
	}
	if options.PongWait <= 0 {
		options.PongWait = 60 * time.Second
	}
	return &Gateway{options: options, sessions: map[*Session]struct{}{}}, nil
}

func (gateway *Gateway) open(appID string) (*Session, error) {
	if appID == "" {
		return nil, errors.New("stream application identity is required")
	}
	connectionID, err := gateway.options.NewID("conn_")
	if err != nil || connectionID == "" {
		return nil, errors.New("create stream connection identity")
	}
	gateway.mu.Lock()
	defer gateway.mu.Unlock()
	if gateway.draining {
		return nil, errors.New("stream gateway is draining")
	}
	ctx, cancel := context.WithCancel(context.Background())
	session := &Session{gateway: gateway, appID: appID, connectionID: connectionID, ctx: ctx, cancel: cancel, frames: make(chan []byte, gateway.options.OutboundQueue), done: make(chan struct{}), inflight: map[string]*inflightDelivery{}}
	gateway.sessions[session] = struct{}{}
	if !session.enqueue(map[string]any{"type": "ready", "protocol_version": 1, "app_id": appID, "connection_id": connectionID, "heartbeat_interval_ms": 25000, "max_in_flight_limit": gateway.options.MaxInFlight}) {
		delete(gateway.sessions, session)
		cancel()
		return nil, errors.New("initialize stream session")
	}
	return session, nil
}

func (gateway *Gateway) unregister(session *Session) {
	gateway.mu.Lock()
	delete(gateway.sessions, session)
	gateway.mu.Unlock()
}

func (gateway *Gateway) Drain(ctx context.Context) error {
	gateway.mu.Lock()
	gateway.draining = true
	sessions := make([]*Session, 0, len(gateway.sessions))
	for session := range gateway.sessions {
		sessions = append(sessions, session)
	}
	gateway.mu.Unlock()
	var result error
	for _, session := range sessions {
		if err := session.Close(ctx); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}

func durableName(appID string) string {
	token, err := natsbroker.AppToken(appID)
	if err != nil {
		return "invalid"
	}
	return fmt.Sprintf("rh_%s_default", token)
}
