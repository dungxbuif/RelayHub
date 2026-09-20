// Package broker defines RelayHub's private messaging boundary. Public API and
// SDK types must not expose subjects, stream names or JetStream sequence values.
package broker

import (
	"context"
	"errors"
	"time"
)

type Publication struct {
	Subject   string
	Data      []byte
	MessageID string
}

type PublishAck struct {
	Duplicate bool
}

type Publisher interface {
	Publish(context.Context, Publication) (PublishAck, error)
}

type Message interface {
	Data() []byte
	Ack(context.Context) error
	Nack(time.Duration) error
	Progress() error
}

type Handler func(context.Context, Message)

type ConsumerConfig struct {
	Stream      string
	DurableName string
	Filter      string
	MaxPending  int
}

type Subscription interface {
	Drain(context.Context) error
}

type Consumer interface {
	Consume(context.Context, ConsumerConfig, Handler) (Subscription, error)
}

type Requester interface {
	Request(context.Context, string, []byte) ([]byte, error)
}

type StreamInfo struct {
	Name     string
	Messages uint64
	Bytes    uint64
}

type Inspector interface {
	Stream(context.Context, string) (StreamInfo, error)
}

type Client interface {
	Publisher
	Consumer
	Requester
	Inspector
	Ping(context.Context) error
	Drain() error
	Close()
}

type HealthChecker interface {
	Ping(context.Context) error
}

type CompositeHealth []HealthChecker

func (checks CompositeHealth) Ping(ctx context.Context) error {
	var failed bool
	for _, check := range checks {
		if check == nil || check.Ping(ctx) != nil {
			failed = true
		}
	}
	if failed {
		return errors.New("required dependency unavailable")
	}
	return nil
}
