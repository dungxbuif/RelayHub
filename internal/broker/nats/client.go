package natsbroker

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/dungxbuif/RelayHub/internal/broker"
	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var ErrUnavailable = errors.New("NATS unavailable")

type Hooks struct {
	Disconnected func()
	Reconnected  func()
	SlowConsumer func()
	AsyncError   func()
	Drained      func()
}

type Options struct {
	URL            string
	Name           string
	Username       string
	Password       string
	ConnectTimeout time.Duration
	ReconnectWait  time.Duration
	MaxReconnects  int
	DrainTimeout   time.Duration
	Hooks          Hooks
}

type Client struct {
	connection *gonats.Conn
	jetstream  jetstream.JetStream
	drainHook  func()
}

func Connect(options Options) (*Client, error) {
	if err := validateOptions(options); err != nil {
		return nil, err
	}
	if options.Name == "" {
		options.Name = "relayhub"
	}
	if options.ConnectTimeout <= 0 {
		options.ConnectTimeout = 2 * time.Second
	}
	if options.ReconnectWait <= 0 {
		options.ReconnectWait = 2 * time.Second
	}
	if options.DrainTimeout <= 0 {
		options.DrainTimeout = 10 * time.Second
	}
	natsOptions := []gonats.Option{
		gonats.Name(options.Name),
		gonats.Timeout(options.ConnectTimeout),
		gonats.ReconnectWait(options.ReconnectWait),
		gonats.MaxReconnects(options.MaxReconnects),
		gonats.DrainTimeout(options.DrainTimeout),
		gonats.ReconnectJitter(100*time.Millisecond, time.Second),
		gonats.ReconnectOnFlusherError(),
		gonats.DisconnectErrHandler(func(_ *gonats.Conn, _ error) {
			if options.Hooks.Disconnected != nil {
				options.Hooks.Disconnected()
			}
		}),
		gonats.ReconnectHandler(func(_ *gonats.Conn) {
			if options.Hooks.Reconnected != nil {
				options.Hooks.Reconnected()
			}
		}),
		gonats.ErrorHandler(func(_ *gonats.Conn, _ *gonats.Subscription, err error) {
			if errors.Is(err, gonats.ErrSlowConsumer) && options.Hooks.SlowConsumer != nil {
				options.Hooks.SlowConsumer()
			}
			if options.Hooks.AsyncError != nil {
				options.Hooks.AsyncError()
			}
		}),
	}
	if options.Username != "" || options.Password != "" {
		natsOptions = append(natsOptions, gonats.UserInfo(options.Username, options.Password))
	}
	connection, err := gonats.Connect(options.URL, natsOptions...)
	if err != nil {
		return nil, fmt.Errorf("%w: connection failed", ErrUnavailable)
	}
	js, err := jetstream.New(connection)
	if err != nil {
		connection.Close()
		return nil, fmt.Errorf("%w: JetStream client failed", ErrUnavailable)
	}
	return &Client{connection: connection, jetstream: js, drainHook: options.Hooks.Drained}, nil
}

func validateOptions(options Options) error {
	parsed, err := url.Parse(options.URL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "nats" && parsed.Scheme != "tls") || parsed.User != nil {
		return errors.New("RELAYHUB_NATS_URL is invalid")
	}
	if options.MaxReconnects < -1 {
		return errors.New("RELAYHUB_NATS_MAX_RECONNECTS is invalid")
	}
	return nil
}

func (client *Client) Conn() *gonats.Conn { return client.connection }

func (client *Client) Ping(ctx context.Context) error {
	if client == nil || client.connection == nil || !client.connection.IsConnected() {
		return ErrUnavailable
	}
	if _, err := client.jetstream.AccountInfo(ctx); err != nil {
		return fmt.Errorf("%w: JetStream health check failed", ErrUnavailable)
	}
	return nil
}

func (client *Client) Publish(ctx context.Context, publication broker.Publication) (broker.PublishAck, error) {
	if err := sanitizeSubject(publication.Subject); err != nil {
		return broker.PublishAck{}, err
	}
	message := gonats.NewMsg(publication.Subject)
	message.Data = append([]byte(nil), publication.Data...)
	if publication.MessageID != "" {
		message.Header.Set(gonats.MsgIdHdr, publication.MessageID)
	}
	ack, err := client.jetstream.PublishMsg(ctx, message)
	if err != nil {
		return broker.PublishAck{}, fmt.Errorf("%w: publish failed", ErrUnavailable)
	}
	return broker.PublishAck{Duplicate: ack.Duplicate}, nil
}

func (client *Client) Request(ctx context.Context, subject string, data []byte) ([]byte, error) {
	if err := sanitizeSubject(subject); err != nil {
		return nil, err
	}
	reply, err := client.connection.RequestWithContext(ctx, subject, data)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), reply.Data...), nil
}

type message struct{ inner jetstream.Msg }

func (msg message) Data() []byte { return msg.inner.Data() }

func (msg message) Ack(ctx context.Context) error { return msg.inner.DoubleAck(ctx) }

func (msg message) Nack(delay time.Duration) error {
	if delay > 0 {
		return msg.inner.NakWithDelay(delay)
	}
	return msg.inner.Nak()
}

func (msg message) Progress() error { return msg.inner.InProgress() }

type subscription struct{ inner jetstream.ConsumeContext }

func (sub subscription) Drain(ctx context.Context) error {
	sub.inner.Drain()
	select {
	case <-sub.inner.Closed():
		return nil
	case <-ctx.Done():
		sub.inner.Stop()
		return ctx.Err()
	}
}

func (client *Client) Consume(ctx context.Context, config broker.ConsumerConfig, handler broker.Handler) (broker.Subscription, error) {
	if handler == nil || config.Stream == "" || config.DurableName == "" || config.MaxPending < 1 {
		return nil, errors.New("invalid consumer configuration")
	}
	if err := sanitizeSubject(config.Filter); err != nil {
		return nil, err
	}
	consumer, err := client.jetstream.CreateOrUpdateConsumer(ctx, config.Stream, jetstream.ConsumerConfig{
		Durable: config.DurableName, Name: config.DurableName,
		AckPolicy: jetstream.AckExplicitPolicy, FilterSubject: config.Filter,
		MaxAckPending: config.MaxPending,
	})
	if err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}
	consumeContext, err := consumer.Consume(func(received jetstream.Msg) {
		handler(ctx, message{inner: received})
	})
	if err != nil {
		return nil, fmt.Errorf("consume: %w", err)
	}
	return subscription{inner: consumeContext}, nil
}

func (client *Client) Stream(ctx context.Context, name string) (broker.StreamInfo, error) {
	stream, err := client.jetstream.Stream(ctx, name)
	if err != nil {
		return broker.StreamInfo{}, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return broker.StreamInfo{}, err
	}
	return broker.StreamInfo{Name: info.Config.Name, Messages: info.State.Msgs, Bytes: info.State.Bytes}, nil
}

func (client *Client) Drain() error {
	if client == nil || client.connection == nil {
		return nil
	}
	err := client.connection.Drain()
	if err == nil && client.drainHook != nil {
		client.drainHook()
	}
	return err
}

func (client *Client) Close() {
	if client != nil && client.connection != nil {
		client.connection.Close()
	}
}

func (client *Client) IsClosed() bool {
	return client == nil || client.connection == nil || client.connection.IsClosed()
}

func sanitizeSubject(subject string) error {
	if subject == "" || subject != strings.TrimSpace(subject) || strings.ContainsAny(subject, " \t\r\n") {
		return errors.New("invalid internal subject")
	}
	return nil
}
