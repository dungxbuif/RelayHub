//go:build integration

package natsbroker

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/broker"
	server "github.com/nats-io/nats-server/v2/server"
	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestBootstrapCreatesExpectedStreamsAndIsIdempotent(t *testing.T) {
	serverURL := startJetStreamServer(t)
	client := connectTestClient(t, serverURL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	settings := DefaultStreamSettings()
	if err := client.Bootstrap(ctx, settings); err != nil {
		t.Fatalf("Bootstrap() error = %v", err)
	}
	if err := client.Bootstrap(ctx, settings); err != nil {
		t.Fatalf("Bootstrap() second call error = %v", err)
	}

	js, err := jetstream.New(client.Conn())
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}
	for _, expected := range ExpectedStreams(settings) {
		stream, err := js.Stream(ctx, expected.Name)
		if err != nil {
			t.Fatalf("Stream(%s) error = %v", expected.Name, err)
		}
		info, err := stream.Info(ctx)
		if err != nil {
			t.Fatalf("Info(%s) error = %v", expected.Name, err)
		}
		assertManagedConfig(t, info.Config, expected)
	}
}

func TestDeploymentNATSConfigurationParses(t *testing.T) {
	t.Setenv("RELAYHUB_NATS_USERNAME", "relayhub")
	t.Setenv("RELAYHUB_NATS_PASSWORD", "test-password")
	options, err := server.ProcessConfigFile(filepath.Join("..", "..", "..", "deploy", "nats", "nats.conf"))
	if err != nil {
		t.Fatalf("ProcessConfigFile() error = %v", err)
	}
	if !options.JetStream || options.Port != 4222 || options.HTTPPort != 8222 || options.StoreDir != "/data" {
		t.Fatalf("NATS options = %+v, want private JetStream listeners and /data", options)
	}
}

func TestBootstrapRejectsUnsafeExistingStreamDifference(t *testing.T) {
	serverURL := startJetStreamServer(t)
	client := connectTestClient(t, serverURL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	js, err := jetstream.New(client.Conn())
	if err != nil {
		t.Fatal(err)
	}
	settings := DefaultStreamSettings()
	expected := ExpectedStreams(settings)[0]
	expected.Storage = jetstream.MemoryStorage
	if _, err := js.CreateStream(ctx, expected); err != nil {
		t.Fatal(err)
	}

	err = client.Bootstrap(ctx, settings)
	if err == nil || !errors.Is(err, ErrUnsafeStreamConfig) {
		t.Fatalf("Bootstrap() error = %v, want ErrUnsafeStreamConfig", err)
	}
	stream, err := js.Stream(ctx, expected.Name)
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.Storage != jetstream.MemoryStorage {
		t.Fatalf("Bootstrap() silently mutated storage to %v", info.Config.Storage)
	}
}

func TestClientPublishesConsumesAcknowledgesAndInspects(t *testing.T) {
	serverURL := startJetStreamServer(t)
	client := connectTestClient(t, serverURL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Bootstrap(ctx, DefaultStreamSettings()); err != nil {
		t.Fatal(err)
	}
	subjects, err := SubjectsForApp("app_consumer")
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan []byte, 1)
	subscription, err := client.Consume(ctx, broker.ConsumerConfig{Stream: "RH_DELIVERIES", DurableName: "app_consumer_default", Filter: subjects.Deliveries, MaxPending: 4}, func(handlerCtx context.Context, message broker.Message) {
		received <- append([]byte(nil), message.Data()...)
		if ackErr := message.Ack(handlerCtx); ackErr != nil {
			t.Errorf("Ack() error = %v", ackErr)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Drain(context.Background())
	ack, err := client.Publish(ctx, broker.Publication{Subject: subjects.Deliveries, Data: []byte(`{"event":"one"}`), MessageID: "delivery_1"})
	if err != nil || ack.Duplicate {
		t.Fatalf("Publish() = %+v, %v", ack, err)
	}
	duplicate, err := client.Publish(ctx, broker.Publication{Subject: subjects.Deliveries, Data: []byte(`{"event":"one"}`), MessageID: "delivery_1"})
	if err != nil || !duplicate.Duplicate {
		t.Fatalf("duplicate Publish() = %+v, %v", duplicate, err)
	}
	select {
	case data := <-received:
		if string(data) != `{"event":"one"}` {
			t.Fatalf("message data = %s", data)
		}
	case <-ctx.Done():
		t.Fatal("consumer did not receive message")
	}
	info, err := client.Stream(ctx, "RH_DELIVERIES")
	if err != nil || info.Name != "RH_DELIVERIES" || info.Messages != 0 {
		t.Fatalf("Stream() = %+v, %v, want acknowledged empty stream", info, err)
	}
}

func TestClientRequestReply(t *testing.T) {
	serverURL := startJetStreamServer(t)
	client := connectTestClient(t, serverURL)
	subscription, err := client.Conn().Subscribe("rh.functions.test", func(message *gonats.Msg) {
		_ = message.Respond([]byte("response"))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Unsubscribe()
	if err := client.Conn().Flush(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	reply, err := client.Request(ctx, "rh.functions.test", []byte("request"))
	if err != nil || string(reply) != "response" {
		t.Fatalf("Request() = %q, %v", reply, err)
	}
}

func TestClientReconnectsAndDrains(t *testing.T) {
	storeDir := t.TempDir()
	options := &server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: storeDir, NoLog: true, NoSigs: true}
	srv, err := server.NewServer(options)
	if err != nil {
		t.Fatal(err)
	}
	srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server did not become ready")
	}
	serverURL := srv.ClientURL()

	reconnected := make(chan struct{}, 1)
	client, err := Connect(Options{URL: serverURL, ConnectTimeout: time.Second, ReconnectWait: 25 * time.Millisecond, MaxReconnects: 100, DrainTimeout: time.Second, Hooks: Hooks{Reconnected: func() { reconnected <- struct{}{} }}})
	if err != nil {
		t.Fatal(err)
	}
	srv.Shutdown()
	srv.WaitForShutdown()

	restarted, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: options.Port, JetStream: true, StoreDir: storeDir, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	restarted.Start()
	defer restarted.Shutdown()
	if !restarted.ReadyForConnections(5 * time.Second) {
		t.Fatal("restarted NATS server did not become ready")
	}
	select {
	case <-reconnected:
	case <-time.After(5 * time.Second):
		t.Fatal("client did not report reconnect")
	}
	if err := client.Drain(); err != nil {
		t.Fatalf("Drain() error = %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for !client.IsClosed() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !client.IsClosed() {
		t.Fatal("client remained open after Drain deadline")
	}
}

func startJetStreamServer(t *testing.T) string {
	t.Helper()
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server did not become ready")
	}
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	return srv.ClientURL()
}

func connectTestClient(t *testing.T, url string) *Client {
	t.Helper()
	client, err := Connect(Options{URL: url, ConnectTimeout: time.Second, ReconnectWait: 25 * time.Millisecond, MaxReconnects: 10, DrainTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

func assertManagedConfig(t *testing.T, got, want jetstream.StreamConfig) {
	t.Helper()
	if got.Name != want.Name || got.Retention != want.Retention || got.Storage != want.Storage || got.Replicas != want.Replicas || got.MaxAge != want.MaxAge || got.Duplicates != want.Duplicates || got.Discard != want.Discard || len(got.Subjects) != 1 || got.Subjects[0] != want.Subjects[0] {
		t.Fatalf("stream config = %+v, want managed fields from %+v", got, want)
	}
}

var _ broker.Client = (*Client)(nil)
