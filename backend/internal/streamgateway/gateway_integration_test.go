//go:build integration

package streamgateway

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/broker"
	natsbroker "github.com/dungxbuif/RelayHub/internal/broker/nats"
	secretcrypto "github.com/dungxbuif/RelayHub/internal/crypto"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	postgresstore "github.com/dungxbuif/RelayHub/internal/store/postgres"
	"github.com/google/uuid"
	server "github.com/nats-io/nats-server/v2/server"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestGatewayReplicasShareDurableAndRedeliverAfterDisconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	assignments := isolatedPostgres(t, ctx)
	natsURL := isolatedNATS(t)
	firstBroker := integrationBroker(t, ctx, natsURL, "gateway-one")
	secondBroker := integrationBroker(t, ctx, natsURL, "gateway-two")

	now := time.Now().UTC().Truncate(time.Millisecond)
	for _, app := range []domain.App{
		{ID: "producer", Name: "producer", DeliveryMode: domain.DeliveryWebSocket, Enabled: true, CreatedAt: now, UpdatedAt: now},
		{ID: "target", Name: "target", DeliveryMode: domain.DeliveryWebSocket, Enabled: true, CreatedAt: now, UpdatedAt: now},
	} {
		if err := assignments.CreateApplication(ctx, app, store.AppCredential{AppID: app.ID, APIKeyHash: "hash-" + app.ID, HMACSecret: []byte("secret")}); err != nil {
			t.Fatal(err)
		}
	}
	event := domain.Event{ID: "evt_gateway", Type: "order.created", SourceAppID: "producer", TargetAppIDs: []string{"target"}, Data: json.RawMessage(`{"order_id":42}`), CreatedAt: now}
	job := domain.Job{ID: "dlv_gateway", EventID: event.ID, SourceAppID: "producer", TargetAppID: "target", Status: domain.JobPending, CreatedAt: now, UpdatedAt: now}
	if _, _, err := assignments.PublishEvent(ctx, store.Publication{Event: event, Jobs: []domain.Job{job}}, "gateway-idempotency", store.EventRetention{Event: time.Hour, Job: time.Hour, Idempotency: time.Hour}); err != nil {
		t.Fatal(err)
	}

	newGateway := func(consumer broker.Consumer) *Gateway {
		gateway, err := New(Options{Consumer: consumer, Assignments: assignments, NewID: func(prefix string) (string, error) { return prefix + uuid.NewString(), nil }, RetryDelay: 10 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		return gateway
	}
	firstGateway, secondGateway := newGateway(firstBroker), newGateway(secondBroker)
	first, _ := firstGateway.open("target")
	second, _ := secondGateway.open("target")
	defer first.Close(context.Background())
	defer second.Close(context.Background())
	for _, session := range []*Session{first, second} {
		<-session.Frames()
		if protocolErr := session.Handle(ctx, []byte(`{"type":"consumer.start","protocol_version":1,"consumer":"default","max_in_flight":1}`)); protocolErr != nil {
			t.Fatal(protocolErr)
		}
		<-session.Frames()
	}

	outbox, err := assignments.ClaimOutbox(ctx, now.Add(time.Second), now.Add(-time.Minute), "gateway-claim", 1)
	if err != nil || len(outbox) != 1 {
		t.Fatalf("outbox=%#v error=%v", outbox, err)
	}
	if _, err := firstBroker.Publish(ctx, broker.Publication{Subject: outbox[0].Subject, Data: outbox[0].Payload, MessageID: outbox[0].MessageID}); err != nil {
		t.Fatal(err)
	}
	if err := assignments.MarkOutboxDispatched(ctx, outbox[0].ID, outbox[0].ClaimToken, time.Now()); err != nil {
		t.Fatal(err)
	}

	winner, loser, firstFrame := receiveEither(t, first, second)
	if firstFrame["delivery_id"] != job.ID {
		t.Fatalf("first delivery=%v", firstFrame)
	}
	if err := winner.Close(ctx); err != nil {
		t.Fatal(err)
	}
	redelivered := readIntegrationFrame(t, loser)
	if redelivered["delivery_id"] != job.ID || redelivered["attempt"].(float64) < 2 {
		t.Fatalf("redelivery=%v", redelivered)
	}
	if protocolErr := loser.Handle(ctx, []byte(`{"type":"delivery.ack","delivery_id":"dlv_gateway"}`)); protocolErr != nil {
		t.Fatal(protocolErr)
	}
	if accepted := readIntegrationFrame(t, loser); accepted["type"] != "delivery.accepted" {
		t.Fatalf("accepted=%v", accepted)
	}
}

func receiveEither(t *testing.T, first, second *Session) (*Session, *Session, map[string]any) {
	t.Helper()
	select {
	case raw := <-first.Frames():
		return first, second, decodeIntegrationFrame(t, raw)
	case raw := <-second.Frames():
		return second, first, decodeIntegrationFrame(t, raw)
	case <-time.After(5 * time.Second):
		t.Fatal("replicas did not receive delivery")
	}
	return nil, nil, nil
}

func readIntegrationFrame(t *testing.T, session *Session) map[string]any {
	t.Helper()
	select {
	case raw := <-session.Frames():
		return decodeIntegrationFrame(t, raw)
	case <-time.After(5 * time.Second):
		t.Fatal("redelivery timeout")
	}
	return nil
}

func decodeIntegrationFrame(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var frame map[string]any
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatal(err)
	}
	return frame
}

func integrationBroker(t *testing.T, ctx context.Context, url, name string) *natsbroker.Client {
	t.Helper()
	client, err := natsbroker.Connect(natsbroker.Options{URL: url, Name: name, MaxReconnects: 2, DrainTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Bootstrap(ctx, natsbroker.DefaultStreamSettings()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

func isolatedNATS(t *testing.T) string {
	t.Helper()
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("isolated NATS did not start")
	}
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL()
}

func isolatedPostgres(t *testing.T, ctx context.Context) *postgresstore.Client {
	t.Helper()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: "postgres:17-alpine", ExposedPorts: []string{"5432/tcp"}, Env: map[string]string{"POSTGRES_USER": "relayhub", "POSTGRES_PASSWORD": "relayhub-test", "POSTGRES_DB": "relayhub"}, WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(45 * time.Second),
	}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	cipher, err := secretcrypto.NewSecretCipher(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	client, err := postgresstore.NewClient(ctx, postgresstore.Config{DatabaseURL: fmt.Sprintf("postgres://relayhub:relayhub-test@%s:%s/relayhub?sslmode=disable", host, port.Port()), MaxConnections: 8}, cipher)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}
