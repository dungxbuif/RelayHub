//go:build integration

package httpapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/dungxbuif/RelayHub/internal/auth"
	natsbroker "github.com/dungxbuif/RelayHub/internal/broker/nats"
	secretcrypto "github.com/dungxbuif/RelayHub/internal/crypto"
	"github.com/dungxbuif/RelayHub/internal/outbox"
	"github.com/dungxbuif/RelayHub/internal/realtime"
	"github.com/dungxbuif/RelayHub/internal/service"
	postgresstore "github.com/dungxbuif/RelayHub/internal/store/postgres"
	"github.com/dungxbuif/RelayHub/internal/streamgateway"
	"github.com/dungxbuif/RelayHub/internal/streamprotocol"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	server "github.com/nats-io/nats-server/v2/server"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestFullStackRoutedEventReachesRealtimeAndDurableStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	postgres := isolatedHTTPPostgres(t, ctx)
	natsURL := isolatedHTTPNATS(t)
	natsClient, err := natsbroker.Connect(natsbroker.Options{URL: natsURL, Name: "full-stack-http", MaxReconnects: 2, DrainTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := natsClient.Bootstrap(ctx, natsbroker.DefaultStreamSettings()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(natsClient.Close)
	hub := realtime.NewHub()
	defer hub.Close()
	bridge, err := realtime.NewNATSBridge(ctx, natsClient.Conn(), hub, "full-stack-api")
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.Close()
	durable, err := streamgateway.New(streamgateway.Options{Consumer: natsClient, Assignments: postgres, NewID: func(prefix string) (string, error) { return prefix + uuid.NewString(), nil }, RetryDelay: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Drain(context.Background())
	dispatcher, err := outbox.NewDispatcher(postgres, natsClient, outbox.Options{BatchSize: 10, ClaimTTL: time.Minute, BaseRetry: time.Millisecond, MaxRetry: time.Second, MaxAttempts: 3, MaxPendingAge: time.Minute, NewToken: func() (string, error) { return "claim-full-stack", nil }})
	if err != nil {
		t.Fatal(err)
	}
	apps := service.NewAppService(postgres, service.AppOptions{AllowInsecureCallbacks: true})
	routing := service.NewRoutingService(postgres, postgres, service.RoutingOptions{})
	events, err := service.NewEventServiceWithStores(postgres, postgres, nil, postgres, service.EventOptions{Notifier: bridge, Router: routing, Realtime: bridge})
	if err != nil {
		t.Fatal(err)
	}
	issuer := authTokenIssuer(t)
	router := NewRouter(Dependencies{Apps: apps, Events: events, Routing: routing, Realtime: hub, RealtimePub: bridge, TokenIssuer: issuer, Stream: durable, AdminToken: "admin-test-token", Now: func() time.Time { return time.Unix(1789120800, 0) }, Docs: fstest.MapFS{}, Metrics: http.NotFoundHandler()})
	testServer := httptest.NewServer(router)
	defer testServer.Close()

	producer := createAppViaHTTP(t, router, "stack-producer")
	target := createAppViaHTTP(t, router, "stack-target")
	createRule := requestJSON(t, router, http.MethodPost, "/api/v1/routing/rules", []byte(`{"source_app_id":"`+producer.AppID+`","event_type":"order.created","target_app_id":"`+target.AppID+`","realtime_channel":"orders.live"}`), map[string]string{"Authorization": "Bearer admin-test-token"})
	if createRule.Code != http.StatusCreated {
		t.Fatalf("create rule: %d %s", createRule.Code, createRule.Body.String())
	}

	realtimeToken := issueSocketToken(t, router, target, []string{"ws:connect", "ws:subscribe", "ws:read"})
	realtimeConn := wsDial(t, testServer, realtimeToken, "")
	if ready := wsRead(t, realtimeConn); ready.Type != "ready" || ready.AppID != target.AppID {
		t.Fatalf("realtime ready=%#v", ready)
	}
	if err := realtimeConn.WriteJSON(map[string]any{"type": "subscribe", "topics": []string{"channel:orders.live"}}); err != nil {
		t.Fatal(err)
	}
	if subscribed := wsRead(t, realtimeConn); subscribed.Type != "subscribed" {
		t.Fatalf("subscribed=%#v", subscribed)
	}

	streamToken := issueSocketToken(t, router, target, []string{"stream:connect"})
	streamConn := dialStream(t, testServer, streamToken)
	writeStreamFrame(t, streamConn, map[string]any{"type": "consumer.start", "protocol_version": 1, "consumer": "default", "max_in_flight": 1})
	streamReady := readStreamFrame(t, streamConn)
	if streamReady["type"] != "ready" || streamReady["app_id"] != target.AppID {
		t.Fatalf("stream ready=%v", streamReady)
	}
	consumerStarted := readStreamFrame(t, streamConn)
	if consumerStarted["type"] != "consumer.started" {
		t.Fatalf("consumer started=%v", consumerStarted)
	}

	published := signedEventRequest(t, router, producer, http.MethodPost, "/api/v1/events", []byte(`{"type":"order.created","data":{"order_id":"ord_full"}}`), "full-stack-event")
	if published.Code != http.StatusAccepted {
		t.Fatalf("publish: %d %s", published.Code, published.Body.String())
	}
	if count, err := dispatcher.RunOnce(ctx); err != nil || count == 0 {
		t.Fatalf("outbox dispatch count=%d err=%v", count, err)
	}
	channel := wsRead(t, realtimeConn)
	if channel.Type != "channel.message" || channel.Channel != "orders.live" || channel.PublisherAppID != producer.AppID || string(channel.Data) != `{"order_id":"ord_full"}` {
		t.Fatalf("channel frame=%#v", channel)
	}
	delivery := readStreamFrame(t, streamConn)
	if delivery["type"] != "event.delivery" || delivery["delivery_id"] == "" {
		t.Fatalf("delivery=%v", delivery)
	}
	event, ok := delivery["event"].(map[string]any)
	if !ok || event["source_app_id"] != producer.AppID || event["type"] != "order.created" {
		t.Fatalf("delivery event=%v", delivery["event"])
	}
	writeStreamFrame(t, streamConn, map[string]any{"type": "delivery.ack", "delivery_id": delivery["delivery_id"]})
	accepted := readStreamFrame(t, streamConn)
	if accepted["type"] != "delivery.accepted" || accepted["delivery_id"] != delivery["delivery_id"] {
		t.Fatalf("accepted=%v", accepted)
	}
}

func authTokenIssuer(t *testing.T) *auth.TokenIssuer {
	t.Helper()
	return auth.NewTokenIssuer([]byte("full-stack-token-secret"), time.Now)
}

func issueSocketToken(t *testing.T, router http.Handler, credentials service.AppCredentials, scopes []string) string {
	t.Helper()
	body, err := json.Marshal(map[string]any{"scopes": scopes, "ttl_seconds": 300})
	if err != nil {
		t.Fatal(err)
	}
	response := signedEventRequest(t, router, credentials, http.MethodPost, "/api/v1/socket/token", body, "")
	if response.Code != http.StatusCreated {
		t.Fatalf("socket token: %d %s", response.Code, response.Body.String())
	}
	var payload struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil || payload.Token == "" {
		t.Fatalf("socket token body=%s err=%v", response.Body.String(), err)
	}
	return payload.Token
}

func dialStream(t *testing.T, server *httptest.Server, token string) *websocket.Conn {
	t.Helper()
	dialer := *websocket.DefaultDialer
	dialer.Subprotocols = []string{streamprotocol.Subprotocol}
	connection, response, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/api/v1/stream?token="+token, nil)
	if err != nil {
		if response != nil {
			t.Fatalf("stream status=%d", response.StatusCode)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if connection.Subprotocol() != streamprotocol.Subprotocol {
		t.Fatalf("stream subprotocol=%q", connection.Subprotocol())
	}
	return connection
}

func readStreamFrame(t *testing.T, connection *websocket.Conn) map[string]any {
	t.Helper()
	_ = connection.SetReadDeadline(time.Now().Add(5 * time.Second))
	var frame map[string]any
	if err := connection.ReadJSON(&frame); err != nil {
		t.Fatal(err)
	}
	return frame
}

func writeStreamFrame(t *testing.T, connection *websocket.Conn, frame map[string]any) {
	t.Helper()
	_ = connection.SetWriteDeadline(time.Now().Add(time.Second))
	if err := connection.WriteJSON(frame); err != nil {
		t.Fatal(err)
	}
}

func isolatedHTTPPostgres(t *testing.T, ctx context.Context) *postgresstore.Client {
	t.Helper()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: "postgres:17-alpine", ExposedPorts: []string{"5432/tcp"}, Env: map[string]string{"POSTGRES_USER": "relayhub", "POSTGRES_PASSWORD": "relayhub-test", "POSTGRES_DB": "relayhub"}, WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(45 * time.Second),
	}, Started: true})
	if err != nil {
		t.Skipf("PostgreSQL testcontainer unavailable: %v", err)
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
	cipher, err := secretcrypto.NewSecretCipher(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{8}, 32)))
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

func isolatedHTTPNATS(t *testing.T) string {
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
