package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/dungxbuif/RelayHub/internal/auth"
	"github.com/dungxbuif/RelayHub/internal/streamprotocol"
	"github.com/gorilla/websocket"
)

type recordingStream struct {
	mu   sync.Mutex
	apps []string
}

func (stream *recordingStream) Serve(_ context.Context, appID string, connection *websocket.Conn) {
	stream.mu.Lock()
	stream.apps = append(stream.apps, appID)
	stream.mu.Unlock()
	_ = connection.WriteJSON(map[string]any{"type": "ready", "protocol_version": 1, "app_id": appID, "connection_id": "conn_test", "heartbeat_interval_ms": 25000, "max_in_flight_limit": 256})
	_ = connection.Close()
}

func streamFixture(t *testing.T) (*httptest.Server, *auth.TokenIssuer, *recordingStream) {
	t.Helper()
	issuer := auth.NewTokenIssuer([]byte("stream-secret"), time.Now)
	stream := &recordingStream{}
	router := NewRouter(Dependencies{TokenIssuer: issuer, Stream: stream, AllowedOrigins: []string{"https://allowed.example"}, Docs: fstest.MapFS{}, Metrics: http.NotFoundHandler()})
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	return server, issuer, stream
}

func TestStreamAuthenticatesAndNegotiatesBeforeUpgrade(t *testing.T) {
	server, issuer, stream := streamFixture(t)
	streamToken := wsToken(t, issuer, "app_target", "stream:connect")
	for _, tc := range []struct {
		name, token, origin, protocol string
		status                        int
	}{
		{"missing token", "", "", streamprotocol.Subprotocol, 401},
		{"wrong scope", wsToken(t, issuer, "app_target", "ws:connect"), "", streamprotocol.Subprotocol, 403},
		{"forbidden origin", streamToken, "https://evil.example", streamprotocol.Subprotocol, 403},
		{"missing version", streamToken, "", "", 400},
		{"wrong version", streamToken, "", "relayhub.stream.v2", 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			headers := http.Header{}
			if tc.origin != "" {
				headers.Set("Origin", tc.origin)
			}
			dialer := *websocket.DefaultDialer
			if tc.protocol != "" {
				dialer.Subprotocols = []string{tc.protocol}
			}
			conn, response, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/api/v1/stream?token="+tc.token, headers)
			if conn != nil {
				_ = conn.Close()
			}
			if err == nil || response == nil || response.StatusCode != tc.status {
				t.Fatalf("status=%v error=%v", response, err)
			}
			var body map[string]any
			if json.NewDecoder(response.Body).Decode(&body) != nil || body["error"] == nil {
				t.Fatal("nonstandard pre-upgrade error")
			}
			_ = response.Body.Close()
		})
	}
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.apps) != 0 {
		t.Fatalf("stream started before accepted upgrade: %v", stream.apps)
	}
}

func TestStreamRealSocketBindsAuthenticatedApplication(t *testing.T) {
	server, issuer, stream := streamFixture(t)
	dialer := *websocket.DefaultDialer
	dialer.Subprotocols = []string{streamprotocol.Subprotocol}
	connection, response, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/api/v1/stream?token="+wsToken(t, issuer, "app_target", "stream:connect"), http.Header{"Origin": []string{"https://allowed.example"}})
	if err != nil {
		if response != nil {
			t.Fatalf("status=%d error=%v", response.StatusCode, err)
		}
		t.Fatal(err)
	}
	defer connection.Close()
	if connection.Subprotocol() != streamprotocol.Subprotocol {
		t.Fatalf("selected=%q", connection.Subprotocol())
	}
	_ = connection.SetReadDeadline(time.Now().Add(time.Second))
	var ready map[string]any
	if connection.ReadJSON(&ready) != nil || ready["app_id"] != "app_target" {
		t.Fatalf("ready=%v", ready)
	}
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.apps) != 1 || stream.apps[0] != "app_target" {
		t.Fatalf("apps=%v", stream.apps)
	}
}

func TestStreamReturnsUnavailableAfterAuthenticationWhenGatewayIsDisabled(t *testing.T) {
	issuer := auth.NewTokenIssuer([]byte("stream-secret"), time.Now)
	router := NewRouter(Dependencies{TokenIssuer: issuer, Docs: fstest.MapFS{}, Metrics: http.NotFoundHandler()})
	server := httptest.NewServer(router)
	defer server.Close()
	dialer := *websocket.DefaultDialer
	dialer.Subprotocols = []string{streamprotocol.Subprotocol}
	connection, response, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/api/v1/stream?token="+wsToken(t, issuer, "app_target", "stream:connect"), nil)
	if connection != nil {
		_ = connection.Close()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("response=%v error=%v", response, err)
	}
	defer response.Body.Close()
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if json.NewDecoder(response.Body).Decode(&body) != nil || body.Error.Code != "stream_unavailable" {
		t.Fatalf("body=%+v", body)
	}
}
