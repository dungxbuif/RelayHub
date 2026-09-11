//go:build integration

package redisstore

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestPubSubReconnectClientPreservesTLSAndCustomDialer(t *testing.T) {
	base := integrationRedisClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tlsOptions := redisTLSProxy(t, base)
	var calls atomic.Int32
	dial := redis.NewDialer(tlsOptions)
	tlsOptions.Dialer = func(ctx context.Context, network, address string) (net.Conn, error) {
		calls.Add(1)
		return dial(ctx, network, address)
	}
	source := &Client{client: redis.NewClient(tlsOptions), prefix: base.prefix}
	defer source.Close()
	if err := source.Ping(ctx); err != nil {
		t.Fatalf("TLS fixture is unavailable: %v", err)
	}
	before := calls.Load()
	var disconnected atomic.Bool
	client := reconnectTestClient(source, &disconnected, make(chan struct{}, 1))
	defer client.Close()
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("reconnect fixture lost working TLS transport: %v", err)
	}
	if calls.Load() <= before {
		t.Fatal("reconnect fixture bypassed the configured dialer")
	}
}

func TestPubSubReconnectClientsUseDistinctServerVisibleNames(t *testing.T) {
	base := integrationRedisClient(t)
	ctx := context.Background()
	var disconnected atomic.Bool
	first := reconnectTestClient(base, &disconnected, make(chan struct{}, 1))
	defer first.Close()
	second := reconnectTestClient(base, &disconnected, make(chan struct{}, 1))
	defer second.Close()
	ids := make([]int64, 2)
	for i, c := range []*Client{first, second} {
		id, err := c.client.ClientID(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}
	list, err := base.client.ClientList(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, line := range strings.Split(list, "\n") {
		fields := map[string]string{}
		for _, field := range strings.Fields(line) {
			key, value, _ := strings.Cut(field, "=")
			fields[key] = value
		}
		if fields["name"] == first.client.Options().ClientName || fields["name"] == second.client.Options().ClientName {
			names[fields["name"]] = true
		}
	}
	if ids[0] == ids[1] || len(names) != 2 || names[""] {
		t.Fatalf("reconnect fixtures share a server-visible identity: %v", names)
	}
}

// Forward a trusted local TLS endpoint to real Redis, preserving the upstream
// transport too. No permissive certificate verification or fake Redis protocol.
func redisTLSProxy(t *testing.T, upstream *Client) *redis.Options {
	t.Helper()
	cert := httptest.NewTLSServer(nil)
	serverTLS := cert.TLS.Clone()
	clientTLS := cert.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	cert.Close()
	listener, err := tls.Listen("tcp", "127.0.0.1:0", serverTLS)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var mu sync.Mutex
	connections := []net.Conn{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			down, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			connections = append(connections, down)
			mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer down.Close()
				base := upstream.client.Options()
				up, err := base.Dialer(ctx, base.Network, base.Addr)
				if err != nil {
					return
				}
				defer up.Close()
				mu.Lock()
				connections = append(connections, up)
				mu.Unlock()
				copied := make(chan struct{})
				go func() { _, _ = io.Copy(up, down); _ = up.Close(); close(copied) }()
				_, _ = io.Copy(down, up)
				_ = down.Close()
				<-copied
			}()
		}
	}()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		<-done
		mu.Lock()
		for _, conn := range connections {
			_ = conn.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	options := *upstream.client.Options()
	options.Addr = listener.Addr().String()
	options.Network = "tcp"
	options.TLSConfig = clientTLS
	options.Dialer = nil
	options.PushNotificationProcessor = nil
	if options.MaintNotificationsConfig != nil {
		copy := *options.MaintNotificationsConfig
		options.MaintNotificationsConfig = &copy
	}
	return &options
}
