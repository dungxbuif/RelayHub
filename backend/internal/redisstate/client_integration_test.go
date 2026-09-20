//go:build integration

package redisstate

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestClientPingAndClose(t *testing.T) {
	address, password := integrationRedis(t)
	cfg := integrationConfig(address, password)

	client, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := client.Ping(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Ping() after Close error = %v, want ErrUnavailable", err)
	}
}

func TestClientAuthenticationFailureDoesNotLeakPassword(t *testing.T) {
	address, password := integrationRedis(t)
	wrongPassword := password + "-wrong"
	cfg := integrationConfig(address, wrongPassword)

	client, err := New(context.Background(), cfg)
	if client != nil {
		_ = client.Close()
		t.Fatal("New() returned a client with invalid credentials")
	}
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("New() error = %v, want ErrUnavailable", err)
	}
	for _, secret := range []string{password, wrongPassword, address} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("New() error leaked sensitive connection data: %q", err)
		}
	}
}

func integrationRedis(t *testing.T) (string, string) {
	t.Helper()
	const password = "relayhub-test-secret"
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7.4-alpine",
			ExposedPorts: []string{"6379/tcp"},
			Cmd:          []string{"redis-server", "--save", "", "--appendonly", "no", "--requirepass", password},
			WaitingFor:   wait.ForLog("Ready to accept connections").WithStartupTimeout(45 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Skipf("Redis testcontainer unavailable: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		_ = container.Terminate(stopCtx)
	})
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "6379/tcp")
	if err != nil {
		t.Fatal(err)
	}
	return host + ":" + port.Port(), password
}

func integrationConfig(address, password string) Config {
	return Config{
		Mode:           ModeStandalone,
		Addrs:          []string{address},
		Password:       password,
		KeyPrefix:      "rh",
		ConnectTimeout: 2 * time.Second,
		ReadTimeout:    time.Second,
		WriteTimeout:   time.Second,
		PoolSize:       4,
	}
}
