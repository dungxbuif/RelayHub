//go:build integration

package teststack

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	dockercontainer "github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type Stack struct {
	PostgresURL   string
	NATSURL       string
	NATSUsername  string
	NATSPassword  string
	RedisAddr     string
	RedisPassword string

	postgres testcontainers.Container
	nats     testcontainers.Container
	redis    testcontainers.Container
	mu       sync.Mutex
	redisUp  bool
}

func Start(t *testing.T) *Stack {
	t.Helper()
	postgresPassword := randomCredential(t)
	natsUsername := "relayhub_" + randomCredential(t)[:12]
	natsPassword := randomCredential(t)
	redisPassword := randomCredential(t)
	redisHostPort := reserveHostPort(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	postgres := startContainer(t, ctx, testcontainers.ContainerRequest{
		Image: "postgres:17-alpine", ExposedPorts: []string{"5432/tcp"},
		Env:        map[string]string{"POSTGRES_USER": "relayhub", "POSTGRES_PASSWORD": postgresPassword, "POSTGRES_DB": "relayhub"},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60 * time.Second),
	})
	natsContainer := startContainer(t, ctx, testcontainers.ContainerRequest{
		Image: "nats:2.14.5-alpine", ExposedPorts: []string{"4222/tcp"},
		Cmd:        []string{"-js", "-sd", "/tmp/jetstream", "--user", natsUsername, "--pass", natsPassword},
		WaitingFor: wait.ForLog("Server is ready").WithStartupTimeout(45 * time.Second),
	})
	redisContainer := startContainer(t, ctx, testcontainers.ContainerRequest{
		Image: "redis:7.4-alpine", ExposedPorts: []string{"6379/tcp"},
		Cmd:        []string{"redis-server", "--save", "", "--appendonly", "no", "--requirepass", redisPassword},
		WaitingFor: wait.ForLog("Ready to accept connections").WithStartupTimeout(45 * time.Second),
		HostConfigModifier: func(config *dockercontainer.HostConfig) {
			config.PortBindings = nat.PortMap{"6379/tcp": []nat.PortBinding{{HostPort: redisHostPort}}}
		},
	})

	stack := &Stack{
		PostgresURL: containerURL(t, ctx, postgres, "5432/tcp", func(host, port string) string {
			return fmt.Sprintf("postgres://relayhub:%s@%s:%s/relayhub?sslmode=disable", postgresPassword, host, port)
		}),
		NATSURL: containerURL(t, ctx, natsContainer, "4222/tcp", func(host, port string) string {
			return "nats://" + host + ":" + port
		}),
		NATSUsername: natsUsername, NATSPassword: natsPassword,
		RedisAddr:     containerURL(t, ctx, redisContainer, "6379/tcp", func(host, port string) string { return host + ":" + port }),
		RedisPassword: redisPassword,
		postgres:      postgres, nats: natsContainer, redis: redisContainer, redisUp: true,
	}
	stack.verifyReady(t)
	return stack
}

func (s *Stack) StopRedis(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.redisUp {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	stopTimeout := 5 * time.Second
	if err := s.redis.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal("stop Redis test dependency failed")
	}
	s.redisUp = false
}

func (s *Stack) StartRedis(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.redisUp {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.redis.Start(ctx); err != nil {
		t.Fatal("start Redis test dependency failed")
	}
	currentAddress := containerURL(t, ctx, s.redis, "6379/tcp", func(host, port string) string { return host + ":" + port })
	if currentAddress != s.RedisAddr {
		t.Fatalf("Redis test endpoint changed across restart: %q -> %q", s.RedisAddr, currentAddress)
	}
	s.redisUp = true
}

func (s *Stack) verifyReady(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	postgres, err := pgx.Connect(ctx, s.PostgresURL)
	if err != nil {
		t.Fatal("PostgreSQL test dependency unavailable")
	}
	defer postgres.Close(context.Background())
	if err := postgres.Ping(ctx); err != nil {
		t.Fatal("PostgreSQL test dependency unavailable")
	}
	natsClient, err := nats.Connect(s.NATSURL, nats.UserInfo(s.NATSUsername, s.NATSPassword), nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatal("NATS test dependency unavailable")
	}
	defer natsClient.Close()
	if _, err := natsClient.JetStream(); err != nil {
		t.Fatal("NATS JetStream test dependency unavailable")
	}
	redisClient := redis.NewClient(&redis.Options{Addr: s.RedisAddr, Password: s.RedisPassword, DialTimeout: time.Second})
	defer redisClient.Close()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatal("Redis test dependency unavailable")
	}
}

func startContainer(t *testing.T, ctx context.Context, request testcontainers.ContainerRequest) testcontainers.Container {
	t.Helper()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: request, Started: true})
	if err != nil {
		t.Skip("private dependency containers unavailable")
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = container.Terminate(cleanup)
	})
	return container
}

func containerURL(t *testing.T, ctx context.Context, container testcontainers.Container, port string, build func(string, string) string) string {
	t.Helper()
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal("resolve test dependency host failed")
	}
	mapped, err := container.MappedPort(ctx, nat.Port(port))
	if err != nil {
		t.Fatal("resolve test dependency port failed")
	}
	return build(host, mapped.Port())
}

func randomCredential(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal("generate test credential failed")
	}
	return hex.EncodeToString(raw)
}

func reserveHostPort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("reserve test dependency port failed")
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal("release test dependency port failed")
	}
	return strconv.Itoa(port)
}
