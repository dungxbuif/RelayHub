//go:build integration

package teststack

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)

func TestStackStartsAllPrivateDependencies(t *testing.T) {
	stack := Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	postgres, err := pgx.Connect(ctx, stack.PostgresURL)
	if err != nil {
		t.Fatal("PostgreSQL connection failed")
	}
	t.Cleanup(func() { _ = postgres.Close(context.Background()) })
	if err := postgres.Ping(ctx); err != nil {
		t.Fatal("PostgreSQL ping failed")
	}

	natsClient, err := nats.Connect(stack.NATSURL, nats.UserInfo(stack.NATSUsername, stack.NATSPassword), nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatal("NATS connection failed")
	}
	t.Cleanup(natsClient.Close)
	if _, err := natsClient.JetStream(); err != nil || !natsClient.IsConnected() {
		t.Fatal("NATS JetStream unavailable")
	}

	redisClient := redis.NewClient(&redis.Options{Addr: stack.RedisAddr, Password: stack.RedisPassword, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
	t.Cleanup(func() { _ = redisClient.Close() })
	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatal("Redis ping failed")
	}

	stack.StopRedis(t)
	failedCtx, failedCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer failedCancel()
	if err := redisClient.Ping(failedCtx).Err(); err == nil {
		t.Fatal("Redis ping succeeded while container was stopped")
	}
	if err := postgres.Ping(ctx); err != nil || !natsClient.IsConnected() {
		t.Fatal("stopping Redis affected a durable dependency")
	}

	stack.StartRedis(t)
	eventually(t, 10*time.Second, func() bool {
		pingCtx, pingCancel := context.WithTimeout(context.Background(), time.Second)
		defer pingCancel()
		return redisClient.Ping(pingCtx).Err() == nil
	})
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("condition did not become true before timeout")
}
