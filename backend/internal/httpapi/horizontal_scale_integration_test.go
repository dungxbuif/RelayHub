//go:build integration

package httpapi_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsbroker "github.com/dungxbuif/RelayHub/internal/broker/nats"
	"github.com/dungxbuif/RelayHub/internal/config"
	secretcrypto "github.com/dungxbuif/RelayHub/internal/crypto"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/outbox"
	"github.com/dungxbuif/RelayHub/internal/platform"
	"github.com/dungxbuif/RelayHub/internal/redisstate"
	runtimegraph "github.com/dungxbuif/RelayHub/internal/runtime"
	"github.com/dungxbuif/RelayHub/internal/store"
	postgresstore "github.com/dungxbuif/RelayHub/internal/store/postgres"
	"github.com/dungxbuif/RelayHub/internal/teststack"
	"github.com/nats-io/nats.go/jetstream"
)

func TestHorizontalScaleFoundation(t *testing.T) {
	stack := teststack.Start(t)
	cfg := scaleConfig(stack)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelA()
	defer cancelB()

	apiDepsA := scaleDependencies(t, ctxA, stack, cfg, "api_a")
	apiDepsB := scaleDependencies(t, ctxB, stack, cfg, "api_b")
	workerDepsA := scaleDependencies(t, ctxA, stack, cfg, "worker_a")
	workerDepsB := scaleDependencies(t, ctxB, stack, cfg, "worker_b")
	apiA, err := runtimegraph.NewAPI(ctxA, cfg, apiDepsA, logger)
	if err != nil {
		t.Fatal(err)
	}
	apiB, err := runtimegraph.NewAPI(ctxB, cfg, apiDepsB, logger)
	if err != nil {
		t.Fatal(err)
	}
	workerA, err := runtimegraph.NewWorker(ctxA, cfg, workerDepsA, logger)
	if err != nil {
		t.Fatal(err)
	}
	workerB, err := runtimegraph.NewWorker(ctxB, cfg, workerDepsB, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer apiA.Close(context.Background())
	defer apiB.Close(context.Background())
	defer workerA.Close(context.Background())
	defer workerB.Close(context.Background())

	now := time.Now().UTC().Truncate(time.Millisecond)
	for i := 0; i < 40; i++ {
		session := redisstate.AdminSession{
			ID: "sess_" + twoDigits(i), CSRFHash: "csrf", IssuedAt: now, LastSeenAt: now,
			IdleExpiresAt: now.Add(time.Minute), ExpiresAt: now.Add(2 * time.Minute),
		}
		writer, reader := apiA, apiB
		if i%2 == 1 {
			writer, reader = apiB, apiA
		}
		if err := writer.Sessions.Put(context.Background(), session); err != nil {
			t.Fatal(err)
		}
		if got, err := reader.Sessions.Get(context.Background(), session.ID); err != nil || got != session {
			t.Fatalf("cross-replica session %d = %+v, %v", i, got, err)
		}
	}

	limitKey, err := (redisstate.Keyspace{Prefix: "rh"}).RateLimit("app_scale", "publish", "minute")
	if err != nil {
		t.Fatal(err)
	}
	var allowed atomic.Int64
	var requests sync.WaitGroup
	errorsSeen := make(chan error, 200)
	for i := 0; i < 200; i++ {
		requests.Add(1)
		go func(i int) {
			defer requests.Done()
			limiter := apiA.Limiter
			if i%2 == 1 {
				limiter = apiB.Limiter
			}
			decision, err := limiter.Allow(context.Background(), limitKey, 73, time.Minute, 1)
			if err != nil {
				errorsSeen <- err
				return
			}
			if decision.Allowed {
				allowed.Add(1)
			}
		}(i)
	}
	requests.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Fatal(err)
	}
	if allowed.Load() != 73 {
		t.Fatalf("distributed limiter allowed %d, want 73", allowed.Load())
	}

	if err := apiA.Ownership.Claim(context.Background(), "app_scale", "conn_scale", apiDepsA.Instance.ID, apiDepsA.Instance.Generation, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := apiB.Ownership.Claim(context.Background(), "app_scale", "conn_scale", apiDepsB.Instance.ID, apiDepsB.Instance.Generation, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := apiA.Ownership.Refresh(context.Background(), "app_scale", "conn_scale", apiDepsA.Instance.ID, apiDepsA.Instance.Generation, time.Minute); !errors.Is(err, redisstate.ErrOwnershipLost) {
		t.Fatalf("stale replica refresh error = %v", err)
	}
	if err := apiA.Ownership.Release(context.Background(), "app_scale", "conn_scale", apiDepsA.Instance.ID, apiDepsA.Instance.Generation); !errors.Is(err, redisstate.ErrOwnershipLost) {
		t.Fatalf("stale replica release error = %v", err)
	}

	publishDurableEvent(t, apiDepsA.Postgres, now)
	proveOneOutboxDispatch(t, ctxA, workerDepsA, workerDepsB)

	stack.StopRedis(t)
	for name, ready := range map[string]func(context.Context) error{"api-a": apiA.Ready, "api-b": apiB.Ready, "worker-a": workerA.Ready, "worker-b": workerB.Ready} {
		readyCtx, readyCancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := ready(readyCtx)
		readyCancel()
		if err == nil {
			t.Fatalf("%s remained ready during Redis outage", name)
		}
	}
	if _, err := apiDepsB.Postgres.GetEvent(context.Background(), "evt_scale"); err != nil {
		t.Fatalf("durable event disappeared during Redis outage: %v", err)
	}

	stack.StartRedis(t)
	eventuallyScale(t, 15*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return apiA.Ready(ctx) == nil && apiB.Ready(ctx) == nil && workerA.Ready(ctx) == nil && workerB.Ready(ctx) == nil
	})
	cancelA()
	eventuallyScale(t, 3*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return apiB.Ready(ctx) == nil && workerB.Ready(ctx) == nil
	})
	if _, err := apiDepsB.Postgres.GetEvent(context.Background(), "evt_scale"); err != nil {
		t.Fatalf("durable event disappeared after replica cancellation: %v", err)
	}
}

func scaleConfig(stack *teststack.Stack) config.Config {
	return config.Config{
		Redis:            config.RedisConfig{Mode: "standalone", Addrs: []string{stack.RedisAddr}, Password: stack.RedisPassword, KeyPrefix: "rh", ConnectTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second, PoolSize: 8},
		NATSStreamMaxAge: 24 * time.Hour, NATSDuplicateWindow: time.Minute, NATSReplicas: 1,
	}
}

func scaleDependencies(t *testing.T, ctx context.Context, stack *teststack.Stack, cfg config.Config, id string) runtimegraph.Dependencies {
	t.Helper()
	cipher, err := secretcrypto.NewSecretCipher(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	postgres, err := postgresstore.NewClient(ctx, postgresstore.Config{DatabaseURL: stack.PostgresURL, MaxConnections: 8, MinConnections: 1}, cipher)
	if err != nil {
		t.Fatal("connect PostgreSQL dependency failed")
	}
	if err := postgres.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(postgres.Close)
	natsClient, err := natsbroker.Connect(natsbroker.Options{URL: stack.NATSURL, Username: stack.NATSUsername, Password: stack.NATSPassword, Name: id, ConnectTimeout: 2 * time.Second, ReconnectWait: 100 * time.Millisecond, MaxReconnects: -1, DrainTimeout: time.Second})
	if err != nil {
		t.Fatal("connect NATS dependency failed")
	}
	if err := natsClient.Bootstrap(ctx, natsbroker.StreamSettings{MaxAge: cfg.NATSStreamMaxAge, DuplicateWindow: cfg.NATSDuplicateWindow, Replicas: 1}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(natsClient.Close)
	redisClient, err := redisstate.New(ctx, redisstate.Config{Mode: redisstate.ModeStandalone, Addrs: []string{stack.RedisAddr}, Password: stack.RedisPassword, KeyPrefix: "rh", ConnectTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second, PoolSize: 8})
	if err != nil {
		t.Fatal("connect Redis dependency failed")
	}
	t.Cleanup(func() { _ = redisClient.Close() })
	instance, err := platform.NewInstance(id[:len(id)-2], id, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	return runtimegraph.Dependencies{Postgres: postgres, NATS: natsClient, Redis: redisClient, Instance: instance}
}

func publishDurableEvent(t *testing.T, postgres *postgresstore.Client, now time.Time) {
	t.Helper()
	for _, app := range []domain.App{
		{ID: "app_source", Name: "source", DeliveryMode: domain.DeliveryWebSocket, Enabled: true, CreatedAt: now, UpdatedAt: now},
		{ID: "app_target", Name: "target", DeliveryMode: domain.DeliveryQueue, Enabled: true, CreatedAt: now, UpdatedAt: now},
	} {
		if err := postgres.CreateApplication(context.Background(), app, store.AppCredential{AppID: app.ID, APIKeyHash: "hash_" + app.ID, HMACSecret: []byte("secret")}); err != nil {
			t.Fatal(err)
		}
	}
	publication := store.Publication{
		Event: domain.Event{ID: "evt_scale", Type: "scale.test", SourceAppID: "app_source", TargetAppIDs: []string{"app_target"}, Data: json.RawMessage(`{"ok":true}`), CreatedAt: now},
		Jobs:  []domain.Job{{ID: "job_scale", EventID: "evt_scale", SourceAppID: "app_source", TargetAppID: "app_target", Status: domain.JobPending, CreatedAt: now, UpdatedAt: now}},
	}
	if _, replay, err := postgres.PublishEvent(context.Background(), publication, "scale-key", store.EventRetention{Event: time.Hour, Job: time.Hour, Idempotency: time.Hour}); err != nil || replay {
		t.Fatalf("PublishEvent() replay=%v error=%v", replay, err)
	}
}

func proveOneOutboxDispatch(t *testing.T, parent context.Context, first, second runtimegraph.Dependencies) {
	t.Helper()
	firstDispatcher, err := outbox.NewDispatcher(first.Postgres, first.NATS, outbox.Options{BatchSize: 10, ClaimTTL: 2 * time.Second, BaseRetry: 10 * time.Millisecond, MaxRetry: time.Second, MaxAttempts: 3, MaxPendingAge: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	secondDispatcher, err := outbox.NewDispatcher(second.Postgres, second.NATS, outbox.Options{BatchSize: 10, ClaimTTL: 2 * time.Second, BaseRetry: 10 * time.Millisecond, MaxRetry: time.Second, MaxAttempts: 3, MaxPendingAge: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan error, 2)
	go func() { done <- firstDispatcher.Run(ctx, 10*time.Millisecond) }()
	go func() { done <- secondDispatcher.Run(ctx, 10*time.Millisecond) }()
	eventuallyScale(t, 10*time.Second, func() bool {
		stats, err := first.Postgres.OutboxStats(context.Background())
		return err == nil && stats.Pending == 0 && stats.Claimed == 0
	})
	cancel()
	<-done
	<-done
	js, err := jetstream.New(first.NATS.Conn())
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(context.Background(), "RH_DELIVERIES")
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("delivery stream messages = %d, want exactly 1", info.State.Msgs)
	}
}

func eventuallyScale(t *testing.T, timeout time.Duration, condition func() bool) {
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

func twoDigits(value int) string {
	return string(rune('0'+value/10)) + string(rune('0'+value%10))
}
