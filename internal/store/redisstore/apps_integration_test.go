//go:build integration

package redisstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var errDockerUnavailable = errors.New("Docker unavailable")

func TestApplicationStaleMutationPreservesLatestTimestamp(t *testing.T) {
	c := integrationRedisClient(t)
	ctx := context.Background()
	// Whole seconds and fractional seconds catch RFC3339Nano lexical ordering.
	for _, fraction := range []time.Duration{0, 100 * time.Millisecond} {
		now := time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC).Add(fraction)
		id := fmt.Sprint("app_monotonic_", fraction)
		app := domain.App{ID: id, Name: "before", Enabled: true, DeliveryMode: domain.DeliveryQueue, CreatedAt: now, UpdatedAt: now}
		if err := c.CreateApplication(ctx, app, store.AppCredential{AppID: id, APIKeyHash: id, HMACSecret: []byte("fixture")}); err != nil {
			t.Fatal(err)
		}
		latest := now.Add(100 * time.Millisecond)
		if _, err := c.DisableApplication(ctx, id, latest); err != nil {
			t.Fatal(err)
		}
		app.Name = "after"
		updated, err := c.UpdateApplication(ctx, app)
		if err != nil || updated.Enabled || !updated.UpdatedAt.Equal(latest) || updated.Name != "after" {
			t.Fatalf("stale update regressed metadata: %#v %v", updated, err)
		}
		if err := c.RotateApplicationCredential(ctx, id, store.AppCredential{AppID: id, APIKeyHash: id + "rotated", HMACSecret: []byte("fixture")}, now); err != nil {
			t.Fatal(err)
		}
		updated, err = c.DisableApplication(ctx, id, now)
		if err != nil || !updated.UpdatedAt.Equal(latest) {
			t.Fatalf("stale mutation regressed timestamp: %#v %v", updated, err)
		}
	}
}

func TestApplicationPersistenceAndCredentialIndexes(t *testing.T) {
	client := integrationRedisClient(t)
	ctx := context.Background()
	now := time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC)

	t.Run("CRUD and disable", func(t *testing.T) {
		flushIntegrationRedis(t, client)
		callback := "https://orders.internal/events"
		app := domain.App{ID: "app_crud", Name: "orders", CallbackURL: &callback, DeliveryMode: domain.DeliveryQueue, Enabled: true, CreatedAt: now, UpdatedAt: now}
		credential := store.AppCredential{AppID: app.ID, APIKeyHash: apiHash("crud-key"), HMACSecret: []byte("crud-secret")}
		if err := client.CreateApplication(ctx, app, credential); err != nil {
			t.Fatalf("CreateApplication() error = %v", err)
		}
		got, err := client.GetApplication(ctx, app.ID)
		if err != nil || got.ID != app.ID || got.CallbackURL == nil || *got.CallbackURL != callback {
			t.Fatalf("GetApplication() = %#v, error = %v", got, err)
		}
		listed, err := client.ListApplications(ctx)
		if err != nil || len(listed) != 1 || listed[0].ID != app.ID {
			t.Fatalf("ListApplications() = %#v, error = %v", listed, err)
		}

		got.Name = "orders-v2"
		got.DeliveryMode = domain.DeliveryWebSocket
		got.CallbackURL = nil
		got.UpdatedAt = now.Add(time.Minute)
		if _, err := client.UpdateApplication(ctx, got); err != nil {
			t.Fatalf("UpdateApplication() error = %v", err)
		}
		updated, err := client.GetApplication(ctx, app.ID)
		if err != nil || updated.Name != "orders-v2" || updated.DeliveryMode != domain.DeliveryWebSocket || updated.CallbackURL != nil {
			t.Fatalf("updated app = %#v, error = %v", updated, err)
		}
		disabled, err := client.DisableApplication(ctx, app.ID, now.Add(2*time.Minute))
		if err != nil || disabled.Enabled {
			t.Fatalf("DisableApplication() = %#v, error = %v", disabled, err)
		}
	})

	t.Run("update missing application returns not found", func(t *testing.T) {
		flushIntegrationRedis(t, client)
		missing := domain.App{
			ID:           "app_missing",
			Name:         "missing",
			DeliveryMode: domain.DeliveryQueue,
			Enabled:      true,
			CreatedAt:    now,
			UpdatedAt:    now,
		}
		if _, err := client.UpdateApplication(ctx, missing); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("UpdateApplication(missing) error = %v, want store.ErrNotFound", err)
		}
	})

	t.Run("API key is hash indexed without plaintext", func(t *testing.T) {
		flushIntegrationRedis(t, client)
		const apiKey = "rhk_plaintext-must-never-be-stored"
		app := domain.App{ID: "app_hash", Name: "hash-test", DeliveryMode: domain.DeliveryQueue, Enabled: true, CreatedAt: now, UpdatedAt: now}
		credential := store.AppCredential{AppID: app.ID, APIKeyHash: apiHash(apiKey), HMACSecret: []byte("stored-hmac-secret")}
		if err := client.CreateApplication(ctx, app, credential); err != nil {
			t.Fatalf("CreateApplication() error = %v", err)
		}
		found, err := client.FindCredentialByAPIKeyHash(ctx, apiHash(apiKey))
		if err != nil || found.AppID != app.ID || string(found.HMACSecret) != "stored-hmac-secret" {
			t.Fatalf("FindCredentialByAPIKeyHash() = %#v, error = %v", found, err)
		}
		keys, err := client.client.Keys(ctx, client.key("*")).Result()
		if err != nil {
			t.Fatalf("KEYS error = %v", err)
		}
		allStored := strings.Join(keys, "\n")
		for _, key := range keys {
			typeName, err := client.client.Type(ctx, key).Result()
			if err != nil {
				t.Fatalf("TYPE %s error = %v", key, err)
			}
			switch typeName {
			case "string":
				value, _ := client.client.Get(ctx, key).Result()
				allStored += "\n" + value
			case "hash":
				values, _ := client.client.HGetAll(ctx, key).Result()
				for field, value := range values {
					allStored += "\n" + field + "\n" + value
				}
			}
		}
		if strings.Contains(allStored, apiKey) {
			t.Fatalf("Redis stored plaintext API key in keys or values: %s", allStored)
		}
	})

	t.Run("rotation swaps index atomically", func(t *testing.T) {
		flushIntegrationRedis(t, client)
		app := domain.App{ID: "app_rotate", Name: "rotate-test", DeliveryMode: domain.DeliveryQueue, Enabled: true, CreatedAt: now, UpdatedAt: now}
		oldCredential := store.AppCredential{AppID: app.ID, APIKeyHash: apiHash("old-key"), HMACSecret: []byte("old-secret")}
		newCredential := store.AppCredential{AppID: app.ID, APIKeyHash: apiHash("new-key"), HMACSecret: []byte("new-secret")}
		if err := client.CreateApplication(ctx, app, oldCredential); err != nil {
			t.Fatalf("CreateApplication() error = %v", err)
		}
		if err := client.RotateApplicationCredential(ctx, app.ID, newCredential, now.Add(time.Minute)); err != nil {
			t.Fatalf("RotateApplicationCredential() error = %v", err)
		}
		if _, err := client.FindCredentialByAPIKeyHash(ctx, oldCredential.APIKeyHash); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("old credential lookup error = %v, want ErrNotFound", err)
		}
		found, err := client.FindCredentialByAPIKeyHash(ctx, newCredential.APIKeyHash)
		if err != nil || found.AppID != app.ID || string(found.HMACSecret) != "new-secret" {
			t.Fatalf("new credential lookup = %#v, error = %v", found, err)
		}
	})

	t.Run("stale configuration update preserves disable", func(t *testing.T) {
		flushIntegrationRedis(t, client)
		app := domain.App{ID: "app_disable_wins", Name: "before", DeliveryMode: domain.DeliveryQueue, Enabled: true, CreatedAt: now, UpdatedAt: now}
		credential := store.AppCredential{AppID: app.ID, APIKeyHash: apiHash("disable-wins-key"), HMACSecret: []byte("disable-wins-secret")}
		if err := client.CreateApplication(ctx, app, credential); err != nil {
			t.Fatalf("CreateApplication() error = %v", err)
		}
		stale, err := client.GetApplication(ctx, app.ID)
		if err != nil {
			t.Fatalf("GetApplication() error = %v", err)
		}
		if _, err := client.DisableApplication(ctx, app.ID, now.Add(time.Minute)); err != nil {
			t.Fatalf("DisableApplication() error = %v", err)
		}
		stale.Name = "after"
		stale.UpdatedAt = now.Add(2 * time.Minute)
		updated, err := client.UpdateApplication(ctx, stale)
		if err != nil {
			t.Fatalf("UpdateApplication(stale) error = %v", err)
		}
		if updated.Enabled {
			t.Fatal("UpdateApplication(stale) re-enabled disabled application")
		}
		stored, err := client.GetApplication(ctx, app.ID)
		if err != nil || stored.Enabled || stored.Name != "after" {
			t.Fatalf("GetApplication() = %#v, error = %v; want updated name and disabled state", stored, err)
		}
	})

	t.Run("concurrent create has one winner", func(t *testing.T) {
		flushIntegrationRedis(t, client)
		app := domain.App{ID: "app_unique", Name: "unique-test", DeliveryMode: domain.DeliveryQueue, Enabled: true, CreatedAt: now, UpdatedAt: now}
		credential := store.AppCredential{AppID: app.ID, APIKeyHash: apiHash("unique-key"), HMACSecret: []byte("unique-secret")}
		var success atomic.Int64
		var conflicts atomic.Int64
		var unexpectedMu sync.Mutex
		var unexpected []error
		var workers sync.WaitGroup
		for range 16 {
			workers.Add(1)
			go func() {
				defer workers.Done()
				err := client.CreateApplication(ctx, app, credential)
				switch {
				case err == nil:
					success.Add(1)
				case errors.Is(err, store.ErrConflict):
					conflicts.Add(1)
				default:
					unexpectedMu.Lock()
					unexpected = append(unexpected, err)
					unexpectedMu.Unlock()
				}
			}()
		}
		workers.Wait()
		if len(unexpected) != 0 || success.Load() != 1 || conflicts.Load() != 15 {
			t.Fatalf("concurrent create successes=%d conflicts=%d unexpected=%v, want 1/15/none", success.Load(), conflicts.Load(), unexpected)
		}
	})
}

func TestRedisContainerProvisioningOnlyClassifiesDockerProbeFailuresAsUnavailable(t *testing.T) {
	ctx := context.Background()
	startCalled := false
	_, err := provisionRedisContainer(
		ctx,
		func(context.Context) error { return errors.New("daemon unavailable") },
		func(context.Context) (testcontainers.Container, error) {
			startCalled = true
			return nil, nil
		},
	)
	if !errors.Is(err, errDockerUnavailable) {
		t.Fatalf("probe failure error = %v, want errDockerUnavailable", err)
	}
	if startCalled {
		t.Fatal("container start ran after Docker availability probe failed")
	}

	startupFailure := errors.New("image pull failed")
	_, err = provisionRedisContainer(
		ctx,
		func(context.Context) error { return nil },
		func(context.Context) (testcontainers.Container, error) { return nil, startupFailure },
	)
	if !errors.Is(err, startupFailure) {
		t.Fatalf("startup failure error = %v, want wrapped startup failure", err)
	}
	if errors.Is(err, errDockerUnavailable) {
		t.Fatalf("startup failure error = %v, must not be classified as unavailable Docker", err)
	}
}

func integrationRedisClient(t *testing.T) *Client {
	t.Helper()
	prefix := "test_" + uuid.NewString()
	if rawURL := os.Getenv("RELAYHUB_TEST_REDIS_URL"); rawURL != "" {
		client, err := NewClientWithPrefix(rawURL, prefix)
		if err != nil {
			t.Fatalf("NewClient(RELAYHUB_TEST_REDIS_URL) error = %v", err)
		}
		t.Cleanup(func() { flushIntegrationRedis(t, client); _ = client.Close() })
		return client
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	container, err := provisionRedisContainer(ctx, dockerHealth, func(ctx context.Context) (testcontainers.Container, error) {
		return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        "redis:7-alpine",
				ExposedPorts: []string{"6379/tcp"},
				WaitingFor:   wait.ForListeningPort("6379/tcp").WithStartupTimeout(30 * time.Second),
			},
			Started: true,
		})
	})
	if err != nil {
		if errors.Is(err, errDockerUnavailable) {
			t.Skipf("Docker unavailable for Redis testcontainer: %v", err)
		}
		t.Fatalf("provision Redis testcontainer: %v", err)
	}
	t.Cleanup(func() {
		stopContext, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		_ = testcontainers.TerminateContainer(container, testcontainers.StopContext(stopContext))
	})
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("Redis container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "6379/tcp")
	if err != nil {
		t.Fatalf("Redis container port: %v", err)
	}
	client, err := NewClientWithPrefix(fmt.Sprintf("redis://%s:%s/0", host, port.Port()), prefix)
	if err != nil {
		t.Fatalf("NewClient(container) error = %v", err)
	}
	t.Cleanup(func() { flushIntegrationRedis(t, client); _ = client.Close() })
	return client
}

func provisionRedisContainer(
	ctx context.Context,
	probe func(context.Context) error,
	start func(context.Context) (testcontainers.Container, error),
) (testcontainers.Container, error) {
	if err := probe(ctx); err != nil {
		return nil, fmt.Errorf("%w: %v", errDockerUnavailable, err)
	}
	container, err := start(ctx)
	if err != nil {
		return nil, fmt.Errorf("start Redis testcontainer: %w", err)
	}
	return container, nil
}

func dockerHealth(ctx context.Context) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("Docker provider panic: %v", recovered)
		}
	}()
	client, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return err
	}
	defer client.Close()
	_, err = client.Info(ctx)
	return err
}

func flushIntegrationRedis(t *testing.T, client *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var cursor uint64
	for {
		keys, next, err := client.client.Scan(ctx, cursor, client.key("*"), 100).Result()
		if err != nil {
			t.Errorf("scan owned namespace: %v", err)
			return
		}
		if len(keys) > 0 {
			if err := client.client.Del(ctx, keys...).Err(); err != nil {
				t.Errorf("clean owned namespace: %v", err)
				return
			}
		}
		cursor = next
		if cursor == 0 {
			return
		}
	}
}

func derivedIntegrationClient(t *testing.T, base *Client, suffix string) *Client {
	t.Helper()
	c := &Client{client: base.client, prefix: base.prefix + "_" + suffix, jobRetention: base.jobRetention}
	t.Cleanup(func() { flushIntegrationRedis(t, c) })
	return c
}

func integrationRedisURL(c *Client) string {
	if raw := os.Getenv("RELAYHUB_TEST_REDIS_URL"); raw != "" {
		return raw
	}
	return fmt.Sprintf("redis://%s/%d", c.client.Options().Addr, c.client.Options().DB)
}

func apiHash(value string) string {
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:])
}
