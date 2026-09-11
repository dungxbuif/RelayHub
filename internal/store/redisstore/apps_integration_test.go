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
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

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
		if err := client.UpdateApplication(ctx, got); err != nil {
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
		keys, err := client.client.Keys(ctx, "relayhub:*").Result()
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

func integrationRedisClient(t *testing.T) *Client {
	t.Helper()
	if rawURL := os.Getenv("RELAYHUB_TEST_REDIS_URL"); rawURL != "" {
		client, err := NewClient(rawURL)
		if err != nil {
			t.Fatalf("NewClient(RELAYHUB_TEST_REDIS_URL) error = %v", err)
		}
		t.Cleanup(func() { _ = client.Close() })
		return client
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp").WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Skipf("Docker unavailable for Redis testcontainer: %v", err)
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
	client, err := NewClient(fmt.Sprintf("redis://%s:%s/0", host, port.Port()))
	if err != nil {
		t.Fatalf("NewClient(container) error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func flushIntegrationRedis(t *testing.T, client *Client) {
	t.Helper()
	if err := client.client.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("FlushDB() error = %v", err)
	}
}

func apiHash(value string) string {
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:])
}
