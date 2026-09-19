//go:build integration

package postgres

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	secretcrypto "github.com/dungxbuif/RelayHub/internal/crypto"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestPostgresControlStore(t *testing.T) {
	client := integrationPostgresClient(t)
	ctx := context.Background()
	now := time.Date(2026, 9, 12, 4, 0, 0, 0, time.UTC)

	t.Run("application CRUD CAS disable and encrypted credential rotation", func(t *testing.T) {
		resetControlTables(t, client)
		callback := "https://orders.internal/events"
		app := domain.App{ID: "app_orders", Name: "orders", CallbackURL: &callback, DeliveryMode: domain.DeliveryCallback, Enabled: true, CreatedAt: now, UpdatedAt: now}
		old := store.AppCredential{AppID: app.ID, APIKeyHash: "hash-old", HMACSecret: []byte("rhs_plaintext-old")}
		if err := client.CreateApplication(ctx, app, old); err != nil {
			t.Fatal(err)
		}
		var encrypted string
		if err := client.pool.QueryRow(ctx, `SELECT encrypted_hmac_secret FROM application_credentials WHERE app_id=$1`, app.ID).Scan(&encrypted); err != nil {
			t.Fatal(err)
		}
		if encrypted == string(old.HMACSecret) || !bytes.HasPrefix([]byte(encrypted), []byte("rhsec:v1:")) {
			t.Fatalf("secret stored without versioned encryption: %q", encrypted)
		}
		found, err := client.FindCredentialByAPIKeyHash(ctx, old.APIKeyHash)
		if err != nil || !bytes.Equal(found.HMACSecret, old.HMACSecret) {
			t.Fatalf("FindCredentialByAPIKeyHash() = %#v, %v", found, err)
		}
		current, err := client.GetApplication(ctx, app.ID)
		if err != nil || current.CallbackURL == nil || *current.CallbackURL != callback {
			t.Fatalf("GetApplication() = %#v, %v", current, err)
		}
		stale := current
		current.Name = "orders-v2"
		current.CallbackURL = nil
		current.DeliveryMode = domain.DeliveryWebSocket
		current.UpdatedAt = now.Add(time.Minute)
		updated, err := client.CompareAndSwapApplication(ctx, stale, current)
		if err != nil || updated.Name != "orders-v2" || updated.CallbackURL != nil {
			t.Fatalf("CompareAndSwapApplication() = %#v, %v", updated, err)
		}
		if _, err := client.CompareAndSwapApplication(ctx, stale, current); !errors.Is(err, store.ErrConflict) {
			t.Fatalf("stale CAS error = %v", err)
		}
		disabled, err := client.DisableApplication(ctx, app.ID, now.Add(2*time.Minute))
		if err != nil || disabled.Enabled {
			t.Fatalf("DisableApplication() = %#v, %v", disabled, err)
		}
		fresh := store.AppCredential{AppID: app.ID, APIKeyHash: "hash-new", HMACSecret: []byte("rhs_plaintext-new")}
		if err := client.RotateApplicationCredential(ctx, app.ID, fresh, now.Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
		if _, err := client.FindCredentialByAPIKeyHash(ctx, old.APIKeyHash); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("old credential lookup error = %v", err)
		}
		active, err := client.CurrentCredential(ctx, app.ID)
		if err != nil || active.Version != 2 || active.RevokedAt != nil || active.APIKeyHash != fresh.APIKeyHash {
			t.Fatalf("CurrentCredential() = %#v, %v", active, err)
		}
		var oldVersion int64
		var revokedAt *time.Time
		if err := client.pool.QueryRow(ctx, `SELECT version,revoked_at FROM application_credentials WHERE app_id=$1 AND api_key_hash=$2`, app.ID, old.APIKeyHash).Scan(&oldVersion, &revokedAt); err != nil || oldVersion != 1 || revokedAt == nil {
			t.Fatalf("old credential version=%d revoked=%v error=%v", oldVersion, revokedAt, err)
		}
	})

	t.Run("concurrent application create has one winner", func(t *testing.T) {
		resetControlTables(t, client)
		app := domain.App{ID: "app_unique", Name: "unique", DeliveryMode: domain.DeliveryWebSocket, Enabled: true, CreatedAt: now, UpdatedAt: now}
		credential := store.AppCredential{AppID: app.ID, APIKeyHash: "hash-unique", HMACSecret: []byte("secret")}
		var success, conflicts atomic.Int64
		var workers sync.WaitGroup
		for range 12 {
			workers.Add(1)
			go func() {
				defer workers.Done()
				err := client.CreateApplication(ctx, app, credential)
				if err == nil {
					success.Add(1)
				} else if errors.Is(err, store.ErrConflict) {
					conflicts.Add(1)
				}
			}()
		}
		workers.Wait()
		if success.Load() != 1 || conflicts.Load() != 11 {
			t.Fatalf("success=%d conflicts=%d", success.Load(), conflicts.Load())
		}
	})

	t.Run("function ownership and append-only audit", func(t *testing.T) {
		resetControlTables(t, client)
		for _, id := range []string{"app_owner", "app_other"} {
			app := domain.App{ID: id, Name: id, DeliveryMode: domain.DeliveryWebSocket, Enabled: true, CreatedAt: now, UpdatedAt: now}
			if err := client.CreateApplication(ctx, app, store.AppCredential{AppID: id, APIKeyHash: "hash-" + id, HMACSecret: []byte("secret")}); err != nil {
				t.Fatal(err)
			}
		}
		function := domain.Function{ID: "fn_1", AppID: "app_owner", Name: "calculate", TimeoutSeconds: 10, Enabled: true, CreatedAt: now, UpdatedAt: now}
		if err := client.CreateFunction(ctx, function); err != nil {
			t.Fatal(err)
		}
		if err := client.CreateFunction(ctx, function); !errors.Is(err, store.ErrConflict) {
			t.Fatalf("duplicate function error = %v", err)
		}
		listed, err := client.ListFunctions(ctx, "app_owner")
		if err != nil || len(listed) != 1 || listed[0].ID != function.ID {
			t.Fatalf("ListFunctions() = %#v, %v", listed, err)
		}
		if err := client.DeleteFunction(ctx, "app_other", function.ID); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("cross-owner delete error = %v", err)
		}
		var before int
		if err := client.pool.QueryRow(ctx, `SELECT count(*) FROM audit_log`).Scan(&before); err != nil {
			t.Fatal(err)
		}
		metadata := json.RawMessage(`{"ip":"10.0.0.5"}`)
		if err := client.AppendAudit(ctx, AuditEntry{OccurredAt: now, ActorType: "admin", ActorID: "operator", Action: "function.create", ResourceType: "function", ResourceID: function.ID, Outcome: "success", Metadata: metadata}); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := client.pool.QueryRow(ctx, `SELECT count(*) FROM audit_log`).Scan(&count); err != nil || count != before+1 {
			t.Fatalf("audit count=%d error=%v", count, err)
		}
		if _, err := client.pool.Exec(ctx, `UPDATE audit_log SET action='tampered'`); err == nil {
			t.Fatal("audit_log UPDATE succeeded")
		}
		var action string
		if err := client.pool.QueryRow(ctx, `SELECT action FROM audit_log`).Scan(&action); err != nil || action != "function.create" {
			// Older rows may exist because append-only protection intentionally
			// prevents test cleanup. Inspect the newest row instead.
			if err := client.pool.QueryRow(ctx, `SELECT action FROM audit_log ORDER BY id DESC LIMIT 1`).Scan(&action); err != nil || action != "function.create" {
				t.Fatalf("audit action=%q error=%v", action, err)
			}
		}
		if _, err := client.pool.Exec(ctx, `TRUNCATE audit_log`); err == nil {
			t.Fatal("audit_log TRUNCATE succeeded")
		}
	})

	t.Run("migration rejects schema newer than binary", func(t *testing.T) {
		if _, err := client.pool.Exec(ctx, `INSERT INTO schema_migrations(version,name,checksum) VALUES(999,'future','future')`); err != nil {
			t.Fatal(err)
		}
		if err := client.Migrate(ctx); err == nil || !strings.Contains(err.Error(), "newer than this RelayHub binary") {
			t.Fatalf("Migrate() error = %v", err)
		}
		if _, err := client.pool.Exec(ctx, `DELETE FROM schema_migrations WHERE version=999`); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("failed migration rolls back schema and ledger", func(t *testing.T) {
		connection, err := client.pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Release()
		item := migration{Version: 999, Name: "rollback_probe", Checksum: "test", SQL: []byte(`CREATE TABLE rollback_probe(id integer); SELECT missing_migration_function();`)}
		if err := applyMigration(ctx, connection.Conn(), item); err == nil {
			t.Fatal("applyMigration(invalid SQL) succeeded")
		}
		var relation *string
		if err := connection.QueryRow(ctx, `SELECT to_regclass('rollback_probe')::text`).Scan(&relation); err != nil {
			t.Fatal(err)
		}
		if relation != nil {
			t.Fatalf("rollback_probe still exists: %q", *relation)
		}
		var count int
		if err := connection.QueryRow(ctx, `SELECT count(*) FROM schema_migrations WHERE version=999`).Scan(&count); err != nil || count != 0 {
			t.Fatalf("migration ledger count=%d error=%v", count, err)
		}
	})
}

func integrationPostgresClient(t *testing.T) *Client {
	t.Helper()
	rawURL := os.Getenv("RELAYHUB_TEST_POSTGRES_URL")
	var container testcontainers.Container
	if rawURL == "" {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		var err error
		container, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
			Image: "postgres:17-alpine", ExposedPorts: []string{"5432/tcp"}, Env: map[string]string{"POSTGRES_USER": "relayhub", "POSTGRES_PASSWORD": "relayhub-test", "POSTGRES_DB": "relayhub"}, WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(45 * time.Second),
		}, Started: true})
		if err != nil {
			t.Skipf("PostgreSQL testcontainer unavailable: %v", err)
		}
		host, _ := container.Host(ctx)
		port, _ := container.MappedPort(ctx, "5432/tcp")
		rawURL = fmt.Sprintf("postgres://relayhub:relayhub-test@%s:%s/relayhub?sslmode=disable", host, port.Port())
		t.Cleanup(func() {
			stop, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = container.Terminate(stop)
		})
	}
	cipher, err := secretcrypto.NewSecretCipher(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{9}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(context.Background(), Config{DatabaseURL: rawURL, MaxConnections: 16, MinConnections: 1}, cipher)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := client.Migrate(context.Background()); err != nil {
		t.Fatalf("repeat Migrate() error = %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

func resetControlTables(t *testing.T, client *Client) {
	t.Helper()
	if _, err := client.pool.Exec(context.Background(), `TRUNCATE functions, callback_endpoints, application_credentials, applications RESTART IDENTITY CASCADE`); err != nil {
		t.Fatal(err)
	}
}
