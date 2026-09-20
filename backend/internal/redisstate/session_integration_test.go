//go:build integration

package redisstate

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestAdminSessionValidateAndTouch(t *testing.T) {
	address, password := integrationRedis(t)
	writer := NewRedisSessionStore(integrationClient(t, address, password), Keyspace{Prefix: "rh"})
	reader := NewRedisSessionStore(integrationClient(t, address, password), Keyspace{Prefix: "rh"})
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	session := AdminSession{
		ID: "sess_1", CSRFHash: "csrf-hash", IssuedAt: now, LastSeenAt: now,
		IdleExpiresAt: now.Add(250 * time.Millisecond), ExpiresAt: now.Add(700 * time.Millisecond),
	}

	if err := writer.Put(ctx, session); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	time.Sleep(75 * time.Millisecond)
	got, err := reader.ValidateAndTouch(ctx, session.ID, time.Now(), 500*time.Millisecond)
	if err != nil {
		t.Fatalf("ValidateAndTouch() error = %v", err)
	}
	if got.ID != session.ID || got.CSRFHash != session.CSRFHash || !got.IdleExpiresAt.After(session.IdleExpiresAt) {
		t.Fatalf("ValidateAndTouch() = %+v, want extended session", got)
	}
	if got.IdleExpiresAt.After(session.ExpiresAt) || got.ExpiresAt != session.ExpiresAt {
		t.Fatalf("touch exceeded absolute expiry: %+v", got)
	}

	eventually(t, time.Second, func() bool {
		_, err := reader.ValidateAndTouch(ctx, session.ID, time.Now(), time.Minute)
		return errors.Is(err, ErrNotFound)
	})
	if _, err := reader.ValidateAndTouch(ctx, session.ID, time.Now(), time.Minute); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired session resurrected: %v", err)
	}
}

func TestAdminSessionConcurrentTouchAndRevocation(t *testing.T) {
	address, password := integrationRedis(t)
	store := NewRedisSessionStore(integrationClient(t, address, password), Keyspace{Prefix: "rh"})
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	session := AdminSession{
		ID: "sess_concurrent", CSRFHash: "csrf-hash", IssuedAt: now, LastSeenAt: now,
		IdleExpiresAt: now.Add(time.Second), ExpiresAt: now.Add(3 * time.Second),
	}
	if err := store.Put(ctx, session); err != nil {
		t.Fatal(err)
	}

	var group sync.WaitGroup
	errorsCh := make(chan error, 50)
	for range 50 {
		group.Add(1)
		go func() {
			defer group.Done()
			got, err := store.ValidateAndTouch(ctx, session.ID, time.Now(), 2*time.Second)
			if err == nil && (got.IdleExpiresAt.After(got.ExpiresAt) || got.LastSeenAt.Before(session.LastSeenAt)) {
				err = errors.New("invalid touched bounds")
			}
			errorsCh <- err
		}()
	}
	group.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := store.Delete(ctx, session.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ValidateAndTouch(ctx, session.ID, time.Now(), time.Minute); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ValidateAndTouch() after Delete error = %v, want ErrNotFound", err)
	}
}

func TestSessionStoreDeletesMalformedJSON(t *testing.T) {
	address, password := integrationRedis(t)
	client := integrationClient(t, address, password)
	keys := Keyspace{Prefix: "rh"}
	store := NewRedisSessionStore(client, keys)
	ctx := context.Background()
	key, err := keys.AdminSession("sess_corrupt")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Universal().Set(ctx, key, `{not-json`, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}

	if _, err := store.ValidateAndTouch(ctx, "sess_corrupt", time.Now(), time.Minute); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("ValidateAndTouch() error = %v, want ErrCorruptRecord", err)
	}
	if exists, err := client.Universal().Exists(ctx, key).Result(); err != nil || exists != 0 {
		t.Fatalf("corrupt key exists = %d, error = %v", exists, err)
	}

	mismatchKey, err := keys.AdminSession("sess_mismatch")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	mismatch := AdminSession{
		ID: "another_session", CSRFHash: "csrf", IssuedAt: now, LastSeenAt: now,
		IdleExpiresAt: now.Add(time.Minute), ExpiresAt: now.Add(time.Hour),
	}
	payload, err := json.Marshal(encodeAdminSession(mismatch))
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Universal().Set(ctx, mismatchKey, payload, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ValidateAndTouch(ctx, "sess_mismatch", time.Now(), time.Minute); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("mismatched session error = %v, want ErrCorruptRecord", err)
	}
	if exists, err := client.Universal().Exists(ctx, mismatchKey).Result(); err != nil || exists != 0 {
		t.Fatalf("mismatched key exists = %d, error = %v", exists, err)
	}
}

func integrationClient(t *testing.T, address, password string) *Client {
	t.Helper()
	client, err := New(context.Background(), integrationConfig(address, password))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition did not become true before timeout")
}
