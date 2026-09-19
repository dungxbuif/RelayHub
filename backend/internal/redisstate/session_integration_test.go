//go:build integration

package redisstate

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSessionStoreRoundTripExpiryAndDelete(t *testing.T) {
	address, password := integrationRedis(t)
	client := integrationClient(t, address, password)
	store := NewRedisSessionStore(client, Keyspace{Prefix: "rh"})
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	session := AdminSession{ID: "sess_1", CSRFHash: "csrf-hash", IssuedAt: now, ExpiresAt: now.Add(250 * time.Millisecond)}

	if err := store.Put(ctx, session); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	got, err := store.Get(ctx, session.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got != session {
		t.Fatalf("Get() = %+v, want %+v", got, session)
	}

	eventually(t, time.Second, func() bool {
		_, err := store.Get(ctx, session.ID)
		return errors.Is(err, ErrNotFound)
	})

	session.ID = "sess_delete"
	session.ExpiresAt = time.Now().Add(time.Minute)
	if err := store.Put(ctx, session); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, session.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, session.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get() after Delete error = %v, want ErrNotFound", err)
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

	if _, err := store.Get(ctx, "sess_corrupt"); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("Get() error = %v, want ErrCorruptRecord", err)
	}
	if exists, err := client.Universal().Exists(ctx, key).Result(); err != nil || exists != 0 {
		t.Fatalf("corrupt key exists = %d, error = %v", exists, err)
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
