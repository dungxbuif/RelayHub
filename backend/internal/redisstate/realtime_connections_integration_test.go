//go:build integration

package redisstate

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRealtimeConnectionRegistryLifecycleAndIsolation(t *testing.T) {
	address, password := integrationRedis(t)
	store := NewRealtimeConnectionStore(integrationClient(t, address, password), Keyspace{Prefix: "rh"})
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	connection := RealtimeConnection{AppID: "app_a", ClientID: "client_1", ConnectionID: "conn_1", InstanceID: "api_1", Generation: 7, Protocol: "relayhub.realtime.v2", Channels: []string{"room"}, ConnectedAt: now}
	if err := store.Put(ctx, connection, time.Minute); err != nil {
		t.Fatal(err)
	}
	listed, err := store.List(ctx, "app_a", time.Now(), 100)
	if err != nil || len(listed) != 1 || listed[0].ClientID != "client_1" || listed[0].Channels[0] != "room" {
		t.Fatalf("List()=%#v, %v", listed, err)
	}
	if other, err := store.List(ctx, "app_b", time.Now(), 100); err != nil || len(other) != 0 {
		t.Fatalf("cross-app List()=%#v, %v", other, err)
	}
	if got, err := store.Get(ctx, "app_a", "conn_1"); err != nil || got.InstanceID != "api_1" {
		t.Fatalf("Get()=%#v, %v", got, err)
	}
	if _, err := store.Get(ctx, "app_b", "conn_1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-app Get()=%v", err)
	}
	if err := store.UpdateChannels(ctx, connection, []string{"room", "alerts"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := store.Refresh(ctx, connection, time.Minute); err != nil {
		t.Fatal(err)
	}
	stale := connection
	stale.Generation = 6
	if err := store.Delete(ctx, stale); !errors.Is(err, ErrOwnershipLost) {
		t.Fatalf("stale Delete()=%v", err)
	}
	if err := store.Delete(ctx, connection); err != nil {
		t.Fatal(err)
	}
	if listed, err := store.List(ctx, "app_a", time.Now(), 100); err != nil || len(listed) != 0 {
		t.Fatalf("List() after delete=%#v, %v", listed, err)
	}
}
