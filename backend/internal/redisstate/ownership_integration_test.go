//go:build integration

package redisstate

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestOwnershipStoreFencesStaleOwners(t *testing.T) {
	address, password := integrationRedis(t)
	firstClient := integrationClient(t, address, password)
	secondClient := integrationClient(t, address, password)
	first := NewOwnershipStore(firstClient, Keyspace{Prefix: "rh"})
	second := NewOwnershipStore(secondClient, Keyspace{Prefix: "rh"})
	ctx := context.Background()
	const oldGeneration = uint64(1<<63) + 101
	const newGeneration = uint64(1<<63) + 202

	if err := first.Claim(ctx, "app_1", "conn_1", "api_old", oldGeneration, time.Minute); err != nil {
		t.Fatal(err)
	}
	owner, err := second.Owner(ctx, "app_1", "conn_1")
	if err != nil || owner != (ConnectionOwner{InstanceID: "api_old", Generation: oldGeneration}) {
		t.Fatalf("Owner() = %+v, %v", owner, err)
	}
	if err := first.Refresh(ctx, "app_1", "conn_1", "api_old", oldGeneration-1, time.Minute); !errors.Is(err, ErrOwnershipLost) {
		t.Fatalf("stale generation Refresh() error = %v", err)
	}

	if err := second.Claim(ctx, "app_1", "conn_1", "api_new", newGeneration, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := first.Refresh(ctx, "app_1", "conn_1", "api_old", oldGeneration, time.Minute); !errors.Is(err, ErrOwnershipLost) {
		t.Fatalf("old owner Refresh() error = %v", err)
	}
	if err := first.Release(ctx, "app_1", "conn_1", "api_old", oldGeneration); !errors.Is(err, ErrOwnershipLost) {
		t.Fatalf("old owner Release() error = %v", err)
	}
	owner, err = second.Owner(ctx, "app_1", "conn_1")
	if err != nil || owner != (ConnectionOwner{InstanceID: "api_new", Generation: newGeneration}) {
		t.Fatalf("new Owner() = %+v, %v", owner, err)
	}
	if err := second.Release(ctx, "app_1", "conn_1", "api_new", newGeneration); err != nil {
		t.Fatalf("matching Release() error = %v", err)
	}
	if _, err := second.Owner(ctx, "app_1", "conn_1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Owner() after release error = %v", err)
	}
}

func TestOwnershipStoreRejectsCrossApplicationLookup(t *testing.T) {
	address, password := integrationRedis(t)
	client := integrationClient(t, address, password)
	store := NewOwnershipStore(client, Keyspace{Prefix: "rh"})
	ctx := context.Background()
	if err := store.Claim(ctx, "app_1", "conn_1", "api_1", 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Owner(ctx, "app_2", "conn_1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-application Owner() error = %v, want ErrNotFound", err)
	}
}

func TestHeartbeatExpiryAndBoundedLiveInstances(t *testing.T) {
	address, password := integrationRedis(t)
	firstClient := integrationClient(t, address, password)
	secondClient := integrationClient(t, address, password)
	first := NewOwnershipStore(firstClient, Keyspace{Prefix: "rh"})
	second := NewOwnershipStore(secondClient, Keyspace{Prefix: "rh"})
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	instances := []Instance{
		{ID: "api_1", Role: "api", Generation: 11, StartedAt: now.Add(-time.Minute)},
		{ID: "api_2", Role: "api", Generation: 22, StartedAt: now.Add(-time.Minute)},
		{ID: "worker_1", Role: "worker", Generation: 33, StartedAt: now.Add(-time.Minute)},
	}
	for i, instance := range instances {
		store := first
		if i%2 == 1 {
			store = second
		}
		if err := store.Heartbeat(ctx, instance, 300*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}

	live, err := second.LiveInstances(ctx, time.Now(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 2 {
		t.Fatalf("LiveInstances(limit=2) returned %d", len(live))
	}
	for _, instance := range live {
		if instance.HeartbeatAt.IsZero() {
			t.Fatalf("instance missing heartbeat time: %+v", instance)
		}
	}

	eventually(t, 2*time.Second, func() bool {
		live, err := first.LiveInstances(ctx, time.Now(), 10)
		return err == nil && len(live) == 0
	})
}

func TestHeartbeatFencesAnOlderProcessGeneration(t *testing.T) {
	address, password := integrationRedis(t)
	oldClient := integrationClient(t, address, password)
	newClient := integrationClient(t, address, password)
	oldStore := NewOwnershipStore(oldClient, Keyspace{Prefix: "rh"})
	newStore := NewOwnershipStore(newClient, Keyspace{Prefix: "rh"})
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	oldInstance := Instance{ID: "api_shared", Role: "api", Generation: 10, StartedAt: now}
	newInstance := Instance{ID: "api_shared", Role: "api", Generation: 20, StartedAt: now.Add(time.Microsecond)}

	if err := oldStore.Heartbeat(ctx, oldInstance, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := newStore.Heartbeat(ctx, newInstance, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := oldStore.Heartbeat(ctx, oldInstance, time.Minute); !errors.Is(err, ErrOwnershipLost) {
		t.Fatalf("old Heartbeat() error = %v, want ErrOwnershipLost", err)
	}
	live, err := newStore.LiveInstances(ctx, time.Now(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 1 || live[0].Generation != newInstance.Generation || live[0].StartedAt != newInstance.StartedAt {
		t.Fatalf("LiveInstances() = %+v, want newer generation", live)
	}
}
