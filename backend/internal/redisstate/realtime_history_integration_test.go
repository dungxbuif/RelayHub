//go:build integration

package redisstate

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestRealtimeHistoryIsAppScopedBoundedAndCursorPaged(t *testing.T) {
	address, password := integrationRedis(t)
	store := NewRealtimeHistoryStore(integrationClient(t, address, password), Keyspace{Prefix: "rh"}, 3, time.Minute)
	ctx := context.Background()
	for index := 1; index <= 4; index++ {
		payload, _ := json.Marshal(map[string]int{"n": index})
		if _, err := store.Append(ctx, "app_a", "room", payload); err != nil {
			t.Fatal(err)
		}
	}
	first, cursor, err := store.Read(ctx, "app_a", "room", "", 2)
	if err != nil || len(first) != 2 || string(first[0].Payload) != `{"n":3}` || string(first[1].Payload) != `{"n":4}` || cursor == "" {
		t.Fatalf("first=%#v cursor=%q error=%v", first, cursor, err)
	}
	second, next, err := store.Read(ctx, "app_a", "room", cursor, 2)
	if err != nil || len(second) != 1 || string(second[0].Payload) != `{"n":2}` || next != "" {
		t.Fatalf("second=%#v cursor=%q error=%v", second, next, err)
	}
	isolated, _, err := store.Read(ctx, "app_b", "room", "", 10)
	if err != nil || len(isolated) != 0 {
		t.Fatalf("cross-app history=%#v error=%v", isolated, err)
	}
	if _, _, err := store.Read(ctx, "app_a", "room", "not-a-cursor", 2); err == nil {
		t.Fatal("malformed cursor accepted")
	}
}

func TestRealtimeHistoryExpires(t *testing.T) {
	address, password := integrationRedis(t)
	store := NewRealtimeHistoryStore(integrationClient(t, address, password), Keyspace{Prefix: "rh"}, 10, 100*time.Millisecond)
	if _, err := store.Append(context.Background(), "app_a", "room", json.RawMessage(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	items, _, err := store.Read(context.Background(), "app_a", "room", "", 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("expired items=%#v error=%v", items, err)
	}
}
