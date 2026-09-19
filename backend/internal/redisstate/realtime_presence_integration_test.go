//go:build integration

package redisstate

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestRealtimePresenceJoinUpdateLeaveAndExpiry(t *testing.T) {
	address, password := integrationRedis(t)
	store := NewRealtimePresenceStore(integrationClient(t, address, password), Keyspace{Prefix: "rh"})
	presence := RealtimePresence{AppID: "app_a", Channel: "room", ClientID: "client_1", ConnectionID: "conn_1", Data: json.RawMessage(`{"status":"online"}`)}
	joined, occupancy, err := store.Upsert(context.Background(), presence, 250*time.Millisecond)
	if err != nil || !joined || occupancy != 1 {
		t.Fatalf("join=%v occupancy=%d err=%v", joined, occupancy, err)
	}
	joined, occupancy, err = store.Upsert(context.Background(), presence, 250*time.Millisecond)
	if err != nil || joined || occupancy != 1 {
		t.Fatalf("update join=%v occupancy=%d err=%v", joined, occupancy, err)
	}
	time.Sleep(300 * time.Millisecond)
	expired, occupancy, err := store.Expire(context.Background(), "app_a", "room", time.Now(), 100)
	if err != nil || len(expired) != 1 || expired[0] != "conn_1" || occupancy != 0 {
		t.Fatalf("Expire()=%v occupancy=%d err=%v", expired, occupancy, err)
	}
	second := presence
	second.ConnectionID = "conn_2"
	joined, occupancy, err = store.Upsert(context.Background(), second, time.Minute)
	if err != nil || !joined || occupancy != 1 {
		t.Fatalf("expiry reconciliation join=%v occupancy=%d err=%v", joined, occupancy, err)
	}
	if occupancy, err = store.Delete(context.Background(), second); err != nil || occupancy != 0 {
		t.Fatalf("leave occupancy=%d err=%v", occupancy, err)
	}
}
