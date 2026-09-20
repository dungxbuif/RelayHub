//go:build integration

package redisstate

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestRealtimeActionsAreIdempotentAuthorFencedAndTombstoned(t *testing.T) {
	address, password := integrationRedis(t)
	store := NewRealtimeActionStore(integrationClient(t, address, password), Keyspace{Prefix: "rh"}, 2, time.Minute)
	ctx := context.Background()
	if err := store.RecordMessage(ctx, "app_a", "room", "msg_1", false); err != nil {
		t.Fatal(err)
	}
	action := RealtimeMessageAction{ID: "action_1", AppID: "app_a", Channel: "room", MessageID: "msg_1", ClientID: "client_a", Type: "reaction", IdempotencyKey: "idem_1", Data: json.RawMessage(`{"emoji":"ok"}`), CreatedAt: time.Now().UTC()}
	first, err := store.Put(ctx, action)
	if err != nil {
		t.Fatal(err)
	}
	action.ID = "action_other"
	replayed, err := store.Put(ctx, action)
	if err != nil || replayed.ID != first.ID {
		t.Fatalf("replayed=%#v error=%v", replayed, err)
	}
	if _, err := store.Remove(ctx, "app_a", "room", "msg_1", first.ID, "client_b"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("foreign remove=%v", err)
	}
	removed, err := store.Remove(ctx, "app_a", "room", "msg_1", first.ID, "client_a")
	if err != nil || removed.RemovedAt == nil {
		t.Fatalf("removed=%#v error=%v", removed, err)
	}
	actions, err := store.List(ctx, "app_a", "room", "msg_1")
	if err != nil || len(actions) != 1 || actions[0].RemovedAt == nil {
		t.Fatalf("actions=%#v error=%v", actions, err)
	}
	if _, err := store.List(ctx, "app_b", "room", "msg_1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-app list=%v", err)
	}
}
