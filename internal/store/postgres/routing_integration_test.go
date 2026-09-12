//go:build integration

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
)

func TestPostgresRoutingRules(t *testing.T) {
	client := integrationPostgresClient(t)
	resetControlTables(t, client)
	ctx := context.Background()
	now := time.Date(2026, 9, 12, 11, 0, 0, 0, time.UTC)
	for _, id := range []string{"app_source", "app_target", "app_other"} {
		app := domain.App{ID: id, Name: id, DeliveryMode: domain.DeliveryQueue, Enabled: true, CreatedAt: now, UpdatedAt: now}
		if err := client.CreateApplication(ctx, app, store.AppCredential{AppID: id, APIKeyHash: "hash-" + id, HMACSecret: []byte("secret")}); err != nil {
			t.Fatal(err)
		}
	}
	source := "app_source"
	channel := "orders.live"
	rule := domain.RoutingRule{ID: "route_1", SourceAppID: &source, EventType: "order.created", TargetAppID: "app_target", RealtimeChannel: &channel, Enabled: true, CreatedAt: now, UpdatedAt: now}
	if err := client.CreateRoutingRule(ctx, rule); err != nil {
		t.Fatal(err)
	}
	if err := client.CreateRoutingRule(ctx, rule); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("duplicate routing rule error=%v", err)
	}
	matched, err := client.ResolveRoutingRules(ctx, "app_source", "order.created")
	if err != nil || len(matched) != 1 || matched[0].ID != rule.ID || matched[0].RealtimeChannel == nil || *matched[0].RealtimeChannel != channel {
		t.Fatalf("ResolveRoutingRules()=%#v error=%v", matched, err)
	}
	if unmatched, err := client.ResolveRoutingRules(ctx, "app_other", "order.created"); err != nil || len(unmatched) != 0 {
		t.Fatalf("source-specific rule leaked: %#v error=%v", unmatched, err)
	}
	rule.SourceAppID = nil
	rule.Enabled = false
	rule.UpdatedAt = now.Add(time.Minute)
	updated, err := client.UpdateRoutingRule(ctx, rule)
	if err != nil || updated.SourceAppID != nil || updated.Enabled {
		t.Fatalf("UpdateRoutingRule()=%#v error=%v", updated, err)
	}
	if inactive, err := client.ResolveRoutingRules(ctx, "app_other", "order.created"); err != nil || len(inactive) != 0 {
		t.Fatalf("disabled rule matched: %#v error=%v", inactive, err)
	}
	rule.Enabled = true
	if _, err := client.UpdateRoutingRule(ctx, rule); err != nil {
		t.Fatal(err)
	}
	if err := client.DeleteRoutingRule(ctx, rule.ID, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetRoutingRule(ctx, rule.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("deleted rule error=%v", err)
	}
}
