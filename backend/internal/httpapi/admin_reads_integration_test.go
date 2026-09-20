//go:build integration

package httpapi_test

import (
	"context"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/adminread"
	"github.com/dungxbuif/RelayHub/internal/redisstate"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/dungxbuif/RelayHub/internal/teststack"
)

func TestAdminReadsAggregateReplicasAndDegradeWithoutRedis(t *testing.T) {
	stack := teststack.Start(t)
	cfg := scaleConfig(stack)
	ctx := context.Background()
	first := scaleDependencies(t, ctx, stack, cfg, "api_a")
	second := scaleDependencies(t, ctx, stack, cfg, "api_b")
	keys := redisstate.Keyspace{Prefix: cfg.Redis.KeyPrefix}
	firstMetrics := redisstate.NewDashboardMetricsStore(first.Redis, keys)
	secondMetrics := redisstate.NewDashboardMetricsStore(second.Redis, keys)

	for _, sample := range []struct {
		store *redisstate.DashboardMetricsStore
		field redisstate.MetricField
		delta int64
	}{
		{firstMetrics, redisstate.MetricRequestTotal, 2},
		{secondMetrics, redisstate.MetricRequestTotal, 3},
		{firstMetrics, redisstate.MetricStatus2xx, 1},
		{secondMetrics, redisstate.MetricStatus2xx, 2},
	} {
		if err := sample.store.Record(ctx, sample.field, sample.delta); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC()
	for _, heartbeat := range []struct {
		store       *redisstate.DashboardMetricsStore
		instanceID  string
		connections int64
	}{
		{firstMetrics, "api_a", 2},
		{secondMetrics, "api_b", 3},
	} {
		if err := heartbeat.store.HeartbeatInstance(ctx, redisstate.DashboardInstanceState{InstanceID: heartbeat.instanceID, Connections: heartbeat.connections, NATSConnected: true, NATSChangedAt: now}, 30*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	publishDurableEvent(t, first.Postgres, now)

	reader, err := service.NewAdminReadService(second.Postgres, redisstate.NewDashboardMetricsStore(second.Redis, keys), service.AdminReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dashboard, err := reader.Dashboard(ctx, 5*time.Minute, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var requests, successes int64
	for _, point := range dashboard.Series {
		requests += point.Values[string(redisstate.MetricRequestTotal)]
		successes += point.Values[string(redisstate.MetricStatus2xx)]
	}
	if requests != 5 || successes != 3 || dashboard.ActiveConnections != 5 || len(dashboard.Instances) != 2 || dashboard.Durable.Pending != 1 {
		t.Fatalf("combined dashboard = %#v, requests=%d successes=%d", dashboard, requests, successes)
	}

	stack.StopRedis(t)
	degraded, err := reader.Dashboard(ctx, 5*time.Minute, time.Minute)
	if err != nil {
		t.Fatalf("Redis outage blocked PostgreSQL truth: %v", err)
	}
	if degraded.Durable.Pending != 1 || len(degraded.DegradedComponents) != 2 || degraded.DegradedComponents[0] != "instances" || degraded.DegradedComponents[1] != "rolling_metrics" {
		t.Fatalf("degraded dashboard = %#v", degraded)
	}
	events, err := reader.ListEvents(ctx, adminread.EventListQuery{Options: adminread.ListOptions{Limit: 10}})
	if err != nil || len(events.Items) != 1 || events.Items[0].ID != "evt_scale" {
		t.Fatalf("PostgreSQL list during Redis outage = %#v, %v", events, err)
	}
}
