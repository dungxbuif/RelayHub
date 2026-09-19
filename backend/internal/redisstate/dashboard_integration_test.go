//go:build integration

package redisstate

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestDashboardMetricsAggregateAcrossClientsAndBoundFields(t *testing.T) {
	address, password := integrationRedis(t)
	first := integrationClient(t, address, password)
	second := integrationClient(t, address, password)
	keys := Keyspace{Prefix: "rh"}
	stores := []*DashboardMetricsStore{NewDashboardMetricsStore(first, keys), NewDashboardMetricsStore(second, keys)}

	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := stores[i%2].Record(context.Background(), MetricRequestTotal, 1); err != nil {
				t.Errorf("Record request: %v", err)
			}
			if err := stores[i%2].Record(context.Background(), MetricStatus2xx, 1); err != nil {
				t.Errorf("Record status: %v", err)
			}
		}(i)
	}
	wg.Wait()
	series, err := stores[0].Read(context.Background(), time.Now().UTC(), 5*time.Minute, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var requests, successes int64
	for _, bucket := range series {
		requests += bucket.Values[MetricRequestTotal]
		successes += bucket.Values[MetricStatus2xx]
	}
	if len(series) != 5 || requests != 40 || successes != 40 {
		t.Fatalf("series len=%d requests=%d successes=%d: %#v", len(series), requests, successes, series)
	}
	if err := stores[0].Record(context.Background(), MetricField("unbounded_app_id"), 1); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("unbounded field error = %v", err)
	}
}

func TestDashboardInstanceHeartbeatAggregatesAndCleansStaleMembers(t *testing.T) {
	address, password := integrationRedis(t)
	client := integrationClient(t, address, password)
	keys := Keyspace{Prefix: "rh"}
	store := NewDashboardMetricsStore(client, keys)
	now := time.Now().UTC()
	for _, state := range []DashboardInstanceState{
		{InstanceID: "api_1", Connections: 3, NATSConnected: true, NATSChangedAt: now.Add(-time.Minute)},
		{InstanceID: "api_2", Connections: 5, NATSConnected: false, NATSChangedAt: now},
	} {
		if err := store.HeartbeatInstance(context.Background(), state, 30*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	instances, err := store.LiveInstances(context.Background(), 10)
	if err != nil || len(instances) != 2 {
		t.Fatalf("LiveInstances = %#v, %v", instances, err)
	}
	var connections int64
	for _, instance := range instances {
		connections += instance.Connections
		if instance.HeartbeatAt.IsZero() {
			t.Fatalf("missing heartbeat: %#v", instance)
		}
	}
	if connections != 8 {
		t.Fatalf("connections = %d", connections)
	}
	staleKey, _ := keys.DashboardInstance("api_2")
	if err := client.Universal().Del(context.Background(), staleKey).Err(); err != nil {
		t.Fatal(err)
	}
	instances, err = store.LiveInstances(context.Background(), 10)
	if err != nil || len(instances) != 1 || instances[0].InstanceID != "api_1" {
		t.Fatalf("after stale cleanup = %#v, %v", instances, err)
	}
}

func TestDashboardMetricsRejectInvalidWindowsCancellationAndOutage(t *testing.T) {
	address, password := integrationRedis(t)
	client := integrationClient(t, address, password)
	store := NewDashboardMetricsStore(client, Keyspace{Prefix: "rh"})
	if _, err := store.Read(context.Background(), time.Now(), 7*time.Minute, time.Minute); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("invalid window error = %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.Record(cancelled, MetricRequestTotal, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled record error = %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Read(context.Background(), time.Now(), 5*time.Minute, time.Minute); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("outage read error = %v", err)
	}
}
