//go:build integration

package redisstate

import (
	"context"
	"testing"
	"time"
)

func TestRealtimePublishLimiterScopesAppConnectionAndChannel(t *testing.T) {
	address, password := integrationRedis(t)
	limiter := NewRealtimePublishLimiter(integrationClient(t, address, password), Keyspace{Prefix: "rh"}, RealtimePublishLimits{App: 10, Connection: 10, Channel: 1, Window: time.Minute})
	allowed, err := limiter.Allow(context.Background(), "app_a", "conn_1", "room")
	if err != nil || !allowed {
		t.Fatalf("first allow=%v error=%v", allowed, err)
	}
	allowed, err = limiter.Allow(context.Background(), "app_a", "conn_1", "room")
	if err != nil || allowed {
		t.Fatalf("channel limit allow=%v error=%v", allowed, err)
	}
	allowed, err = limiter.Allow(context.Background(), "app_b", "conn_1", "room")
	if err != nil || !allowed {
		t.Fatalf("cross-app isolation allow=%v error=%v", allowed, err)
	}
}

func TestRealtimePublishLimiterFailsClosedForInvalidConfiguration(t *testing.T) {
	address, password := integrationRedis(t)
	limiter := NewRealtimePublishLimiter(integrationClient(t, address, password), Keyspace{Prefix: "rh"}, RealtimePublishLimits{})
	if allowed, err := limiter.Allow(context.Background(), "app_a", "conn_1", "room"); err == nil || allowed {
		t.Fatalf("allow=%v error=%v", allowed, err)
	}
}
