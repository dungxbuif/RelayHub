//go:build integration

package redisstate

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRateLimiterConcurrentClientsNeverExceedBurst(t *testing.T) {
	address, password := integrationRedis(t)
	firstClient := integrationClient(t, address, password)
	secondClient := integrationClient(t, address, password)
	limiters := []*RateLimiter{NewRateLimiter(firstClient), NewRateLimiter(secondClient)}
	key, err := (Keyspace{Prefix: "rh"}).RateLimit("app_1", "publish", "minute")
	if err != nil {
		t.Fatal(err)
	}

	var allowed atomic.Int64
	var wg sync.WaitGroup
	errorsSeen := make(chan error, 64)
	resetTimes := make(chan time.Time, 64)
	startedAt := time.Now()
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			decision, err := limiters[i%len(limiters)].Allow(context.Background(), key, 17, time.Minute, 1)
			if err != nil {
				errorsSeen <- err
				return
			}
			if decision.Remaining < 0 {
				errorsSeen <- errors.New("negative remaining tokens")
				return
			}
			resetTimes <- decision.ResetAt
			if decision.Allowed {
				allowed.Add(1)
			}
		}(i)
	}
	wg.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Fatal(err)
	}
	if got := allowed.Load(); got != 17 {
		t.Fatalf("allowed = %d, want 17", got)
	}
	close(resetTimes)
	latestAllowedReset := startedAt.Add(time.Minute + 2*time.Second)
	for reset := range resetTimes {
		if reset.Before(startedAt.Add(-time.Second)) || reset.After(latestAllowedReset) {
			t.Fatalf("reset timestamp %v outside token-bucket window", reset)
		}
	}
}

func TestRateLimiterRefillsToBurstWithoutExceedingIt(t *testing.T) {
	address, password := integrationRedis(t)
	client := integrationClient(t, address, password)
	limiter := NewRateLimiter(client)
	key, err := (Keyspace{Prefix: "rh"}).RateLimit("app_refill", "publish", "second")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 17; i++ {
		decision, err := limiter.Allow(ctx, key, 17, time.Second, 1)
		if err != nil || !decision.Allowed {
			t.Fatalf("initial decision %d = %+v, %v", i, decision, err)
		}
	}
	if decision, err := limiter.Allow(ctx, key, 17, time.Second, 1); err != nil || decision.Allowed {
		t.Fatalf("burst overflow decision = %+v, %v", decision, err)
	}

	time.Sleep(1100 * time.Millisecond)
	allowed := 0
	for i := 0; i < 18; i++ {
		decision, err := limiter.Allow(ctx, key, 17, time.Second, 1)
		if err != nil {
			t.Fatal(err)
		}
		if decision.Allowed {
			allowed++
		}
	}
	if allowed != 17 {
		t.Fatalf("allowed after refill = %d, want 17", allowed)
	}
}

func TestRateLimiterRejectsInvalidRequestsCancellationAndOutage(t *testing.T) {
	address, password := integrationRedis(t)
	client := integrationClient(t, address, password)
	limiter := NewRateLimiter(client)
	key, err := (Keyspace{Prefix: "rh"}).RateLimit("app_invalid", "publish", "minute")
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name   string
		key    string
		limit  int64
		window time.Duration
		cost   int64
	}{
		{name: "empty key", limit: 1, window: time.Minute, cost: 1},
		{name: "zero limit", key: key, window: time.Minute, cost: 1},
		{name: "negative limit", key: key, limit: -1, window: time.Minute, cost: 1},
		{name: "zero cost", key: key, limit: 1, window: time.Minute},
		{name: "negative cost", key: key, limit: 1, window: time.Minute, cost: -1},
		{name: "cost above burst", key: key, limit: 1, window: time.Minute, cost: 2},
		{name: "window too short", key: key, limit: 1, window: time.Second - time.Millisecond, cost: 1},
		{name: "window too long", key: key, limit: 1, window: 24*time.Hour + time.Millisecond, cost: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decision, err := limiter.Allow(context.Background(), tc.key, tc.limit, tc.window, tc.cost)
			if !errors.Is(err, ErrInvalidRecord) || decision.Allowed {
				t.Fatalf("Allow() = %+v, %v; want fail closed invalid record", decision, err)
			}
		})
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if decision, err := limiter.Allow(cancelled, key, 1, time.Minute, 1); !errors.Is(err, context.Canceled) || decision.Allowed {
		t.Fatalf("cancelled Allow() = %+v, %v", decision, err)
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if decision, err := limiter.Allow(context.Background(), key, 1, time.Minute, 1); !errors.Is(err, ErrUnavailable) || decision.Allowed {
		t.Fatalf("outage Allow() = %+v, %v", decision, err)
	}
}
