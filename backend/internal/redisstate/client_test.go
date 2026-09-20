package redisstate

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestBuildOptionsSelectsStandaloneSentinelAndCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  Config
		kind clientKind
	}{
		{name: "standalone", cfg: testConfig(ModeStandalone), kind: clientStandalone},
		{name: "sentinel", cfg: func() Config {
			cfg := testConfig(ModeSentinel)
			cfg.Addrs = []string{"redis-a:26379", "redis-b:26379"}
			cfg.SentinelMaster = "relayhub-primary"
			return cfg
		}(), kind: clientSentinel},
		{name: "cluster", cfg: func() Config {
			cfg := testConfig(ModeCluster)
			cfg.Addrs = []string{"redis-a:6379", "redis-b:6379"}
			return cfg
		}(), kind: clientCluster},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := buildOptions(tt.cfg)
			if err != nil {
				t.Fatalf("buildOptions() error = %v", err)
			}
			if got.kind != tt.kind {
				t.Fatalf("kind = %q, want %q", got.kind, tt.kind)
			}
			if !reflect.DeepEqual(got.addresses(), tt.cfg.Addrs) {
				t.Fatalf("addresses = %#v, want %#v", got.addresses(), tt.cfg.Addrs)
			}
			if got.maxRetries() != -1 {
				t.Fatalf("max retries = %d, want -1", got.maxRetries())
			}
		})
	}
}

func TestSafeAddressesRemoveCredentialsAndQueries(t *testing.T) {
	t.Parallel()

	got := safeAddresses([]string{
		"redis.internal:6379",
		"user:never-print@private.example:6380?db=4",
		"redis://user:never-print@private.example:6380/0",
		"not an address",
	})
	want := []string{"redis.internal:6379", "private.example:6380", "private.example:6380", "<invalid>"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("safeAddresses() = %#v, want %#v", got, want)
	}
	joined := strings.Join(got, ",")
	for _, secret := range []string{"never-print", "user", "db=4"} {
		if strings.Contains(joined, secret) {
			t.Fatalf("safe addresses leaked %q: %q", secret, joined)
		}
	}
}

func TestNewFailsWhenInitialPingFails(t *testing.T) {
	t.Parallel()

	cfg := testConfig(ModeStandalone)
	cfg.Addrs = []string{"127.0.0.1:1"}
	cfg.Password = "never-print-this-password"
	cfg.ConnectTimeout = 50 * time.Millisecond
	cfg.ReadTimeout = 50 * time.Millisecond
	cfg.WriteTimeout = 50 * time.Millisecond

	client, err := New(context.Background(), cfg)
	if client != nil {
		_ = client.Close()
		t.Fatal("New() returned a client after a failed initial ping")
	}
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("New() error = %v, want ErrUnavailable", err)
	}
	if got := err.Error(); got != "Redis unavailable" {
		t.Fatalf("New() error = %q, want normalized error", got)
	}
}

func testConfig(mode Mode) Config {
	return Config{
		Mode:           mode,
		Addrs:          []string{"redis.internal:6379"},
		Username:       "relayhub",
		Password:       "secret",
		DB:             0,
		KeyPrefix:      "rh",
		ConnectTimeout: time.Second,
		ReadTimeout:    time.Second,
		WriteTimeout:   time.Second,
		PoolSize:       32,
	}
}
