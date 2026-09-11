package config

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

var configEnvironment = []string{
	"RELAYHUB_WORKER_HTTP_ADDR", "RELAYHUB_WORKER_CONCURRENCY", "RELAYHUB_CALLBACK_TIMEOUT", "RELAYHUB_WORKER_RECLAIM_IDLE",
	"RELAYHUB_REDIS_KEY_PREFIX",
	"RELAYHUB_HTTP_ADDR",
	"RELAYHUB_REDIS_URL",
	"RELAYHUB_ADMIN_TOKEN",
	"RELAYHUB_SIGNING_SECRET",
	"RELAYHUB_ALLOWED_ORIGINS",
	"RELAYHUB_ALLOW_INSECURE_CALLBACKS",
	"RELAYHUB_EVENT_RETENTION",
	"RELAYHUB_JOB_RETENTION",
	"RELAYHUB_IDEMPOTENCY_RETENTION",
	"RELAYHUB_SIGNING_SKEW",
	"RELAYHUB_SHUTDOWN_TIMEOUT",
}

func TestLoadParsesInsecureCallbackPolicy(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		want      bool
		wantError bool
	}{
		{name: "disabled by default", want: false},
		{name: "enabled", value: "true", want: true},
		{name: "explicitly disabled", value: "false", want: false},
		{name: "malformed", value: "sometimes", wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequiredEnvironment(t)
			t.Setenv("RELAYHUB_ALLOW_INSECURE_CALLBACKS", tt.value)

			got, err := Load()
			if tt.wantError {
				if err == nil || !strings.Contains(err.Error(), "RELAYHUB_ALLOW_INSECURE_CALLBACKS") {
					t.Fatalf("Load() error = %v, want named invalid variable error", err)
				}
				if strings.Contains(err.Error(), tt.value) {
					t.Fatalf("Load() error exposed invalid value: %q", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if got.AllowInsecureCallbacks != tt.want {
				t.Fatalf("AllowInsecureCallbacks = %t, want %t", got.AllowInsecureCallbacks, tt.want)
			}
		})
	}
}

func TestLoadRequiresSecretsWithoutExposingValues(t *testing.T) {
	tests := []struct {
		name       string
		missing    string
		secretName string
		secret     string
	}{
		{
			name:       "admin token",
			missing:    "RELAYHUB_ADMIN_TOKEN",
			secretName: "RELAYHUB_SIGNING_SECRET",
			secret:     "signing-secret-value",
		},
		{
			name:       "signing secret",
			missing:    "RELAYHUB_SIGNING_SECRET",
			secretName: "RELAYHUB_ADMIN_TOKEN",
			secret:     "admin-token-value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearConfigEnvironment(t)
			t.Setenv(tt.secretName, tt.secret)

			_, err := Load()
			if err == nil {
				t.Fatalf("Load() error = nil, want missing %s error", tt.missing)
			}
			if !strings.Contains(err.Error(), tt.missing) {
				t.Fatalf("Load() error = %q, want variable name %q", err, tt.missing)
			}
			if strings.Contains(err.Error(), tt.secret) {
				t.Fatalf("Load() error exposed secret value: %q", err)
			}
		})
	}
}

func TestLoadUsesDocumentedDefaults(t *testing.T) {
	setRequiredEnvironment(t)

	got, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if got.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want %q", got.HTTPAddr, ":8080")
	}
	if got.RedisURL != "redis://localhost:6379/0" {
		t.Errorf("RedisURL = %q, want %q", got.RedisURL, "redis://localhost:6379/0")
	}
	if got.AdminToken != "admin-token" {
		t.Errorf("AdminToken = %q, want configured value", got.AdminToken)
	}
	if got.SigningSecret != "signing-secret" {
		t.Errorf("SigningSecret = %q, want configured value", got.SigningSecret)
	}
	if got.AllowedOrigins != nil {
		t.Errorf("AllowedOrigins = %#v, want nil", got.AllowedOrigins)
	}
	if got.EventRetention != 7*24*time.Hour {
		t.Errorf("EventRetention = %v, want %v", got.EventRetention, 7*24*time.Hour)
	}
	if got.JobRetention != 7*24*time.Hour {
		t.Errorf("JobRetention = %v, want %v", got.JobRetention, 7*24*time.Hour)
	}
	if got.IdempotencyRetention != 24*time.Hour {
		t.Errorf("IdempotencyRetention = %v, want %v", got.IdempotencyRetention, 24*time.Hour)
	}
	if got.SigningSkew != 5*time.Minute {
		t.Errorf("SigningSkew = %v, want %v", got.SigningSkew, 5*time.Minute)
	}
	if got.ShutdownTimeout != 10*time.Second {
		t.Errorf("ShutdownTimeout = %v, want %v", got.ShutdownTimeout, 10*time.Second)
	}
}

func TestLoadRejectsInvalidDurations(t *testing.T) {
	tests := []struct {
		name     string
		variable string
		value    string
	}{
		{name: "malformed event retention", variable: "RELAYHUB_EVENT_RETENTION", value: "tomorrow"},
		{name: "zero job retention", variable: "RELAYHUB_JOB_RETENTION", value: "0s"},
		{name: "negative idempotency retention", variable: "RELAYHUB_IDEMPOTENCY_RETENTION", value: "-1h"},
		{name: "malformed signing skew", variable: "RELAYHUB_SIGNING_SKEW", value: "300"},
		{name: "zero shutdown timeout", variable: "RELAYHUB_SHUTDOWN_TIMEOUT", value: "0s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequiredEnvironment(t)
			t.Setenv(tt.variable, tt.value)

			_, err := Load()
			if err == nil {
				t.Fatalf("Load() error = nil, want invalid %s error", tt.variable)
			}
			if !strings.Contains(err.Error(), tt.variable) {
				t.Fatalf("Load() error = %q, want variable name %q", err, tt.variable)
			}
			if strings.Contains(err.Error(), tt.value) {
				t.Fatalf("Load() error exposed invalid value: %q", err)
			}
		})
	}
}

func TestLoadRejectsInvalidURLs(t *testing.T) {
	tests := []struct {
		name     string
		variable string
		value    string
	}{
		{name: "Redis URL scheme", variable: "RELAYHUB_REDIS_URL", value: "https://localhost:6379/0"},
		{name: "Redis URL without host", variable: "RELAYHUB_REDIS_URL", value: "redis:///0"},
		{name: "origin with path", variable: "RELAYHUB_ALLOWED_ORIGINS", value: "https://app.example.test/path"},
		{name: "origin without scheme", variable: "RELAYHUB_ALLOWED_ORIGINS", value: "app.example.test"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequiredEnvironment(t)
			t.Setenv(tt.variable, tt.value)

			_, err := Load()
			if err == nil {
				t.Fatalf("Load() error = nil, want invalid %s error", tt.variable)
			}
			if !strings.Contains(err.Error(), tt.variable) {
				t.Fatalf("Load() error = %q, want variable name %q", err, tt.variable)
			}
			if strings.Contains(err.Error(), tt.value) {
				t.Fatalf("Load() error exposed invalid value: %q", err)
			}
		})
	}
}

func TestLoadRejectsInvalidHTTPAddress(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv("RELAYHUB_HTTP_ADDR", "localhost")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want invalid RELAYHUB_HTTP_ADDR error")
	}
	if !strings.Contains(err.Error(), "RELAYHUB_HTTP_ADDR") {
		t.Fatalf("Load() error = %q, want RELAYHUB_HTTP_ADDR", err)
	}
}

func TestLoadNormalizesAllowedOrigins(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv("RELAYHUB_ALLOWED_ORIGINS", " https://one.example.test,https://two.example.test,,https://one.example.test ")

	got, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	want := []string{"https://one.example.test", "https://two.example.test"}
	if !reflect.DeepEqual(got.AllowedOrigins, want) {
		t.Fatalf("AllowedOrigins = %#v, want %#v", got.AllowedOrigins, want)
	}
}

func TestLoadRejectsWildcardOrigin(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv("RELAYHUB_ALLOWED_ORIGINS", "https://one.example.test, *")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want wildcard rejection")
	}
	if !strings.Contains(err.Error(), "RELAYHUB_ALLOWED_ORIGINS") {
		t.Fatalf("Load() error = %q, want RELAYHUB_ALLOWED_ORIGINS", err)
	}
}

func setRequiredEnvironment(t *testing.T) {
	t.Helper()
	clearConfigEnvironment(t)
	t.Setenv("RELAYHUB_ADMIN_TOKEN", "admin-token")
	t.Setenv("RELAYHUB_SIGNING_SECRET", "signing-secret")
}

func clearConfigEnvironment(t *testing.T) {
	t.Helper()
	for _, variable := range configEnvironment {
		t.Setenv(variable, "")
		if variable == "RELAYHUB_REDIS_KEY_PREFIX" {
			_ = os.Unsetenv(variable)
		}
	}
}

func TestRedisKeyPrefix(t *testing.T) {
	setRequiredEnvironment(t)
	got, err := Load()
	if err != nil || got.RedisKeyPrefix != "relayhub" {
		t.Fatalf("default prefix %#v %v", got, err)
	}
	for _, prefix := range []string{"tenant-a", "relayhub_2", "ABC123"} {
		t.Setenv("RELAYHUB_REDIS_KEY_PREFIX", prefix)
		got, err = Load()
		if err != nil || got.RedisKeyPrefix != prefix {
			t.Fatalf("valid prefix %v", err)
		}
	}
	for _, prefix := range []string{"*", "x:y", "a?b", "x[1]", "a b", "", strings.Repeat("x", 65)} {
		t.Setenv("RELAYHUB_REDIS_KEY_PREFIX", prefix)
		if _, err := Load(); err == nil {
			t.Fatalf("accepted unsafe prefix %q", prefix)
		}
	}
}

func TestWorkerConfiguration(t *testing.T) {
	setRequiredEnvironment(t)
	cfg, err := Load()
	if err != nil || cfg.WorkerHTTPAddr != ":9090" || cfg.WorkerConcurrency != 8 || cfg.CallbackTimeout != 10*time.Second || cfg.WorkerReclaimIdle != 30*time.Second {
		t.Fatalf("defaults %+v %v", cfg, err)
	}
	t.Setenv("RELAYHUB_WORKER_CONCURRENCY", "3")
	t.Setenv("RELAYHUB_CALLBACK_TIMEOUT", "12s")
	t.Setenv("RELAYHUB_WORKER_RECLAIM_IDLE", "40s")
	cfg, err = Load()
	if err != nil || cfg.WorkerConcurrency != 3 || cfg.CallbackTimeout != 12*time.Second || cfg.WorkerReclaimIdle != 40*time.Second {
		t.Fatal("config not parsed")
	}
	for _, v := range []string{"0", "-1", "abc", "1025"} {
		t.Setenv("RELAYHUB_WORKER_CONCURRENCY", v)
		if _, err := Load(); err == nil {
			t.Fatalf("accepted %s", v)
		}
	}
	t.Setenv("RELAYHUB_WORKER_CONCURRENCY", "3")
	t.Setenv("RELAYHUB_WORKER_RECLAIM_IDLE", "1s")
	if _, err := Load(); err == nil {
		t.Fatal("lease shorter than attempt accepted")
	}
}

func TestWorkerHTTPAddress(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv("RELAYHUB_WORKER_HTTP_ADDR", "127.0.0.1:9091")
	cfg, err := Load()
	if err != nil || cfg.WorkerHTTPAddr != "127.0.0.1:9091" {
		t.Fatal("worker address not parsed")
	}
	t.Setenv("RELAYHUB_WORKER_HTTP_ADDR", "bad-address")
	if _, err := Load(); err == nil {
		t.Fatal("invalid worker address accepted")
	}
}
