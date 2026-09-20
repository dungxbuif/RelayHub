package config

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

var configEnvironment = []string{
	"RELAYHUB_WORKER_HTTP_ADDR", "RELAYHUB_WORKER_CONCURRENCY", "RELAYHUB_CALLBACK_TIMEOUT", "RELAYHUB_WORKER_RECLAIM_IDLE",
	"RELAYHUB_HTTP_ADDR",
	"RELAYHUB_ADMIN_TOKEN",
	"RELAYHUB_SIGNING_SECRET",
	"RELAYHUB_ALLOWED_ORIGINS",
	"RELAYHUB_ALLOW_INSECURE_CALLBACKS",
	"RELAYHUB_EVENT_RETENTION",
	"RELAYHUB_JOB_RETENTION",
	"RELAYHUB_IDEMPOTENCY_RETENTION",
	"RELAYHUB_SIGNING_SKEW",
	"RELAYHUB_SHUTDOWN_TIMEOUT",
	"RELAYHUB_NATS_URL",
	"RELAYHUB_NATS_USERNAME",
	"RELAYHUB_NATS_PASSWORD",
	"RELAYHUB_NATS_CONNECT_TIMEOUT",
	"RELAYHUB_NATS_RECONNECT_WAIT",
	"RELAYHUB_NATS_MAX_RECONNECTS",
	"RELAYHUB_NATS_DRAIN_TIMEOUT",
	"RELAYHUB_NATS_STREAM_MAX_AGE",
	"RELAYHUB_NATS_DUPLICATE_WINDOW",
	"RELAYHUB_NATS_REPLICAS",
	"RELAYHUB_REDIS_MODE",
	"RELAYHUB_REDIS_ADDRS",
	"RELAYHUB_REDIS_USERNAME",
	"RELAYHUB_REDIS_PASSWORD",
	"RELAYHUB_REDIS_SENTINEL_MASTER",
	"RELAYHUB_REDIS_DB",
	"RELAYHUB_REDIS_TLS",
	"RELAYHUB_REDIS_KEY_PREFIX",
	"RELAYHUB_REDIS_CONNECT_TIMEOUT",
	"RELAYHUB_REDIS_READ_TIMEOUT",
	"RELAYHUB_REDIS_WRITE_TIMEOUT",
	"RELAYHUB_REDIS_POOL_SIZE",
	"RELAYHUB_INSTANCE_ID",
	"RELAYHUB_POSTGRES_URL",
	"RELAYHUB_SECRET_ENCRYPTION_KEY",
	"RELAYHUB_OBJECT_STORAGE_ENDPOINT", "RELAYHUB_OBJECT_STORAGE_BUCKET", "RELAYHUB_OBJECT_STORAGE_REGION", "RELAYHUB_OBJECT_STORAGE_ACCESS_KEY", "RELAYHUB_OBJECT_STORAGE_SECRET_KEY",
	"RELAYHUB_APNS_ENDPOINT", "RELAYHUB_APNS_AUTHORIZATION", "RELAYHUB_APNS_TOPIC",
	"RELAYHUB_FCM_ENDPOINT", "RELAYHUB_FCM_AUTHORIZATION", "RELAYHUB_FCM_PROJECT",
}

func TestLoadPushProvidersAreOptionalCompleteAndHTTPSOnly(t *testing.T) {
	setRequiredEnvironment(t)
	config, err := Load()
	if err != nil || config.Push.APNS.Endpoint != "" || config.Push.FCM.Endpoint != "" {
		t.Fatalf("config=%#v error=%v", config.Push, err)
	}

	setRequiredEnvironment(t)
	t.Setenv("RELAYHUB_APNS_ENDPOINT", "https://api.push.apple.com")
	t.Setenv("RELAYHUB_APNS_AUTHORIZATION", "bearer operator-managed-token")
	t.Setenv("RELAYHUB_APNS_TOPIC", "com.example.app")
	t.Setenv("RELAYHUB_FCM_ENDPOINT", "https://fcm.googleapis.com")
	t.Setenv("RELAYHUB_FCM_AUTHORIZATION", "Bearer operator-managed-token")
	t.Setenv("RELAYHUB_FCM_PROJECT", "relayhub-demo")
	config, err = Load()
	if err != nil || config.Push.APNS.Topic != "com.example.app" || config.Push.FCM.Project != "relayhub-demo" {
		t.Fatalf("config=%#v error=%v", config.Push, err)
	}

	for _, endpoint := range []string{"http://api.push.apple.com", "https://user:pass@api.push.apple.com"} {
		setRequiredEnvironment(t)
		t.Setenv("RELAYHUB_APNS_ENDPOINT", endpoint)
		t.Setenv("RELAYHUB_APNS_AUTHORIZATION", "secret")
		t.Setenv("RELAYHUB_APNS_TOPIC", "com.example.app")
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), "APNS") {
			t.Fatalf("endpoint=%q error=%v", endpoint, err)
		}
	}
}

func TestLoadObjectStorageIsOptionalButRejectsPartialOrInsecureConfiguration(t *testing.T) {
	setRequiredEnvironment(t)
	if config, err := Load(); err != nil || config.ObjectStorage.Endpoint != "" {
		t.Fatalf("config=%#v error=%v", config.ObjectStorage, err)
	}
	setRequiredEnvironment(t)
	t.Setenv("RELAYHUB_OBJECT_STORAGE_ENDPOINT", "https://objects.example")
	t.Setenv("RELAYHUB_OBJECT_STORAGE_BUCKET", "relayhub-files")
	t.Setenv("RELAYHUB_OBJECT_STORAGE_REGION", "us-east-1")
	t.Setenv("RELAYHUB_OBJECT_STORAGE_ACCESS_KEY", "access")
	t.Setenv("RELAYHUB_OBJECT_STORAGE_SECRET_KEY", "secret")
	if config, err := Load(); err != nil || config.ObjectStorage.Bucket != "relayhub-files" {
		t.Fatalf("config=%#v error=%v", config.ObjectStorage, err)
	}
	for _, endpoint := range []string{"http://objects.example", "https://user:pass@objects.example", "https://objects.example/path"} {
		setRequiredEnvironment(t)
		t.Setenv("RELAYHUB_OBJECT_STORAGE_ENDPOINT", endpoint)
		t.Setenv("RELAYHUB_OBJECT_STORAGE_BUCKET", "relayhub-files")
		t.Setenv("RELAYHUB_OBJECT_STORAGE_ACCESS_KEY", "access")
		t.Setenv("RELAYHUB_OBJECT_STORAGE_SECRET_KEY", "secret")
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), "S3-compatible") {
			t.Fatalf("endpoint=%q error=%v", endpoint, err)
		}
	}
}

func TestLoadParsesOptionalInstanceIDAndRejectsUnsafeValues(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv("RELAYHUB_INSTANCE_ID", "api_primary-1")
	got, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.InstanceID != "api_primary-1" {
		t.Fatalf("InstanceID = %q", got.InstanceID)
	}

	for _, value := range []string{"bad id", "bad{id}", strings.Repeat("a", 129)} {
		setRequiredEnvironment(t)
		t.Setenv("RELAYHUB_INSTANCE_ID", value)
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), "RELAYHUB_INSTANCE_ID") || strings.Contains(err.Error(), value) {
			t.Fatalf("Load(%q) error = %v", value, err)
		}
	}
}

func TestLoadParsesRedisModes(t *testing.T) {
	tests := []struct {
		name, mode, addrs, master string
		db                        int
	}{
		{name: "standalone", mode: "standalone", addrs: "redis.internal:6379", db: 3},
		{name: "sentinel", mode: "sentinel", addrs: "redis-a:26379, redis-b:26379", master: "relayhub-primary", db: 2},
		{name: "cluster", mode: "cluster", addrs: "redis-a:6379,redis-b:6379"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequiredEnvironment(t)
			t.Setenv("RELAYHUB_REDIS_MODE", tt.mode)
			t.Setenv("RELAYHUB_REDIS_ADDRS", tt.addrs)
			t.Setenv("RELAYHUB_REDIS_SENTINEL_MASTER", tt.master)
			t.Setenv("RELAYHUB_REDIS_DB", strconv.Itoa(tt.db))
			t.Setenv("RELAYHUB_REDIS_USERNAME", "relayhub")
			t.Setenv("RELAYHUB_REDIS_PASSWORD", "private-password")
			t.Setenv("RELAYHUB_REDIS_TLS", "true")
			t.Setenv("RELAYHUB_REDIS_KEY_PREFIX", "tenant-a")
			t.Setenv("RELAYHUB_REDIS_CONNECT_TIMEOUT", "3s")
			t.Setenv("RELAYHUB_REDIS_READ_TIMEOUT", "1500ms")
			t.Setenv("RELAYHUB_REDIS_WRITE_TIMEOUT", "1750ms")
			t.Setenv("RELAYHUB_REDIS_POOL_SIZE", "64")

			got, err := Load()
			if err != nil {
				t.Fatal(err)
			}
			wantAddrs := strings.Split(strings.ReplaceAll(tt.addrs, " ", ""), ",")
			if got.Redis.Mode != tt.mode || !reflect.DeepEqual(got.Redis.Addrs, wantAddrs) || got.Redis.SentinelMaster != tt.master || got.Redis.DB != tt.db || got.Redis.Username != "relayhub" || got.Redis.Password != "private-password" || !got.Redis.TLS || got.Redis.KeyPrefix != "tenant-a" || got.Redis.ConnectTimeout != 3*time.Second || got.Redis.ReadTimeout != 1500*time.Millisecond || got.Redis.WriteTimeout != 1750*time.Millisecond || got.Redis.PoolSize != 64 {
				t.Fatalf("Redis config = %+v", got.Redis)
			}
		})
	}
}

func TestLoadRejectsRedisCredentialsInAddresses(t *testing.T) {
	for _, value := range []string{"redis://redis.internal:6379", "user:secret@redis.internal:6379", "redis.internal:6379/path", "redis.internal:6379?db=1", ":6379", "redis.internal", "redis.internal:6379,redis.internal:6379"} {
		t.Run(value, func(t *testing.T) {
			setRequiredEnvironment(t)
			t.Setenv("RELAYHUB_REDIS_ADDRS", value)
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), "RELAYHUB_REDIS_ADDRS") {
				t.Fatalf("Load() error = %v", err)
			}
			if strings.Contains(err.Error(), value) || strings.Contains(err.Error(), "secret") {
				t.Fatalf("Load() leaked address: %q", err)
			}
		})
	}
}

func TestLoadRequiresSentinelMasterOnlyForSentinel(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv("RELAYHUB_REDIS_MODE", "sentinel")
	t.Setenv("RELAYHUB_REDIS_ADDRS", "redis-a:26379,redis-b:26379")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "RELAYHUB_REDIS_SENTINEL_MASTER") {
		t.Fatalf("missing Sentinel master error = %v", err)
	}

	for _, mode := range []string{"standalone", "cluster"} {
		setRequiredEnvironment(t)
		t.Setenv("RELAYHUB_REDIS_MODE", mode)
		t.Setenv("RELAYHUB_REDIS_SENTINEL_MASTER", "unexpected-master")
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), "RELAYHUB_REDIS_SENTINEL_MASTER") {
			t.Fatalf("mode %s accepted Sentinel master: %v", mode, err)
		}
	}
}

func TestLoadRejectsClusterDatabaseSelection(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv("RELAYHUB_REDIS_MODE", "cluster")
	t.Setenv("RELAYHUB_REDIS_ADDRS", "redis-a:6379,redis-b:6379")
	t.Setenv("RELAYHUB_REDIS_DB", "1")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "RELAYHUB_REDIS_DB") {
		t.Fatalf("cluster DB error = %v", err)
	}

	for _, value := range []string{"-1", "16", "one"} {
		setRequiredEnvironment(t)
		t.Setenv("RELAYHUB_REDIS_DB", value)
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), "RELAYHUB_REDIS_DB") {
			t.Fatalf("DB %q error = %v", value, err)
		}
	}
}

func TestLoadDoesNotExposeRedisSecrets(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv("RELAYHUB_REDIS_PASSWORD", "never-print-this-password")
	t.Setenv("RELAYHUB_REDIS_ADDRS", "user:never-print-this-password@redis.internal:6379")
	_, err := Load()
	if err == nil || strings.Contains(err.Error(), "never-print-this-password") {
		t.Fatalf("Load() error = %q", err)
	}
}

func TestLoadRejectsUnboundedRedisPool(t *testing.T) {
	for _, value := range []string{"0", "-1", "4097", "many"} {
		setRequiredEnvironment(t)
		t.Setenv("RELAYHUB_REDIS_POOL_SIZE", value)
		_, err := Load()
		if err == nil || !strings.Contains(err.Error(), "RELAYHUB_REDIS_POOL_SIZE") || strings.Contains(err.Error(), value) {
			t.Fatalf("pool %q error = %v", value, err)
		}
	}
	for _, value := range []string{"Uppercase", "space prefix", "brace{slot}", strings.Repeat("a", 33)} {
		setRequiredEnvironment(t)
		t.Setenv("RELAYHUB_REDIS_KEY_PREFIX", value)
		_, err := Load()
		if err == nil || !strings.Contains(err.Error(), "RELAYHUB_REDIS_KEY_PREFIX") || strings.Contains(err.Error(), value) {
			t.Fatalf("prefix %q error = %v", value, err)
		}
	}
}

func TestLoadRequiresPostgresStreamStorePair(t *testing.T) {
	clearConfigEnvironment(t)
	t.Setenv("RELAYHUB_ADMIN_TOKEN", "admin-token")
	t.Setenv("RELAYHUB_SIGNING_SECRET", "signing-secret")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "RELAYHUB_POSTGRES_URL") {
		t.Fatalf("missing PostgreSQL pair error=%v", err)
	}

	setRequiredEnvironment(t)
	t.Setenv("RELAYHUB_POSTGRES_URL", "postgres://relayhub:secret@postgres:5432/relayhub?sslmode=disable")
	t.Setenv("RELAYHUB_SECRET_ENCRYPTION_KEY", "base64-master-key")
	got, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.PostgresURL == "" || got.SecretEncryptionKey != "base64-master-key" {
		t.Fatalf("postgres config=%+v", got)
	}
	for _, missing := range []string{"RELAYHUB_POSTGRES_URL", "RELAYHUB_SECRET_ENCRYPTION_KEY"} {
		setRequiredEnvironment(t)
		t.Setenv("RELAYHUB_POSTGRES_URL", "postgres://postgres:5432/relayhub")
		t.Setenv("RELAYHUB_SECRET_ENCRYPTION_KEY", "key")
		t.Setenv(missing, "")
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), missing) {
			t.Fatalf("missing %s error=%v", missing, err)
		}
	}
}

func TestLoadUsesDocumentedNATSDefaults(t *testing.T) {
	setRequiredEnvironment(t)

	got, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got.NATSURL != "nats://localhost:4222" || got.NATSConnectTimeout != 2*time.Second || got.NATSReconnectWait != 2*time.Second || got.NATSMaxReconnects != -1 || got.NATSDrainTimeout != 10*time.Second || got.NATSStreamMaxAge != 7*24*time.Hour || got.NATSDuplicateWindow != 24*time.Hour || got.NATSReplicas != 1 {
		t.Fatalf("NATS defaults = %+v", got)
	}
}

func TestLoadParsesNATSConfiguration(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv("RELAYHUB_NATS_URL", "tls://relayhub-nats:4222")
	t.Setenv("RELAYHUB_NATS_USERNAME", "relayhub")
	t.Setenv("RELAYHUB_NATS_PASSWORD", "nats-secret")
	t.Setenv("RELAYHUB_NATS_CONNECT_TIMEOUT", "3s")
	t.Setenv("RELAYHUB_NATS_RECONNECT_WAIT", "250ms")
	t.Setenv("RELAYHUB_NATS_MAX_RECONNECTS", "12")
	t.Setenv("RELAYHUB_NATS_DRAIN_TIMEOUT", "9s")
	t.Setenv("RELAYHUB_NATS_STREAM_MAX_AGE", "48h")
	t.Setenv("RELAYHUB_NATS_DUPLICATE_WINDOW", "4h")
	t.Setenv("RELAYHUB_NATS_REPLICAS", "3")

	got, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got.NATSURL != "tls://relayhub-nats:4222" || got.NATSUsername != "relayhub" || got.NATSPassword != "nats-secret" || got.NATSConnectTimeout != 3*time.Second || got.NATSReconnectWait != 250*time.Millisecond || got.NATSMaxReconnects != 12 || got.NATSDrainTimeout != 9*time.Second || got.NATSStreamMaxAge != 48*time.Hour || got.NATSDuplicateWindow != 4*time.Hour || got.NATSReplicas != 3 {
		t.Fatalf("parsed NATS config = %+v", got)
	}
}

func TestLoadRejectsUnsafeNATSConfigurationWithoutExposingValues(t *testing.T) {
	tests := []struct{ name, variable, value string }{
		{"URL scheme", "RELAYHUB_NATS_URL", "https://secret.example.test"},
		{"URL credentials", "RELAYHUB_NATS_URL", "nats://user:password@localhost:4222"},
		{"max reconnects", "RELAYHUB_NATS_MAX_RECONNECTS", "-2"},
		{"replicas", "RELAYHUB_NATS_REPLICAS", "2"},
		{"duplicate window", "RELAYHUB_NATS_DUPLICATE_WINDOW", "0s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequiredEnvironment(t)
			t.Setenv(tt.variable, tt.value)
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), tt.variable) {
				t.Fatalf("Load() error = %v, want named rejection", err)
			}
			if strings.Contains(err.Error(), tt.value) {
				t.Fatalf("Load() error leaked value: %q", err)
			}
		})
	}
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
	t.Setenv("RELAYHUB_POSTGRES_URL", "postgres://relayhub:secret@postgres:5432/relayhub?sslmode=disable")
	t.Setenv("RELAYHUB_SECRET_ENCRYPTION_KEY", "base64-master-key")
}

func clearConfigEnvironment(t *testing.T) {
	t.Helper()
	for _, variable := range configEnvironment {
		t.Setenv(variable, "")
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
