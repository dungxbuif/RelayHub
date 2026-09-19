package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	defaultHTTPAddr             = ":8080"
	defaultNATSURL              = "nats://localhost:4222"
	defaultEventRetention       = 7 * 24 * time.Hour
	defaultJobRetention         = 7 * 24 * time.Hour
	defaultIdempotencyRetention = 24 * time.Hour
	defaultSigningSkew          = 5 * time.Minute
	defaultShutdownTimeout      = 10 * time.Second
)

type Config struct {
	WorkerHTTPAddr         string
	WorkerConcurrency      int
	CallbackTimeout        time.Duration
	WorkerReclaimIdle      time.Duration
	HTTPAddr               string
	AdminToken             string
	SigningSecret          string
	AllowInsecureCallbacks bool
	AllowedOrigins         []string
	EventRetention         time.Duration
	JobRetention           time.Duration
	IdempotencyRetention   time.Duration
	SigningSkew            time.Duration
	ShutdownTimeout        time.Duration
	NATSURL                string
	NATSUsername           string
	NATSPassword           string
	NATSConnectTimeout     time.Duration
	NATSReconnectWait      time.Duration
	NATSMaxReconnects      int
	NATSDrainTimeout       time.Duration
	NATSStreamMaxAge       time.Duration
	NATSDuplicateWindow    time.Duration
	NATSReplicas           int
	Redis                  RedisConfig
	PostgresURL            string
	SecretEncryptionKey    string
}

type RedisConfig struct {
	Mode           string
	Addrs          []string
	Username       string
	Password       string
	SentinelMaster string
	DB             int
	TLS            bool
	KeyPrefix      string
	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	PoolSize       int
}

var redisKeyPrefixPattern = regexp.MustCompile(`^[a-z0-9:_-]{1,32}$`)

func Load() (Config, error) {
	cfg := Config{
		WorkerHTTPAddr:    envOrDefault("RELAYHUB_WORKER_HTTP_ADDR", ":9090"),
		WorkerConcurrency: 8, CallbackTimeout: 10 * time.Second, WorkerReclaimIdle: 30 * time.Second,
		HTTPAddr:             envOrDefault("RELAYHUB_HTTP_ADDR", defaultHTTPAddr),
		AdminToken:           strings.TrimSpace(os.Getenv("RELAYHUB_ADMIN_TOKEN")),
		SigningSecret:        strings.TrimSpace(os.Getenv("RELAYHUB_SIGNING_SECRET")),
		EventRetention:       defaultEventRetention,
		JobRetention:         defaultJobRetention,
		IdempotencyRetention: defaultIdempotencyRetention,
		SigningSkew:          defaultSigningSkew,
		ShutdownTimeout:      defaultShutdownTimeout,
		NATSURL:              envOrDefault("RELAYHUB_NATS_URL", defaultNATSURL),
		NATSUsername:         strings.TrimSpace(os.Getenv("RELAYHUB_NATS_USERNAME")),
		NATSPassword:         os.Getenv("RELAYHUB_NATS_PASSWORD"),
		NATSConnectTimeout:   2 * time.Second,
		NATSReconnectWait:    2 * time.Second,
		NATSMaxReconnects:    -1,
		NATSDrainTimeout:     10 * time.Second,
		NATSStreamMaxAge:     7 * 24 * time.Hour,
		NATSDuplicateWindow:  24 * time.Hour,
		NATSReplicas:         1,
		Redis: RedisConfig{
			Mode: "standalone", Addrs: []string{"localhost:6379"}, KeyPrefix: "rh",
			ConnectTimeout: 2 * time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second, PoolSize: 32,
		},
		PostgresURL:         strings.TrimSpace(os.Getenv("RELAYHUB_POSTGRES_URL")),
		SecretEncryptionKey: strings.TrimSpace(os.Getenv("RELAYHUB_SECRET_ENCRYPTION_KEY")),
	}

	if cfg.AdminToken == "" {
		return Config{}, fmt.Errorf("RELAYHUB_ADMIN_TOKEN is required")
	}
	if cfg.SigningSecret == "" {
		return Config{}, fmt.Errorf("RELAYHUB_SIGNING_SECRET is required")
	}
	if cfg.PostgresURL == "" {
		return Config{}, fmt.Errorf("RELAYHUB_POSTGRES_URL is required")
	}
	if cfg.SecretEncryptionKey == "" {
		return Config{}, fmt.Errorf("RELAYHUB_SECRET_ENCRYPTION_KEY is required")
	}
	if err := validateHTTPAddr(cfg.WorkerHTTPAddr); err != nil {
		return Config{}, fmt.Errorf("RELAYHUB_WORKER_HTTP_ADDR is invalid")
	}
	if err := validateHTTPAddr(cfg.HTTPAddr); err != nil {
		return Config{}, err
	}
	if err := validateNATSURL(cfg.NATSURL); err != nil {
		return Config{}, err
	}
	redisConfig, err := loadRedisConfig(cfg.Redis)
	if err != nil {
		return Config{}, err
	}
	cfg.Redis = redisConfig

	durations := []struct {
		name   string
		target *time.Duration
	}{
		{name: "RELAYHUB_CALLBACK_TIMEOUT", target: &cfg.CallbackTimeout},
		{name: "RELAYHUB_WORKER_RECLAIM_IDLE", target: &cfg.WorkerReclaimIdle},
		{name: "RELAYHUB_EVENT_RETENTION", target: &cfg.EventRetention},
		{name: "RELAYHUB_JOB_RETENTION", target: &cfg.JobRetention},
		{name: "RELAYHUB_IDEMPOTENCY_RETENTION", target: &cfg.IdempotencyRetention},
		{name: "RELAYHUB_SIGNING_SKEW", target: &cfg.SigningSkew},
		{name: "RELAYHUB_SHUTDOWN_TIMEOUT", target: &cfg.ShutdownTimeout},
		{name: "RELAYHUB_NATS_CONNECT_TIMEOUT", target: &cfg.NATSConnectTimeout},
		{name: "RELAYHUB_NATS_RECONNECT_WAIT", target: &cfg.NATSReconnectWait},
		{name: "RELAYHUB_NATS_DRAIN_TIMEOUT", target: &cfg.NATSDrainTimeout},
		{name: "RELAYHUB_NATS_STREAM_MAX_AGE", target: &cfg.NATSStreamMaxAge},
		{name: "RELAYHUB_NATS_DUPLICATE_WINDOW", target: &cfg.NATSDuplicateWindow},
	}
	for _, duration := range durations {
		if err := loadPositiveDuration(duration.name, duration.target); err != nil {
			return Config{}, err
		}
	}

	if cfg.WorkerReclaimIdle < cfg.CallbackTimeout+5*time.Second {
		return Config{}, fmt.Errorf("RELAYHUB_WORKER_RECLAIM_IDLE must exceed RELAYHUB_CALLBACK_TIMEOUT by at least 5s")
	}
	if raw := strings.TrimSpace(os.Getenv("RELAYHUB_WORKER_CONCURRENCY")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 1024 {
			return Config{}, fmt.Errorf("RELAYHUB_WORKER_CONCURRENCY is invalid")
		}
		cfg.WorkerConcurrency = n
	}
	if raw := strings.TrimSpace(os.Getenv("RELAYHUB_NATS_MAX_RECONNECTS")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < -1 {
			return Config{}, fmt.Errorf("RELAYHUB_NATS_MAX_RECONNECTS is invalid")
		}
		cfg.NATSMaxReconnects = n
	}
	if raw := strings.TrimSpace(os.Getenv("RELAYHUB_NATS_REPLICAS")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || (n != 1 && n != 3 && n != 5) {
			return Config{}, fmt.Errorf("RELAYHUB_NATS_REPLICAS is invalid")
		}
		cfg.NATSReplicas = n
	}
	origins, err := loadAllowedOrigins()
	if err != nil {
		return Config{}, err
	}
	cfg.AllowedOrigins = origins

	allowInsecureCallbacks, err := loadOptionalBool("RELAYHUB_ALLOW_INSECURE_CALLBACKS")
	if err != nil {
		return Config{}, err
	}
	cfg.AllowInsecureCallbacks = allowInsecureCallbacks

	return cfg, nil
}

func loadRedisConfig(cfg RedisConfig) (RedisConfig, error) {
	cfg.Mode = envOrDefault("RELAYHUB_REDIS_MODE", cfg.Mode)
	if cfg.Mode != "standalone" && cfg.Mode != "sentinel" && cfg.Mode != "cluster" {
		return RedisConfig{}, fmt.Errorf("RELAYHUB_REDIS_MODE is invalid")
	}

	rawAddrs := strings.TrimSpace(os.Getenv("RELAYHUB_REDIS_ADDRS"))
	if rawAddrs == "" {
		rawAddrs = strings.Join(cfg.Addrs, ",")
	}
	cfg.Addrs = cfg.Addrs[:0]
	seen := make(map[string]struct{})
	for _, rawAddr := range strings.Split(rawAddrs, ",") {
		addr := strings.TrimSpace(rawAddr)
		if !validRedisAddress(addr) {
			return RedisConfig{}, fmt.Errorf("RELAYHUB_REDIS_ADDRS is invalid")
		}
		if _, exists := seen[addr]; exists {
			return RedisConfig{}, fmt.Errorf("RELAYHUB_REDIS_ADDRS is invalid")
		}
		seen[addr] = struct{}{}
		cfg.Addrs = append(cfg.Addrs, addr)
	}
	if len(cfg.Addrs) == 0 || cfg.Mode == "standalone" && len(cfg.Addrs) != 1 {
		return RedisConfig{}, fmt.Errorf("RELAYHUB_REDIS_ADDRS is invalid")
	}

	cfg.Username = strings.TrimSpace(os.Getenv("RELAYHUB_REDIS_USERNAME"))
	cfg.Password = os.Getenv("RELAYHUB_REDIS_PASSWORD")
	if strings.ContainsAny(cfg.Username, "\x00\r\n") || strings.ContainsAny(cfg.Password, "\x00\r\n") {
		return RedisConfig{}, fmt.Errorf("Redis credentials are invalid")
	}
	cfg.SentinelMaster = strings.TrimSpace(os.Getenv("RELAYHUB_REDIS_SENTINEL_MASTER"))
	if cfg.Mode == "sentinel" {
		if cfg.SentinelMaster == "" || len(cfg.SentinelMaster) > 256 || strings.ContainsAny(cfg.SentinelMaster, "\x00\r\n ") {
			return RedisConfig{}, fmt.Errorf("RELAYHUB_REDIS_SENTINEL_MASTER is invalid")
		}
	} else if cfg.SentinelMaster != "" {
		return RedisConfig{}, fmt.Errorf("RELAYHUB_REDIS_SENTINEL_MASTER is invalid")
	}

	if raw := strings.TrimSpace(os.Getenv("RELAYHUB_REDIS_DB")); raw != "" {
		db, err := strconv.Atoi(raw)
		if err != nil || db < 0 || db > 15 {
			return RedisConfig{}, fmt.Errorf("RELAYHUB_REDIS_DB is invalid")
		}
		cfg.DB = db
	}
	if cfg.Mode == "cluster" && cfg.DB != 0 {
		return RedisConfig{}, fmt.Errorf("RELAYHUB_REDIS_DB is invalid")
	}

	tlsEnabled, err := loadOptionalBool("RELAYHUB_REDIS_TLS")
	if err != nil {
		return RedisConfig{}, err
	}
	cfg.TLS = tlsEnabled
	cfg.KeyPrefix = envOrDefault("RELAYHUB_REDIS_KEY_PREFIX", cfg.KeyPrefix)
	if !redisKeyPrefixPattern.MatchString(cfg.KeyPrefix) {
		return RedisConfig{}, fmt.Errorf("RELAYHUB_REDIS_KEY_PREFIX is invalid")
	}
	for _, duration := range []struct {
		name   string
		target *time.Duration
	}{
		{name: "RELAYHUB_REDIS_CONNECT_TIMEOUT", target: &cfg.ConnectTimeout},
		{name: "RELAYHUB_REDIS_READ_TIMEOUT", target: &cfg.ReadTimeout},
		{name: "RELAYHUB_REDIS_WRITE_TIMEOUT", target: &cfg.WriteTimeout},
	} {
		if err := loadPositiveDuration(duration.name, duration.target); err != nil {
			return RedisConfig{}, err
		}
	}
	if raw := strings.TrimSpace(os.Getenv("RELAYHUB_REDIS_POOL_SIZE")); raw != "" {
		poolSize, err := strconv.Atoi(raw)
		if err != nil || poolSize < 1 || poolSize > 4096 {
			return RedisConfig{}, fmt.Errorf("RELAYHUB_REDIS_POOL_SIZE is invalid")
		}
		cfg.PoolSize = poolSize
	}
	return cfg, nil
}

func validRedisAddress(address string) bool {
	if address == "" || strings.ContainsAny(address, "/@?#\x00\r\n ") {
		return false
	}
	host, rawPort, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return false
	}
	port, err := strconv.Atoi(rawPort)
	return err == nil && port >= 1 && port <= 65535
}

func loadOptionalBool(name string) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return false, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s is invalid", name)
	}
	return value, nil
}

func envOrDefault(name, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}

func validateHTTPAddr(address string) error {
	_, port, err := net.SplitHostPort(address)
	if err != nil || port == "" {
		return fmt.Errorf("RELAYHUB_HTTP_ADDR is invalid")
	}
	return nil
}

func validateNATSURL(rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil || (parsed.Scheme != "nats" && parsed.Scheme != "tls") || parsed.Host == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("RELAYHUB_NATS_URL is invalid")
	}
	return nil
}

func loadPositiveDuration(name string, target *time.Duration) error {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil || parsed <= 0 {
		return fmt.Errorf("%s is invalid", name)
	}
	*target = parsed
	return nil
}

func loadAllowedOrigins() ([]string, error) {
	raw := strings.TrimSpace(os.Getenv("RELAYHUB_ALLOWED_ORIGINS"))
	if raw == "" {
		return nil, nil
	}

	origins := make([]string, 0)
	seen := make(map[string]struct{})
	for _, candidate := range strings.Split(raw, ",") {
		origin := strings.TrimSpace(candidate)
		if origin == "" {
			continue
		}
		if origin == "*" || !validOrigin(origin) {
			return nil, fmt.Errorf("RELAYHUB_ALLOWED_ORIGINS is invalid")
		}
		if _, exists := seen[origin]; exists {
			continue
		}
		seen[origin] = struct{}{}
		origins = append(origins, origin)
	}
	return origins, nil
}

func validOrigin(origin string) bool {
	parsed, err := url.Parse(origin)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return false
	}
	return parsed.User == nil && parsed.Path == "" && parsed.RawQuery == "" && parsed.Fragment == ""
}
