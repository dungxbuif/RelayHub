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
	defaultRedisURL             = "redis://localhost:6379/0"
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
	RedisKeyPrefix         string
	HTTPAddr               string
	RedisURL               string
	AdminToken             string
	SigningSecret          string
	AllowInsecureCallbacks bool
	AllowedOrigins         []string
	EventRetention         time.Duration
	JobRetention           time.Duration
	IdempotencyRetention   time.Duration
	SigningSkew            time.Duration
	ShutdownTimeout        time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		WorkerHTTPAddr:    envOrDefault("RELAYHUB_WORKER_HTTP_ADDR", ":9090"),
		WorkerConcurrency: 8, CallbackTimeout: 10 * time.Second, WorkerReclaimIdle: 30 * time.Second,
		HTTPAddr:             envOrDefault("RELAYHUB_HTTP_ADDR", defaultHTTPAddr),
		RedisURL:             envOrDefault("RELAYHUB_REDIS_URL", defaultRedisURL),
		AdminToken:           strings.TrimSpace(os.Getenv("RELAYHUB_ADMIN_TOKEN")),
		SigningSecret:        strings.TrimSpace(os.Getenv("RELAYHUB_SIGNING_SECRET")),
		EventRetention:       defaultEventRetention,
		JobRetention:         defaultJobRetention,
		IdempotencyRetention: defaultIdempotencyRetention,
		SigningSkew:          defaultSigningSkew,
		ShutdownTimeout:      defaultShutdownTimeout,
	}

	if cfg.AdminToken == "" {
		return Config{}, fmt.Errorf("RELAYHUB_ADMIN_TOKEN is required")
	}
	if cfg.SigningSecret == "" {
		return Config{}, fmt.Errorf("RELAYHUB_SIGNING_SECRET is required")
	}
	if err := validateHTTPAddr(cfg.WorkerHTTPAddr); err != nil {
		return Config{}, fmt.Errorf("RELAYHUB_WORKER_HTTP_ADDR is invalid")
	}
	if err := validateHTTPAddr(cfg.HTTPAddr); err != nil {
		return Config{}, err
	}
	if err := validateRedisURL(cfg.RedisURL); err != nil {
		return Config{}, err
	}
	// A separate password avoids unsafe Compose string interpolation into URLs.
	if password := os.Getenv("RELAYHUB_REDIS_PASSWORD"); password != "" {
		parsed, _ := url.Parse(cfg.RedisURL)
		username := ""
		if parsed.User != nil {
			username = parsed.User.Username()
		}
		parsed.User = url.UserPassword(username, password)
		cfg.RedisURL = parsed.String()
	}

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
	cfg.RedisKeyPrefix = "relayhub"
	if raw, ok := os.LookupEnv("RELAYHUB_REDIS_KEY_PREFIX"); ok {
		cfg.RedisKeyPrefix = raw
	}
	if !regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`).MatchString(cfg.RedisKeyPrefix) {
		return Config{}, fmt.Errorf("RELAYHUB_REDIS_KEY_PREFIX is invalid")
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

func validateRedisURL(rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil || (parsed.Scheme != "redis" && parsed.Scheme != "rediss") || parsed.Host == "" {
		return fmt.Errorf("RELAYHUB_REDIS_URL is invalid")
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
