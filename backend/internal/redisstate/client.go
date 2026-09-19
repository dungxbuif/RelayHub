package redisstate

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type Mode string

const (
	ModeStandalone Mode = "standalone"
	ModeSentinel   Mode = "sentinel"
	ModeCluster    Mode = "cluster"
)

var (
	ErrUnavailable   = errors.New("Redis unavailable")
	ErrInvalidConfig = errors.New("invalid Redis configuration")
)

type Config struct {
	Mode           Mode
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

type Client struct {
	universal redis.UniversalClient
}

type clientKind uint8

const (
	clientStandalone clientKind = iota + 1
	clientSentinel
	clientCluster
)

type builtOptions struct {
	kind       clientKind
	standalone *redis.Options
	sentinel   *redis.FailoverOptions
	cluster    *redis.ClusterOptions
}

func New(ctx context.Context, cfg Config) (*Client, error) {
	options, err := buildOptions(cfg)
	if err != nil {
		return nil, err
	}

	var universal redis.UniversalClient
	switch options.kind {
	case clientStandalone:
		universal = redis.NewClient(options.standalone)
	case clientSentinel:
		universal = redis.NewFailoverClient(options.sentinel)
	case clientCluster:
		universal = redis.NewClusterClient(options.cluster)
	default:
		return nil, ErrInvalidConfig
	}

	client := &Client{universal: universal}
	pingCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()
	if err := client.Ping(pingCtx); err != nil {
		_ = universal.Close()
		return nil, ErrUnavailable
	}
	return client, nil
}

func (c *Client) Ping(ctx context.Context) error {
	if c == nil || c.universal == nil {
		return ErrUnavailable
	}
	if err := c.universal.Ping(ctx).Err(); err != nil {
		return ErrUnavailable
	}
	return nil
}

func (c *Client) Close() error {
	if c == nil || c.universal == nil {
		return nil
	}
	if err := c.universal.Close(); err != nil {
		return ErrUnavailable
	}
	return nil
}

func (c *Client) Universal() redis.UniversalClient {
	if c == nil {
		return nil
	}
	return c.universal
}

func buildOptions(cfg Config) (builtOptions, error) {
	if err := validateConfig(cfg); err != nil {
		return builtOptions{}, err
	}

	var tlsConfig *tls.Config
	if cfg.TLS {
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS13}
	}

	switch cfg.Mode {
	case ModeStandalone:
		return builtOptions{kind: clientStandalone, standalone: &redis.Options{
			Addr: cfg.Addrs[0], Username: cfg.Username, Password: cfg.Password, DB: cfg.DB,
			DialTimeout: cfg.ConnectTimeout, ReadTimeout: cfg.ReadTimeout, WriteTimeout: cfg.WriteTimeout,
			PoolSize: cfg.PoolSize, MaxRetries: -1, TLSConfig: tlsConfig,
		}}, nil
	case ModeSentinel:
		return builtOptions{kind: clientSentinel, sentinel: &redis.FailoverOptions{
			MasterName: cfg.SentinelMaster, SentinelAddrs: append([]string(nil), cfg.Addrs...),
			Username: cfg.Username, Password: cfg.Password, DB: cfg.DB,
			DialTimeout: cfg.ConnectTimeout, ReadTimeout: cfg.ReadTimeout, WriteTimeout: cfg.WriteTimeout,
			PoolSize: cfg.PoolSize, MaxRetries: -1, TLSConfig: tlsConfig,
		}}, nil
	case ModeCluster:
		return builtOptions{kind: clientCluster, cluster: &redis.ClusterOptions{
			Addrs: append([]string(nil), cfg.Addrs...), Username: cfg.Username, Password: cfg.Password,
			DialTimeout: cfg.ConnectTimeout, ReadTimeout: cfg.ReadTimeout, WriteTimeout: cfg.WriteTimeout,
			PoolSize: cfg.PoolSize, MaxRetries: -1, MaxRedirects: -1, TLSConfig: tlsConfig,
		}}, nil
	default:
		return builtOptions{}, ErrInvalidConfig
	}
}

func validateConfig(cfg Config) error {
	if len(cfg.Addrs) == 0 || cfg.ConnectTimeout <= 0 || cfg.ReadTimeout <= 0 || cfg.WriteTimeout <= 0 || cfg.PoolSize <= 0 {
		return ErrInvalidConfig
	}
	for _, addr := range cfg.Addrs {
		if safeAddress(addr) != addr {
			return ErrInvalidConfig
		}
	}
	switch cfg.Mode {
	case ModeStandalone:
		if len(cfg.Addrs) != 1 || cfg.SentinelMaster != "" {
			return ErrInvalidConfig
		}
	case ModeSentinel:
		if cfg.SentinelMaster == "" {
			return ErrInvalidConfig
		}
	case ModeCluster:
		if cfg.DB != 0 || cfg.SentinelMaster != "" {
			return ErrInvalidConfig
		}
	default:
		return ErrInvalidConfig
	}
	return nil
}

func (o builtOptions) addresses() []string {
	switch o.kind {
	case clientStandalone:
		return []string{o.standalone.Addr}
	case clientSentinel:
		return o.sentinel.SentinelAddrs
	case clientCluster:
		return o.cluster.Addrs
	default:
		return nil
	}
}

func (o builtOptions) maxRetries() int {
	switch o.kind {
	case clientStandalone:
		return o.standalone.MaxRetries
	case clientSentinel:
		return o.sentinel.MaxRetries
	case clientCluster:
		return o.cluster.MaxRetries
	default:
		return 0
	}
}

func safeAddresses(addresses []string) []string {
	safe := make([]string, len(addresses))
	for i, address := range addresses {
		safe[i] = safeAddress(address)
	}
	return safe
}

func safeAddress(address string) string {
	raw := strings.TrimSpace(address)
	if raw == "" || strings.ContainsAny(raw, "\x00\r\n ") {
		return "<invalid>"
	}
	parseTarget := raw
	if !strings.Contains(raw, "://") {
		parseTarget = "redis://" + raw
	}
	parsed, err := url.Parse(parseTarget)
	if err != nil {
		return "<invalid>"
	}
	hostport := parsed.Host
	host, port, err := net.SplitHostPort(hostport)
	if err != nil || host == "" {
		return "<invalid>"
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return "<invalid>"
	}
	return net.JoinHostPort(host, port)
}
