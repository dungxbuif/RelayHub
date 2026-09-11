package redisstore

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	reclaimMu     sync.Mutex
	reclaimCursor string
	prefix        string
	client        *redis.Client
	jobRetention  time.Duration
}

func NewClient(rawURL string, jobRetention ...time.Duration) (*Client, error) {
	return NewClientWithPrefix(rawURL, "relayhub", jobRetention...)
}

// NewClientWithPrefix applies one namespace to every durable key and notification.
func NewClientWithPrefix(rawURL, prefix string, jobRetention ...time.Duration) (*Client, error) {
	if !regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`).MatchString(prefix) {
		return nil, fmt.Errorf("invalid Redis key prefix")
	}
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, err
	}
	options.ContextTimeoutEnabled = true
	retention := 7 * 24 * time.Hour
	if len(jobRetention) > 0 && jobRetention[0] > 0 {
		retention = jobRetention[0]
	}
	return &Client{prefix: prefix, client: redis.NewClient(options), jobRetention: retention}, nil
}

func (client *Client) Ping(ctx context.Context) error {
	return client.client.Ping(ctx).Err()
}

func (client *Client) Close() error {
	return client.client.Close()
}

func (c *Client) key(defaultKey string) string {
	return c.prefix + ":" + strings.TrimPrefix(defaultKey, "relayhub:")
}
func (c *Client) eventKey(id string) string      { return c.key(eventKey(id)) }
func (c *Client) jobKey(id string) string        { return c.key(jobKey(id)) }
func (c *Client) queueKey(target string) string  { return c.key(queueKey(target)) }
func (c *Client) streamKey(target string) string { return c.key(streamKey(target)) }
func (c *Client) acknowledgementKey(target, event string) string {
	return c.key(acknowledgementKey(target, event))
}
func (c *Client) idempotencyKey(source, key string) string { return c.key(idempotencyKey(source, key)) }
func (c *Client) applicationKey(id string) string          { return c.key(applicationKey(id)) }
func (c *Client) credentialKey(hash string) string         { return c.key(credentialKey(hash)) }
