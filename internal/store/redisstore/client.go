package redisstore

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	client       *redis.Client
	jobRetention time.Duration
}

func NewClient(rawURL string, jobRetention ...time.Duration) (*Client, error) {
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, err
	}
	retention := 7 * 24 * time.Hour
	if len(jobRetention) > 0 && jobRetention[0] > 0 {
		retention = jobRetention[0]
	}
	return &Client{client: redis.NewClient(options), jobRetention: retention}, nil
}

func (client *Client) Ping(ctx context.Context) error {
	return client.client.Ping(ctx).Err()
}

func (client *Client) Close() error {
	return client.client.Close()
}
