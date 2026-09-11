package redisstore

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	client *redis.Client
}

func NewClient(rawURL string) (*Client, error) {
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, err
	}
	return &Client{client: redis.NewClient(options)}, nil
}

func (client *Client) Ping(ctx context.Context) error {
	return client.client.Ping(ctx).Err()
}

func (client *Client) Close() error {
	return client.client.Close()
}
