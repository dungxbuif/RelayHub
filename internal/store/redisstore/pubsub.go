package redisstore

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/realtime"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/redis/go-redis/v9"
)

type notification struct {
	AppID string               `json:"app_id"`
	Frame realtime.ServerFrame `json:"frame"`
}

// Bridge uses one pattern subscription per API instance. Pub/Sub is a best-effort
// hint, with local bounded fan-out and the durable queue as recovery authority.
type Bridge struct {
	client *Client
	sub    *redis.PubSub
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

func NewBridge(ctx context.Context, c *Client, h *realtime.Hub) (*Bridge, error) {
	ctx, cancel := context.WithCancel(ctx)
	sub := c.client.PSubscribe(ctx, c.prefix+":pubsub:*")
	startup, stop := context.WithTimeout(ctx, 3*time.Second)
	_, err := sub.Receive(startup)
	stop()
	if err != nil {
		cancel()
		_ = sub.Close()
		return nil, err
	}
	b := &Bridge{client: c, sub: sub, cancel: cancel, done: make(chan struct{})}
	messages := sub.Channel(redis.WithChannelSize(64))
	go func() {
		defer close(b.done)
		defer sub.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case message, ok := <-messages:
				if !ok {
					return
				}
				var n notification
				if json.Unmarshal([]byte(message.Payload), &n) != nil || n.AppID == "" || message.Channel != b.channel(n.AppID) {
					continue
				}
				h.Deliver(n.AppID, n.Frame)
			}
		}
	}()
	return b, nil
}
func (b *Bridge) channel(app string) string {
	return b.client.prefix + ":pubsub:" + hex.EncodeToString([]byte(app))
}
func (b *Bridge) publish(ctx context.Context, app string, frame realtime.ServerFrame) error {
	raw, err := json.Marshal(notification{AppID: app, Frame: frame})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	return b.client.client.Publish(ctx, b.channel(app), raw).Err()
}
func (b *Bridge) PublishEvent(ctx context.Context, e domain.Event) error {
	var errs []error
	for _, target := range e.TargetAppIDs {
		if err := b.publish(ctx, target, realtime.ServerFrame{Type: "event", Event: &e}); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
func (b *Bridge) PublishJob(ctx context.Context, j domain.Job) error {
	return b.publish(ctx, j.TargetAppID, realtime.ServerFrame{Type: "job.updated", Job: &j})
}
func (b *Bridge) Close() { b.once.Do(func() { b.cancel(); _ = b.sub.Close(); <-b.done }) }
func (c *Client) GetEventJob(ctx context.Context, target, event string) (domain.Job, error) {
	id, err := c.client.Get(ctx, c.acknowledgementKey(target, event)).Result()
	if errors.Is(err, redis.Nil) {
		return domain.Job{}, store.ErrNotFound
	}
	if err != nil {
		return domain.Job{}, err
	}
	j, err := c.GetJob(ctx, id)
	if err == nil && (j.TargetAppID != target || j.EventID != event) {
		return domain.Job{}, store.ErrNotFound
	}
	return j, err
}
