package redisstate

import (
	"context"
	"strconv"
	"time"
)

type RealtimePublishLimits struct {
	App        int64
	Connection int64
	Channel    int64
	Window     time.Duration
}

type RealtimePublishLimiter struct {
	limiter *RateLimiter
	keys    Keyspace
	limits  RealtimePublishLimits
}

func NewRealtimePublishLimiter(client *Client, keys Keyspace, limits RealtimePublishLimits) *RealtimePublishLimiter {
	return &RealtimePublishLimiter{limiter: NewRateLimiter(client), keys: keys, limits: limits}
}

func (limiter *RealtimePublishLimiter) Allow(ctx context.Context, appID, connectionID, channel string) (bool, error) {
	if limiter == nil || limiter.limiter == nil || limiter.limits.App < 1 || limiter.limits.Connection < 1 || limiter.limits.Channel < 1 || limiter.limits.Window < time.Second || limiter.limits.Window > 24*time.Hour {
		return false, ErrInvalidRecord
	}
	window := strconv.FormatInt(limiter.limits.Window.Milliseconds(), 10)
	dimensions := []struct {
		name  string
		limit int64
	}{
		{"realtime-publish-app", limiter.limits.App},
		{"realtime-publish-connection:" + connectionID, limiter.limits.Connection},
		{"realtime-publish-channel:" + channel, limiter.limits.Channel},
	}
	for _, dimension := range dimensions {
		key, err := limiter.keys.RateLimit(appID, dimension.name, window)
		if err != nil {
			return false, ErrInvalidRecord
		}
		decision, err := limiter.limiter.Allow(ctx, key, dimension.limit, limiter.limits.Window, 1)
		if err != nil {
			return false, err
		}
		if !decision.Allowed {
			return false, nil
		}
	}
	return true, nil
}
