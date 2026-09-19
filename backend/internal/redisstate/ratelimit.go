package redisstate

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/redis/go-redis/v9"
)

type RateDecision struct {
	Allowed    bool
	Remaining  float64
	ResetAt    time.Time
	RetryAfter time.Duration
}

type RateLimiter struct {
	client *Client
}

var tokenBucketScript = redis.NewScript(`
local server_time = redis.call('TIME')
local now_ms = tonumber(server_time[1]) * 1000 + math.floor(tonumber(server_time[2]) / 1000)
local burst = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local ttl_ms = tonumber(ARGV[4])
local state = redis.call('HMGET', KEYS[1], 'tokens', 'last_ms')
local tokens = tonumber(state[1])
local last_ms = tonumber(state[2])
if not tokens or not last_ms then
  tokens = burst
  last_ms = now_ms
end
local elapsed_ms = math.max(0, now_ms - last_ms)
tokens = math.min(burst, tokens + elapsed_ms * burst / window_ms)
local allowed = 0
local retry_ms = 0
if tokens >= cost then
  allowed = 1
  tokens = tokens - cost
else
  retry_ms = math.ceil((cost - tokens) * window_ms / burst)
end
local reset_ms = now_ms + math.ceil((burst - tokens) * window_ms / burst)
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'last_ms', tostring(now_ms))
redis.call('PEXPIRE', KEYS[1], ttl_ms)
return {allowed, tostring(tokens), tostring(reset_ms), tostring(retry_ms)}
`)

func NewRateLimiter(client *Client) *RateLimiter {
	return &RateLimiter{client: client}
}

func (l *RateLimiter) Allow(ctx context.Context, key string, limit int64, window time.Duration, cost int64) (RateDecision, error) {
	if err := ctx.Err(); err != nil {
		return RateDecision{}, err
	}
	if l == nil || l.client == nil || !validRateLimitKey(key) || limit <= 0 || limit > 1<<53 || cost <= 0 || cost > limit || window < time.Second || window > 24*time.Hour {
		return RateDecision{}, ErrInvalidRecord
	}
	result, err := tokenBucketScript.Run(ctx, l.client.Universal(), []string{key}, limit, window.Milliseconds(), cost, (2 * window).Milliseconds()).Slice()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return RateDecision{}, ctxErr
		}
		return RateDecision{}, ErrUnavailable
	}
	if len(result) != 4 {
		return RateDecision{}, ErrUnavailable
	}
	allowed, err := parseScriptInt(result[0])
	if err != nil || (allowed != 0 && allowed != 1) {
		return RateDecision{}, ErrUnavailable
	}
	remaining, err := parseScriptFloat(result[1])
	if err != nil || remaining < 0 || remaining > float64(limit) {
		return RateDecision{}, ErrUnavailable
	}
	resetMS, err := parseScriptInt(result[2])
	if err != nil || resetMS <= 0 {
		return RateDecision{}, ErrUnavailable
	}
	retryMS, err := parseScriptInt(result[3])
	if err != nil || retryMS < 0 {
		return RateDecision{}, ErrUnavailable
	}
	return RateDecision{
		Allowed: allowed == 1, Remaining: remaining, ResetAt: time.UnixMilli(resetMS).UTC(),
		RetryAfter: time.Duration(retryMS) * time.Millisecond,
	}, nil
}

func validRateLimitKey(key string) bool {
	if key == "" || len(key) > 512 {
		return false
	}
	open := strings.IndexByte(key, '{')
	close := strings.IndexByte(key, '}')
	if open < 0 || close <= open+1 || strings.Contains(key[open+1:], "{") || strings.Contains(key[close+1:], "}") {
		return false
	}
	for _, r := range key {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func parseScriptInt(value any) (int64, error) {
	return strconv.ParseInt(fmt.Sprint(value), 10, 64)
}

func parseScriptFloat(value any) (float64, error) {
	parsed, err := strconv.ParseFloat(fmt.Sprint(value), 64)
	if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return 0, ErrUnavailable
	}
	return parsed, nil
}
