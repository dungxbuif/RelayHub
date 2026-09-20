package redisstate

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrForbidden = errors.New("Redis operation forbidden")

type RealtimeMessageAction struct {
	ID             string          `json:"id"`
	AppID          string          `json:"app_id"`
	Channel        string          `json:"channel"`
	MessageID      string          `json:"message_id"`
	ClientID       string          `json:"client_id"`
	Type           string          `json:"type"`
	IdempotencyKey string          `json:"idempotency_key"`
	Data           json.RawMessage `json:"data"`
	CreatedAt      time.Time       `json:"created_at"`
	RemovedAt      *time.Time      `json:"removed_at,omitempty"`
}

type RealtimeActionStore struct {
	client     *Client
	keys       Keyspace
	maxActions int64
	ttl        time.Duration
}

func NewRealtimeActionStore(client *Client, keys Keyspace, maxActions int64, ttl time.Duration) *RealtimeActionStore {
	return &RealtimeActionStore{client: client, keys: keys, maxActions: maxActions, ttl: ttl}
}

var putRealtimeActionScript = redis.NewScript(`
if redis.call('HEXISTS', KEYS[1], '_exists') == 0 then return {'not_found'} end
local existing = redis.call('HGET', KEYS[1], 'idem:' .. ARGV[1] .. ':' .. ARGV[2])
if existing then
  local value = redis.call('HGET', KEYS[1], 'action:' .. existing)
  if value then return {'ok', value} end
end
local count = tonumber(redis.call('HGET', KEYS[1], '_count') or '0')
if count >= tonumber(ARGV[3]) then return {'limit'} end
redis.call('HSET', KEYS[1], 'action:' .. ARGV[4], ARGV[5], 'owner:' .. ARGV[4], ARGV[1], 'idem:' .. ARGV[1] .. ':' .. ARGV[2], ARGV[4], '_count', count + 1)
redis.call('PEXPIRE', KEYS[1], ARGV[6])
return {'ok', ARGV[5]}
`)

var removeRealtimeActionScript = redis.NewScript(`
local value = redis.call('HGET', KEYS[1], 'action:' .. ARGV[1])
if not value then return {'not_found'} end
if redis.call('HGET', KEYS[1], 'owner:' .. ARGV[1]) ~= ARGV[2] then return {'forbidden'} end
local decoded = cjson.decode(value)
if not decoded['removed_at'] then
  decoded['removed_at'] = ARGV[3]
  value = cjson.encode(decoded)
  redis.call('HSET', KEYS[1], 'action:' .. ARGV[1], value)
end
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return {'ok', value}
`)

func (store *RealtimeActionStore) RecordMessage(ctx context.Context, appID, channel, messageID string, encrypted bool) error {
	key, err := store.key(appID, channel, messageID)
	if err != nil {
		return err
	}
	pipe := store.client.Universal().TxPipeline()
	pipe.HSet(ctx, key, "_exists", "1", "_encrypted", encrypted)
	pipe.PExpire(ctx, key, store.ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return ErrUnavailable
	}
	return nil
}

func (store *RealtimeActionStore) Put(ctx context.Context, action RealtimeMessageAction) (RealtimeMessageAction, error) {
	key, err := store.key(action.AppID, action.Channel, action.MessageID)
	raw, marshalErr := json.Marshal(action)
	if err != nil || marshalErr != nil || len(raw) > 8192 || action.ID == "" || action.ClientID == "" || action.IdempotencyKey == "" {
		return RealtimeMessageAction{}, ErrInvalidRecord
	}
	result, err := putRealtimeActionScript.Run(ctx, store.client.Universal(), []string{key}, action.ClientID, action.IdempotencyKey, store.maxActions, action.ID, raw, store.ttl.Milliseconds()).Slice()
	if err != nil || len(result) == 0 {
		return RealtimeMessageAction{}, ErrUnavailable
	}
	status, _ := result[0].(string)
	switch status {
	case "not_found":
		return RealtimeMessageAction{}, ErrNotFound
	case "limit":
		return RealtimeMessageAction{}, ErrLimitExceeded
	case "ok":
		if len(result) != 2 {
			return RealtimeMessageAction{}, ErrCorruptRecord
		}
		value, ok := result[1].(string)
		if !ok || json.Unmarshal([]byte(value), &action) != nil {
			return RealtimeMessageAction{}, ErrCorruptRecord
		}
		return action, nil
	default:
		return RealtimeMessageAction{}, ErrCorruptRecord
	}
}

func (store *RealtimeActionStore) List(ctx context.Context, appID, channel, messageID string) ([]RealtimeMessageAction, error) {
	key, err := store.key(appID, channel, messageID)
	if err != nil {
		return nil, err
	}
	values, err := store.client.Universal().HGetAll(ctx, key).Result()
	if err != nil {
		return nil, ErrUnavailable
	}
	if values["_exists"] != "1" {
		return nil, ErrNotFound
	}
	actions := make([]RealtimeMessageAction, 0)
	for field, value := range values {
		if !strings.HasPrefix(field, "action:") {
			continue
		}
		var action RealtimeMessageAction
		if json.Unmarshal([]byte(value), &action) != nil || action.AppID != appID || action.Channel != channel || action.MessageID != messageID {
			return nil, ErrCorruptRecord
		}
		actions = append(actions, action)
	}
	sort.Slice(actions, func(i, j int) bool { return actions[i].CreatedAt.Before(actions[j].CreatedAt) })
	return actions, nil
}

func (store *RealtimeActionStore) Remove(ctx context.Context, appID, channel, messageID, actionID, clientID string) (RealtimeMessageAction, error) {
	key, err := store.key(appID, channel, messageID)
	if err != nil {
		return RealtimeMessageAction{}, err
	}
	now := time.Now().UTC()
	result, err := removeRealtimeActionScript.Run(ctx, store.client.Universal(), []string{key}, actionID, clientID, now.Format(time.RFC3339Nano), store.ttl.Milliseconds()).Slice()
	if err != nil || len(result) == 0 {
		return RealtimeMessageAction{}, ErrUnavailable
	}
	status, _ := result[0].(string)
	if status == "not_found" {
		return RealtimeMessageAction{}, ErrNotFound
	}
	if status == "forbidden" {
		return RealtimeMessageAction{}, ErrForbidden
	}
	if status != "ok" || len(result) != 2 {
		return RealtimeMessageAction{}, ErrCorruptRecord
	}
	value, ok := result[1].(string)
	var action RealtimeMessageAction
	if !ok || json.Unmarshal([]byte(value), &action) != nil {
		return action, ErrCorruptRecord
	}
	return action, nil
}

func (store *RealtimeActionStore) key(appID, channel, messageID string) (string, error) {
	if store == nil || store.client == nil || store.maxActions < 1 || store.maxActions > 100 || store.ttl <= 0 {
		return "", ErrInvalidRecord
	}
	return store.keys.RealtimeMessageActions(appID, channel, messageID)
}
