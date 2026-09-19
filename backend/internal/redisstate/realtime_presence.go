package redisstate

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

type RealtimePresence struct {
	AppID        string          `json:"app_id"`
	Channel      string          `json:"channel"`
	ClientID     string          `json:"client_id"`
	ConnectionID string          `json:"connection_id"`
	Data         json.RawMessage `json:"data"`
	LastSeenAt   time.Time       `json:"last_seen_at"`
}

type RealtimePresenceStore struct {
	client *Client
	keys   Keyspace
}

var upsertPresenceScript = redis.NewScript(`
local joined = 0
if redis.call('EXISTS', KEYS[1]) == 0 then joined = 1 end
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[1])
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
redis.call('ZADD', KEYS[2], ARGV[4], ARGV[5])
return {joined, redis.call('ZCARD', KEYS[2])}
`)

var deletePresenceScript = redis.NewScript(`
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[2])
return redis.call('ZCARD', KEYS[2])
`)

func NewRealtimePresenceStore(client *Client, keys Keyspace) *RealtimePresenceStore {
	return &RealtimePresenceStore{client: client, keys: keys}
}

func (store *RealtimePresenceStore) Upsert(ctx context.Context, presence RealtimePresence, ttl time.Duration) (bool, int, error) {
	if !json.Valid(presence.Data) {
		return false, 0, ErrInvalidRecord
	}
	memberKey, indexKey, err := store.input(presence, ttl)
	if err != nil {
		return false, 0, err
	}
	presence.LastSeenAt = time.Now().UTC()
	payload, err := json.Marshal(presence)
	if err != nil || len(payload) > 8*1024 {
		return false, 0, ErrInvalidRecord
	}
	now := time.Now()
	result, err := upsertPresenceScript.Run(ctx, store.client.Universal(), []string{memberKey, indexKey}, now.UnixMilli(), payload, ttl.Milliseconds(), now.Add(ttl).UnixMilli(), presence.ConnectionID).Int64Slice()
	if err != nil || len(result) != 2 {
		return false, 0, ErrUnavailable
	}
	return result[0] == 1, int(result[1]), nil
}

func (store *RealtimePresenceStore) Delete(ctx context.Context, presence RealtimePresence) (int, error) {
	memberKey, indexKey, err := store.input(presence, time.Millisecond)
	if err != nil {
		return 0, err
	}
	result, err := deletePresenceScript.Run(ctx, store.client.Universal(), []string{memberKey, indexKey}, presence.ConnectionID, time.Now().UnixMilli()).Int()
	if err != nil {
		return 0, ErrUnavailable
	}
	return result, nil
}

func (store *RealtimePresenceStore) input(presence RealtimePresence, ttl time.Duration) (string, string, error) {
	if store == nil || store.client == nil || !validKeyPart(presence.AppID) || !validKeyPart(presence.Channel) || !validKeyPart(presence.ClientID) || !validKeyPart(presence.ConnectionID) || ttl <= 0 {
		return "", "", ErrInvalidRecord
	}
	memberKey, err := store.keys.RealtimePresence(presence.AppID, presence.Channel, presence.ConnectionID)
	if err != nil {
		return "", "", err
	}
	indexKey, err := store.keys.RealtimePresenceIndex(presence.AppID, presence.Channel)
	return memberKey, indexKey, err
}
