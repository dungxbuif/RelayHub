package redisstate

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type RealtimeConnection struct {
	AppID        string    `json:"app_id"`
	ClientID     string    `json:"client_id"`
	ConnectionID string    `json:"connection_id"`
	InstanceID   string    `json:"instance_id"`
	Generation   uint64    `json:"generation"`
	Protocol     string    `json:"protocol"`
	Channels     []string  `json:"channels"`
	ConnectedAt  time.Time `json:"connected_at"`
	LastSeenAt   time.Time `json:"last_seen_at"`
}

type RealtimeConnectionStore struct {
	client *Client
	keys   Keyspace
}

type realtimeConnectionRecord struct {
	Version      int      `json:"version"`
	AppID        string   `json:"app_id"`
	ClientID     string   `json:"client_id"`
	ConnectionID string   `json:"connection_id"`
	InstanceID   string   `json:"instance_id"`
	Generation   string   `json:"generation"`
	Protocol     string   `json:"protocol"`
	Channels     []string `json:"channels"`
	ConnectedUS  string   `json:"connected_at_us"`
	LastSeenUS   string   `json:"last_seen_at_us"`
}

var updateRealtimeConnectionScript = redis.NewScript(`
local value = redis.call('GET', KEYS[1])
if not value then return 0 end
local current = cjson.decode(value)
if current.app_id ~= ARGV[1] or current.instance_id ~= ARGV[2] or current.generation ~= ARGV[3] then return -1 end
local incoming = cjson.decode(ARGV[4])
redis.call('SET', KEYS[1], ARGV[4], 'PX', ARGV[5])
redis.call('ZADD', KEYS[2], ARGV[6], incoming.connection_id)
return 1
`)

var deleteRealtimeConnectionScript = redis.NewScript(`
local value = redis.call('GET', KEYS[1])
if not value then redis.call('ZREM', KEYS[2], ARGV[4]); return 0 end
local current = cjson.decode(value)
if current.app_id ~= ARGV[1] or current.instance_id ~= ARGV[2] or current.generation ~= ARGV[3] then return -1 end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[4])
return 1
`)

func NewRealtimeConnectionStore(client *Client, keys Keyspace) *RealtimeConnectionStore {
	return &RealtimeConnectionStore{client: client, keys: keys}
}

func (store *RealtimeConnectionStore) Put(ctx context.Context, connection RealtimeConnection, ttl time.Duration) error {
	record, memberKey, indexKey, err := store.record(connection, ttl)
	if err != nil {
		return err
	}
	payload, _ := json.Marshal(record)
	expires := time.Now().Add(ttl).UnixMilli()
	pipe := store.client.Universal().TxPipeline()
	pipe.Set(ctx, memberKey, payload, ttl)
	pipe.ZAdd(ctx, indexKey, redis.Z{Score: float64(expires), Member: connection.ConnectionID})
	if _, err := pipe.Exec(ctx); err != nil {
		return ErrUnavailable
	}
	return nil
}

func (store *RealtimeConnectionStore) Refresh(ctx context.Context, connection RealtimeConnection, ttl time.Duration) error {
	return store.update(ctx, connection, connection.Channels, ttl)
}

func (store *RealtimeConnectionStore) UpdateChannels(ctx context.Context, connection RealtimeConnection, channels []string, ttl time.Duration) error {
	return store.update(ctx, connection, channels, ttl)
}

func (store *RealtimeConnectionStore) update(ctx context.Context, connection RealtimeConnection, channels []string, ttl time.Duration) error {
	connection.Channels = normalizedChannels(channels)
	connection.LastSeenAt = time.Now().UTC()
	record, memberKey, indexKey, err := store.record(connection, ttl)
	if err != nil {
		return err
	}
	payload, _ := json.Marshal(record)
	result, err := updateRealtimeConnectionScript.Run(ctx, store.client.Universal(), []string{memberKey, indexKey}, connection.AppID, connection.InstanceID, generationString(connection.Generation), payload, ttl.Milliseconds(), time.Now().Add(ttl).UnixMilli()).Int()
	return ownershipResult(result, err)
}

func (store *RealtimeConnectionStore) Delete(ctx context.Context, connection RealtimeConnection) error {
	_, memberKey, indexKey, err := store.record(connection, time.Millisecond)
	if err != nil {
		return err
	}
	result, err := deleteRealtimeConnectionScript.Run(ctx, store.client.Universal(), []string{memberKey, indexKey}, connection.AppID, connection.InstanceID, generationString(connection.Generation), connection.ConnectionID).Int()
	return ownershipResult(result, err)
}

func (store *RealtimeConnectionStore) List(ctx context.Context, appID string, now time.Time, limit int64) ([]RealtimeConnection, error) {
	if store == nil || store.client == nil || now.IsZero() || limit < 1 || limit > 500 {
		return nil, ErrInvalidRecord
	}
	indexKey, err := store.keys.RealtimeConnectionIndex(appID)
	if err != nil {
		return nil, err
	}
	client := store.client.Universal()
	if err := client.ZRemRangeByScore(ctx, indexKey, "-inf", strconv.FormatInt(now.UnixMilli(), 10)).Err(); err != nil {
		return nil, ErrUnavailable
	}
	ids, err := client.ZRange(ctx, indexKey, 0, limit-1).Result()
	if err != nil {
		return nil, ErrUnavailable
	}
	result := make([]RealtimeConnection, 0, len(ids))
	for _, id := range ids {
		memberKey, keyErr := store.keys.RealtimeConnection(appID, id)
		if keyErr != nil {
			return nil, ErrCorruptRecord
		}
		payload, getErr := client.Get(ctx, memberKey).Bytes()
		if errors.Is(getErr, redis.Nil) {
			_ = client.ZRem(ctx, indexKey, id).Err()
			continue
		}
		if getErr != nil {
			return nil, ErrUnavailable
		}
		connection, decodeErr := decodeRealtimeConnection(payload)
		if decodeErr != nil || connection.AppID != appID || connection.ConnectionID != id {
			return nil, ErrCorruptRecord
		}
		result = append(result, connection)
	}
	return result, nil
}

func (store *RealtimeConnectionStore) Get(ctx context.Context, appID, connectionID string) (RealtimeConnection, error) {
	if store == nil || store.client == nil {
		return RealtimeConnection{}, ErrInvalidRecord
	}
	memberKey, err := store.keys.RealtimeConnection(appID, connectionID)
	if err != nil {
		return RealtimeConnection{}, err
	}
	payload, err := store.client.Universal().Get(ctx, memberKey).Bytes()
	if errors.Is(err, redis.Nil) {
		return RealtimeConnection{}, ErrNotFound
	}
	if err != nil {
		return RealtimeConnection{}, ErrUnavailable
	}
	connection, err := decodeRealtimeConnection(payload)
	if err != nil || connection.AppID != appID || connection.ConnectionID != connectionID {
		return RealtimeConnection{}, ErrNotFound
	}
	return connection, nil
}

func (store *RealtimeConnectionStore) record(connection RealtimeConnection, ttl time.Duration) (realtimeConnectionRecord, string, string, error) {
	if store == nil || store.client == nil || !validKeyPart(connection.AppID) || !validKeyPart(connection.ClientID) || !validKeyPart(connection.ConnectionID) || !validKeyPart(connection.InstanceID) || connection.Generation == 0 || connection.Protocol == "" || connection.ConnectedAt.IsZero() || ttl <= 0 || len(connection.Channels) > 100 {
		return realtimeConnectionRecord{}, "", "", ErrInvalidRecord
	}
	memberKey, err := store.keys.RealtimeConnection(connection.AppID, connection.ConnectionID)
	if err != nil {
		return realtimeConnectionRecord{}, "", "", err
	}
	indexKey, err := store.keys.RealtimeConnectionIndex(connection.AppID)
	if err != nil {
		return realtimeConnectionRecord{}, "", "", err
	}
	lastSeen := connection.LastSeenAt
	if lastSeen.IsZero() {
		lastSeen = connection.ConnectedAt
	}
	record := realtimeConnectionRecord{Version: 1, AppID: connection.AppID, ClientID: connection.ClientID, ConnectionID: connection.ConnectionID, InstanceID: connection.InstanceID, Generation: generationString(connection.Generation), Protocol: connection.Protocol, Channels: normalizedChannels(connection.Channels), ConnectedUS: fixedWidthUnixMicro(connection.ConnectedAt), LastSeenUS: fixedWidthUnixMicro(lastSeen)}
	return record, memberKey, indexKey, nil
}

func decodeRealtimeConnection(payload []byte) (RealtimeConnection, error) {
	var record realtimeConnectionRecord
	if json.Unmarshal(payload, &record) != nil || record.Version != 1 {
		return RealtimeConnection{}, ErrCorruptRecord
	}
	generation, generationErr := strconv.ParseUint(record.Generation, 10, 64)
	connected, connectedErr := strconv.ParseInt(record.ConnectedUS, 10, 64)
	lastSeen, lastSeenErr := strconv.ParseInt(record.LastSeenUS, 10, 64)
	if generationErr != nil || connectedErr != nil || lastSeenErr != nil || generation == 0 || !validKeyPart(record.AppID) || !validKeyPart(record.ClientID) || !validKeyPart(record.ConnectionID) || !validKeyPart(record.InstanceID) || len(record.Channels) > 100 {
		return RealtimeConnection{}, ErrCorruptRecord
	}
	return RealtimeConnection{AppID: record.AppID, ClientID: record.ClientID, ConnectionID: record.ConnectionID, InstanceID: record.InstanceID, Generation: generation, Protocol: record.Protocol, Channels: normalizedChannels(record.Channels), ConnectedAt: time.UnixMicro(connected).UTC(), LastSeenAt: time.UnixMicro(lastSeen).UTC()}, nil
}

func normalizedChannels(channels []string) []string {
	result := append([]string(nil), channels...)
	sort.Strings(result)
	return result
}
