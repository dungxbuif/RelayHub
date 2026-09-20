package redisstate

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type ConnectionOwner struct {
	InstanceID string `json:"instance_id"`
	Generation uint64 `json:"generation"`
}

type Instance struct {
	ID          string
	Role        string
	Generation  uint64
	StartedAt   time.Time
	HeartbeatAt time.Time
}

type OwnershipStore struct {
	client *Client
	keys   Keyspace
}

type ownerRecord struct {
	Version    int    `json:"version"`
	AppID      string `json:"app_id"`
	InstanceID string `json:"instance_id"`
	Generation string `json:"generation"`
}

type instanceRecord struct {
	Version       int    `json:"version"`
	ID            string `json:"id"`
	Role          string `json:"role"`
	Generation    string `json:"generation"`
	StartedAtUS   string `json:"started_at_us"`
	HeartbeatAtUS string `json:"heartbeat_at_us"`
}

var refreshOwnerScript = redis.NewScript(`
local value = redis.call('GET', KEYS[1])
if not value then return 0 end
local record = cjson.decode(value)
if record.app_id ~= ARGV[1] or record.instance_id ~= ARGV[2] or record.generation ~= ARGV[3] then
  return -1
end
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return 1
`)

var releaseOwnerScript = redis.NewScript(`
local value = redis.call('GET', KEYS[1])
if not value then return 0 end
local record = cjson.decode(value)
if record.app_id ~= ARGV[1] or record.instance_id ~= ARGV[2] or record.generation ~= ARGV[3] then
  return -1
end
redis.call('DEL', KEYS[1])
return 1
`)

var heartbeatScript = redis.NewScript(`
local incoming = cjson.decode(ARGV[1])
local current = redis.call('GET', KEYS[1])
if current then
  local existing = cjson.decode(current)
  if existing.generation ~= incoming.generation and existing.started_at_us >= incoming.started_at_us then
    return -1
  end
end
local server_time = redis.call('TIME')
local now_us = tonumber(server_time[1]) * 1000000 + tonumber(server_time[2])
local now_ms = math.floor(now_us / 1000)
incoming.heartbeat_at_us = string.format('%.0f', now_us)
local expires_ms = now_ms + tonumber(ARGV[2])
redis.call('SET', KEYS[1], cjson.encode(incoming), 'PX', ARGV[2])
redis.call('ZADD', KEYS[2], expires_ms, incoming.id)
return now_us
`)

var liveInstancesScript = redis.NewScript(`
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
return redis.call('ZRANGE', KEYS[1], 0, tonumber(ARGV[2]) - 1)
`)

var releaseInstanceScript = redis.NewScript(`
local value = redis.call('GET', KEYS[1])
if not value then
  redis.call('ZREM', KEYS[2], ARGV[1])
  return 0
end
local record = cjson.decode(value)
if record.id ~= ARGV[1] or record.generation ~= ARGV[2] or record.started_at_us ~= ARGV[3] then
  return -1
end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return 1
`)

func NewOwnershipStore(client *Client, keys Keyspace) *OwnershipStore {
	return &OwnershipStore{client: client, keys: keys}
}

func (o *OwnershipStore) Claim(ctx context.Context, appID, connectionID, instanceID string, generation uint64, ttl time.Duration) error {
	key, err := o.ownerInput(appID, connectionID, instanceID, generation, ttl)
	if err != nil {
		return err
	}
	record := ownerRecord{Version: 1, AppID: appID, InstanceID: instanceID, Generation: generationString(generation)}
	payload, err := json.Marshal(record)
	if err != nil {
		return ErrInvalidRecord
	}
	if err := o.client.Universal().Set(ctx, key, payload, ttl).Err(); err != nil {
		return ErrUnavailable
	}
	return nil
}

func (o *OwnershipStore) Refresh(ctx context.Context, appID, connectionID, instanceID string, generation uint64, ttl time.Duration) error {
	key, err := o.ownerInput(appID, connectionID, instanceID, generation, ttl)
	if err != nil {
		return err
	}
	result, err := refreshOwnerScript.Run(ctx, o.client.Universal(), []string{key}, appID, instanceID, generationString(generation), ttl.Milliseconds()).Int()
	return ownershipResult(result, err)
}

func (o *OwnershipStore) Owner(ctx context.Context, appID, connectionID string) (ConnectionOwner, error) {
	if o.client == nil || !validKeyPart(appID) {
		return ConnectionOwner{}, ErrInvalidRecord
	}
	key, err := o.keys.ConnectionOwner(connectionID)
	if err != nil {
		return ConnectionOwner{}, err
	}
	payload, err := o.client.Universal().Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return ConnectionOwner{}, ErrNotFound
	}
	if err != nil {
		return ConnectionOwner{}, ErrUnavailable
	}
	var record ownerRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		_ = o.client.Universal().Del(ctx, key).Err()
		return ConnectionOwner{}, ErrCorruptRecord
	}
	generation, parseErr := strconv.ParseUint(record.Generation, 10, 64)
	if parseErr != nil || record.Version != 1 || !validKeyPart(record.AppID) || !validKeyPart(record.InstanceID) || generation == 0 {
		if err := o.client.Universal().Del(ctx, key).Err(); err != nil {
			return ConnectionOwner{}, ErrUnavailable
		}
		return ConnectionOwner{}, ErrCorruptRecord
	}
	if record.AppID != appID {
		return ConnectionOwner{}, ErrNotFound
	}
	return ConnectionOwner{InstanceID: record.InstanceID, Generation: generation}, nil
}

func (o *OwnershipStore) Release(ctx context.Context, appID, connectionID, instanceID string, generation uint64) error {
	key, err := o.ownerInput(appID, connectionID, instanceID, generation, time.Millisecond)
	if err != nil {
		return err
	}
	result, err := releaseOwnerScript.Run(ctx, o.client.Universal(), []string{key}, appID, instanceID, generationString(generation)).Int()
	return ownershipResult(result, err)
}

func (o *OwnershipStore) Heartbeat(ctx context.Context, instance Instance, ttl time.Duration) error {
	if o.client == nil || !validKeyPart(instance.ID) || !validKeyPart(instance.Role) || instance.Generation == 0 || instance.StartedAt.IsZero() || ttl <= 0 {
		return ErrInvalidRecord
	}
	memberKey, err := o.keys.InstanceMember(instance.ID)
	if err != nil {
		return err
	}
	indexKey := o.keys.InstanceIndex()
	if indexKey == "" {
		return ErrInvalidKeyPart
	}
	record := instanceRecord{Version: 1, ID: instance.ID, Role: instance.Role, Generation: generationString(instance.Generation), StartedAtUS: fixedWidthUnixMicro(instance.StartedAt)}
	payload, err := json.Marshal(record)
	if err != nil {
		return ErrInvalidRecord
	}
	result, err := heartbeatScript.Run(ctx, o.client.Universal(), []string{memberKey, indexKey}, payload, ttl.Milliseconds()).Int64()
	if err != nil {
		return ErrUnavailable
	}
	if result < 0 {
		return ErrOwnershipLost
	}
	return nil
}

func (o *OwnershipStore) LiveInstances(ctx context.Context, now time.Time, limit int64) ([]Instance, error) {
	if o.client == nil || now.IsZero() || limit < 1 || limit > 1000 {
		return nil, ErrInvalidRecord
	}
	indexKey := o.keys.InstanceIndex()
	if indexKey == "" {
		return nil, ErrInvalidKeyPart
	}
	ids, err := liveInstancesScript.Run(ctx, o.client.Universal(), []string{indexKey}, now.UnixMilli(), limit).StringSlice()
	if err != nil {
		return nil, ErrUnavailable
	}
	if len(ids) == 0 {
		return []Instance{}, nil
	}
	memberKeys := make([]string, 0, len(ids))
	for _, id := range ids {
		key, err := o.keys.InstanceMember(id)
		if err != nil {
			return nil, ErrCorruptRecord
		}
		memberKeys = append(memberKeys, key)
	}
	values, err := o.client.Universal().MGet(ctx, memberKeys...).Result()
	if err != nil {
		return nil, ErrUnavailable
	}
	instances := make([]Instance, 0, len(values))
	for i, value := range values {
		if value == nil {
			_ = o.client.Universal().ZRem(ctx, indexKey, ids[i]).Err()
			continue
		}
		payload, ok := value.(string)
		if !ok {
			return nil, ErrCorruptRecord
		}
		var record instanceRecord
		if err := json.Unmarshal([]byte(payload), &record); err != nil {
			return nil, ErrCorruptRecord
		}
		generation, parseErr := strconv.ParseUint(record.Generation, 10, 64)
		startedAtUS, startedErr := strconv.ParseInt(record.StartedAtUS, 10, 64)
		heartbeatAtUS, heartbeatErr := strconv.ParseInt(record.HeartbeatAtUS, 10, 64)
		if parseErr != nil || startedErr != nil || heartbeatErr != nil || record.Version != 1 || record.ID != ids[i] || !validKeyPart(record.Role) || generation == 0 || startedAtUS <= 0 || heartbeatAtUS <= 0 {
			return nil, ErrCorruptRecord
		}
		instances = append(instances, Instance{
			ID: record.ID, Role: record.Role, Generation: generation,
			StartedAt: time.UnixMicro(startedAtUS).UTC(), HeartbeatAt: time.UnixMicro(heartbeatAtUS).UTC(),
		})
	}
	return instances, nil
}

func (o *OwnershipStore) ReleaseInstance(ctx context.Context, instance Instance) error {
	if o.client == nil || !validKeyPart(instance.ID) || instance.Generation == 0 || instance.StartedAt.IsZero() {
		return ErrInvalidRecord
	}
	memberKey, err := o.keys.InstanceMember(instance.ID)
	if err != nil {
		return err
	}
	indexKey := o.keys.InstanceIndex()
	if indexKey == "" {
		return ErrInvalidKeyPart
	}
	result, err := releaseInstanceScript.Run(ctx, o.client.Universal(), []string{memberKey, indexKey}, instance.ID, generationString(instance.Generation), fixedWidthUnixMicro(instance.StartedAt)).Int()
	return ownershipResult(result, err)
}

func (o *OwnershipStore) ownerInput(appID, connectionID, instanceID string, generation uint64, ttl time.Duration) (string, error) {
	if o.client == nil || !validKeyPart(appID) || !validKeyPart(instanceID) || generation == 0 || ttl <= 0 {
		return "", ErrInvalidRecord
	}
	return o.keys.ConnectionOwner(connectionID)
}

func ownershipResult(result int, err error) error {
	if err != nil {
		return ErrUnavailable
	}
	switch result {
	case 1:
		return nil
	case 0:
		return ErrNotFound
	default:
		return ErrOwnershipLost
	}
}

func generationString(generation uint64) string {
	return strconv.FormatUint(generation, 10)
}

func fixedWidthUnixMicro(value time.Time) string {
	raw := strconv.FormatInt(value.UnixMicro(), 10)
	if len(raw) >= 20 {
		return raw
	}
	return strings.Repeat("0", 20-len(raw)) + raw
}
