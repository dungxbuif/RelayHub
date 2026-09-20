package redisstate

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrNotFound      = errors.New("Redis record not found")
	ErrLimitExceeded = errors.New("Redis record limit exceeded")
	ErrCorruptRecord = errors.New("corrupt Redis record")
	ErrInvalidRecord = errors.New("invalid Redis record")
	ErrOwnershipLost = errors.New("Redis ownership lost")
)

type AdminSession struct {
	ID            string    `json:"id"`
	CSRFHash      string    `json:"csrf_hash"`
	IssuedAt      time.Time `json:"issued_at"`
	LastSeenAt    time.Time `json:"last_seen_at"`
	IdleExpiresAt time.Time `json:"idle_expires_at"`
	ExpiresAt     time.Time `json:"expires_at"`
}

type SessionStore interface {
	Put(context.Context, AdminSession) error
	Get(context.Context, string) (AdminSession, error)
	ValidateAndTouch(context.Context, string, time.Time, time.Duration) (AdminSession, error)
	Delete(context.Context, string) error
}

type RedisSessionStore struct {
	client *Client
	keys   Keyspace
}

type sessionRecord struct {
	Version         int    `json:"version"`
	ID              string `json:"id"`
	CSRFHash        string `json:"csrf_hash"`
	IssuedAtUS      string `json:"issued_at_us"`
	LastSeenAtUS    string `json:"last_seen_at_us"`
	IdleExpiresAtUS string `json:"idle_expires_at_us"`
	ExpiresAtUS     string `json:"expires_at_us"`
}

var validateAndTouchSessionScript = redis.NewScript(`
local value = redis.call('GET', KEYS[1])
if not value then return {0, ''} end
local decoded, record = pcall(cjson.decode, value)
if not decoded or record.version ~= 2 or record.id ~= ARGV[2] or
   not record.csrf_hash or record.csrf_hash == '' or
   not record.issued_at_us or not record.last_seen_at_us or
   not record.idle_expires_at_us or not record.expires_at_us then
  redis.call('DEL', KEYS[1])
  return {-2, ''}
end
local issued_us = tonumber(record.issued_at_us)
local last_seen_us = tonumber(record.last_seen_at_us)
local idle_expires_us = tonumber(record.idle_expires_at_us)
local expires_us = tonumber(record.expires_at_us)
if not issued_us or not last_seen_us or not idle_expires_us or not expires_us or
   issued_us <= 0 or last_seen_us < issued_us or idle_expires_us <= last_seen_us or
   expires_us < idle_expires_us then
  redis.call('DEL', KEYS[1])
  return {-2, ''}
end
local server_time = redis.call('TIME')
local now_us = tonumber(server_time[1]) * 1000000 + tonumber(server_time[2])
if now_us >= idle_expires_us or now_us >= expires_us then
  redis.call('DEL', KEYS[1])
  return {0, ''}
end
local next_idle_us = math.min(now_us + tonumber(ARGV[1]) * 1000, expires_us)
record.last_seen_at_us = string.format('%.0f', now_us)
record.idle_expires_at_us = string.format('%.0f', next_idle_us)
local ttl_ms = math.max(1, math.ceil((next_idle_us - now_us) / 1000))
local updated = cjson.encode(record)
redis.call('SET', KEYS[1], updated, 'PX', ttl_ms)
return {1, updated}
`)

func NewRedisSessionStore(client *Client, keys Keyspace) *RedisSessionStore {
	return &RedisSessionStore{client: client, keys: keys}
}

func (s *RedisSessionStore) Put(ctx context.Context, session AdminSession) error {
	key, err := s.keys.AdminSession(session.ID)
	if err != nil {
		return err
	}
	if s.client == nil || !validAdminSession(session) {
		return ErrInvalidRecord
	}
	expiresAt := session.IdleExpiresAt
	if session.ExpiresAt.Before(expiresAt) {
		expiresAt = session.ExpiresAt
	}
	ttl := time.Until(expiresAt)
	if ttl <= 0 {
		return ErrInvalidRecord
	}
	payload, err := json.Marshal(encodeAdminSession(session))
	if err != nil {
		return ErrInvalidRecord
	}
	if err := s.client.Universal().Set(ctx, key, payload, ttl).Err(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return ErrUnavailable
	}
	return nil
}

func (s *RedisSessionStore) Get(ctx context.Context, id string) (AdminSession, error) {
	key, err := s.keys.AdminSession(id)
	if err != nil {
		return AdminSession{}, err
	}
	if s.client == nil {
		return AdminSession{}, ErrUnavailable
	}
	payload, err := s.client.Universal().Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return AdminSession{}, ErrNotFound
	}
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return AdminSession{}, ctxErr
		}
		return AdminSession{}, ErrUnavailable
	}
	session, err := decodeAdminSession(payload, id)
	if err != nil {
		if deleteErr := s.client.Universal().Del(ctx, key).Err(); deleteErr != nil {
			return AdminSession{}, ErrUnavailable
		}
		return AdminSession{}, err
	}
	return session, nil
}

func (s *RedisSessionStore) ValidateAndTouch(ctx context.Context, id string, now time.Time, idleTTL time.Duration) (AdminSession, error) {
	key, err := s.keys.AdminSession(id)
	if err != nil {
		return AdminSession{}, err
	}
	if s.client == nil || now.IsZero() || idleTTL <= 0 || idleTTL > 24*time.Hour {
		return AdminSession{}, ErrInvalidRecord
	}
	result, err := validateAndTouchSessionScript.Run(ctx, s.client.Universal(), []string{key}, idleTTL.Milliseconds(), id).Slice()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return AdminSession{}, ctxErr
		}
		return AdminSession{}, ErrUnavailable
	}
	if len(result) != 2 {
		return AdminSession{}, ErrUnavailable
	}
	status, err := parseScriptInt(result[0])
	if err != nil {
		return AdminSession{}, ErrUnavailable
	}
	switch status {
	case 0:
		return AdminSession{}, ErrNotFound
	case -2:
		return AdminSession{}, ErrCorruptRecord
	case 1:
		payload, ok := result[1].(string)
		if !ok {
			return AdminSession{}, ErrUnavailable
		}
		session, decodeErr := decodeAdminSession([]byte(payload), id)
		if decodeErr != nil {
			return AdminSession{}, ErrCorruptRecord
		}
		return session, nil
	default:
		return AdminSession{}, ErrUnavailable
	}
}

func (s *RedisSessionStore) Delete(ctx context.Context, id string) error {
	key, err := s.keys.AdminSession(id)
	if err != nil {
		return err
	}
	if s.client == nil {
		return ErrUnavailable
	}
	if err := s.client.Universal().Del(ctx, key).Err(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return ErrUnavailable
	}
	return nil
}

func validAdminSession(session AdminSession) bool {
	return validKeyPart(session.ID) && session.CSRFHash != "" && !session.IssuedAt.IsZero() &&
		!session.LastSeenAt.Before(session.IssuedAt) && session.IdleExpiresAt.After(session.LastSeenAt) &&
		!session.ExpiresAt.Before(session.IdleExpiresAt)
}

func encodeAdminSession(session AdminSession) sessionRecord {
	return sessionRecord{
		Version: 2, ID: session.ID, CSRFHash: session.CSRFHash,
		IssuedAtUS: fixedWidthUnixMicro(session.IssuedAt), LastSeenAtUS: fixedWidthUnixMicro(session.LastSeenAt),
		IdleExpiresAtUS: fixedWidthUnixMicro(session.IdleExpiresAt), ExpiresAtUS: fixedWidthUnixMicro(session.ExpiresAt),
	}
}

func decodeAdminSession(payload []byte, id string) (AdminSession, error) {
	var record sessionRecord
	if err := json.Unmarshal(payload, &record); err != nil || record.Version != 2 || record.ID != id || record.CSRFHash == "" {
		return AdminSession{}, ErrCorruptRecord
	}
	issuedAt, issuedErr := parseSessionTime(record.IssuedAtUS)
	lastSeenAt, seenErr := parseSessionTime(record.LastSeenAtUS)
	idleExpiresAt, idleErr := parseSessionTime(record.IdleExpiresAtUS)
	expiresAt, expiresErr := parseSessionTime(record.ExpiresAtUS)
	session := AdminSession{ID: record.ID, CSRFHash: record.CSRFHash, IssuedAt: issuedAt, LastSeenAt: lastSeenAt, IdleExpiresAt: idleExpiresAt, ExpiresAt: expiresAt}
	if issuedErr != nil || seenErr != nil || idleErr != nil || expiresErr != nil || !validAdminSession(session) {
		return AdminSession{}, ErrCorruptRecord
	}
	return session, nil
}

func parseSessionTime(raw string) (time.Time, error) {
	microseconds, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || microseconds <= 0 {
		return time.Time{}, ErrCorruptRecord
	}
	return time.UnixMicro(microseconds).UTC(), nil
}
