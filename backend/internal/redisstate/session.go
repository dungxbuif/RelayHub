package redisstate

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrNotFound      = errors.New("Redis record not found")
	ErrCorruptRecord = errors.New("corrupt Redis record")
	ErrInvalidRecord = errors.New("invalid Redis record")
	ErrOwnershipLost = errors.New("Redis ownership lost")
)

type AdminSession struct {
	ID        string    `json:"id"`
	CSRFHash  string    `json:"csrf_hash"`
	IssuedAt  time.Time `json:"issued_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

type SessionStore interface {
	Put(context.Context, AdminSession) error
	Get(context.Context, string) (AdminSession, error)
	Delete(context.Context, string) error
}

type RedisSessionStore struct {
	client *Client
	keys   Keyspace
}

type sessionRecord struct {
	Version int `json:"version"`
	AdminSession
}

func NewRedisSessionStore(client *Client, keys Keyspace) *RedisSessionStore {
	return &RedisSessionStore{client: client, keys: keys}
}

func (s *RedisSessionStore) Put(ctx context.Context, session AdminSession) error {
	key, err := s.keys.AdminSession(session.ID)
	if err != nil {
		return err
	}
	ttl := time.Until(session.ExpiresAt)
	if s.client == nil || session.CSRFHash == "" || session.IssuedAt.IsZero() || ttl <= 0 || !session.ExpiresAt.After(session.IssuedAt) {
		return ErrInvalidRecord
	}
	payload, err := json.Marshal(sessionRecord{Version: 1, AdminSession: session})
	if err != nil {
		return ErrInvalidRecord
	}
	if err := s.client.Universal().Set(ctx, key, payload, ttl).Err(); err != nil {
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
		return AdminSession{}, ErrUnavailable
	}
	var record sessionRecord
	if err := json.Unmarshal(payload, &record); err != nil || record.Version != 1 || record.ID != id || record.CSRFHash == "" || record.IssuedAt.IsZero() || !record.ExpiresAt.After(record.IssuedAt) {
		if err := s.client.Universal().Del(ctx, key).Err(); err != nil {
			return AdminSession{}, ErrUnavailable
		}
		return AdminSession{}, ErrCorruptRecord
	}
	return record.AdminSession, nil
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
		return ErrUnavailable
	}
	return nil
}
