package redisstate

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"regexp"
	"time"

	"github.com/redis/go-redis/v9"
)

const maxRealtimeHistoryPayload = 64 * 1024

var redisStreamIDPattern = regexp.MustCompile(`^[0-9]+-[0-9]+$`)

type RealtimeHistoryEntry struct {
	Cursor  string          `json:"cursor"`
	Payload json.RawMessage `json:"payload"`
}

type RealtimeHistoryStore struct {
	client *Client
	keys   Keyspace
	maxLen int64
	ttl    time.Duration
}

var appendRealtimeHistoryScript = redis.NewScript(`
local id = redis.call('XADD', KEYS[1], 'MAXLEN', '=', ARGV[1], '*', 'payload', ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return id
`)

func NewRealtimeHistoryStore(client *Client, keys Keyspace, maxLen int64, ttl time.Duration) *RealtimeHistoryStore {
	return &RealtimeHistoryStore{client: client, keys: keys, maxLen: maxLen, ttl: ttl}
}

func (store *RealtimeHistoryStore) Append(ctx context.Context, appID, channel string, payload json.RawMessage) (string, error) {
	key, err := store.key(appID, channel)
	if err != nil || !json.Valid(payload) || len(payload) == 0 || len(payload) > maxRealtimeHistoryPayload {
		return "", ErrInvalidRecord
	}
	id, err := appendRealtimeHistoryScript.Run(ctx, store.client.Universal(), []string{key}, store.maxLen, []byte(payload), store.ttl.Milliseconds()).Text()
	if err != nil || !redisStreamIDPattern.MatchString(id) {
		return "", ErrUnavailable
	}
	return encodeHistoryCursor(id), nil
}

func (store *RealtimeHistoryStore) Read(ctx context.Context, appID, channel, cursor string, limit int64) ([]RealtimeHistoryEntry, string, error) {
	key, err := store.key(appID, channel)
	if err != nil || limit < 1 || limit > 100 {
		return nil, "", ErrInvalidRecord
	}
	start := "+"
	if cursor != "" {
		id, decodeErr := decodeHistoryCursor(cursor)
		if decodeErr != nil {
			return nil, "", ErrInvalidRecord
		}
		start = "(" + id
	}
	messages, err := store.client.Universal().XRevRangeN(ctx, key, start, "-", limit+1).Result()
	if err != nil {
		return nil, "", ErrUnavailable
	}
	hasMore := int64(len(messages)) > limit
	if hasMore {
		messages = messages[:limit]
	}
	entries := make([]RealtimeHistoryEntry, 0, len(messages))
	for _, message := range messages {
		value, ok := message.Values["payload"]
		if !ok {
			return nil, "", ErrCorruptRecord
		}
		var payload []byte
		switch typed := value.(type) {
		case string:
			payload = []byte(typed)
		case []byte:
			payload = append([]byte(nil), typed...)
		default:
			return nil, "", ErrCorruptRecord
		}
		if !json.Valid(payload) || len(payload) > maxRealtimeHistoryPayload || !redisStreamIDPattern.MatchString(message.ID) {
			return nil, "", ErrCorruptRecord
		}
		entries = append(entries, RealtimeHistoryEntry{Cursor: encodeHistoryCursor(message.ID), Payload: payload})
	}
	next := ""
	if hasMore && len(messages) > 0 {
		next = encodeHistoryCursor(messages[len(messages)-1].ID)
	}
	// Redis returns newest first; clients receive chronological pages.
	for left, right := 0, len(entries)-1; left < right; left, right = left+1, right-1 {
		entries[left], entries[right] = entries[right], entries[left]
	}
	return entries, next, nil
}

func (store *RealtimeHistoryStore) key(appID, channel string) (string, error) {
	if store == nil || store.client == nil || store.maxLen < 1 || store.maxLen > 10000 || store.ttl <= 0 {
		return "", ErrInvalidRecord
	}
	return store.keys.RealtimeHistory(appID, channel)
}

func encodeHistoryCursor(id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(id))
}

func decodeHistoryCursor(cursor string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil || !redisStreamIDPattern.Match(raw) {
		return "", ErrInvalidRecord
	}
	return string(raw), nil
}
