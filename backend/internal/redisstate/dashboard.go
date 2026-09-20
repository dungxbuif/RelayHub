package redisstate

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type MetricField string

const (
	MetricRequestTotal       MetricField = "request_total"
	MetricStatus2xx          MetricField = "status_2xx"
	MetricStatus3xx          MetricField = "status_3xx"
	MetricStatus4xx          MetricField = "status_4xx"
	MetricStatus5xx          MetricField = "status_5xx"
	MetricEventPublished     MetricField = "event_published"
	MetricEventReplayed      MetricField = "event_replayed"
	MetricEventRejected      MetricField = "event_rejected"
	MetricEventStoreError    MetricField = "event_store_error"
	MetricDeliveryDelivered  MetricField = "delivery_delivered"
	MetricDeliveryPending    MetricField = "delivery_pending"
	MetricDeliveryDeadLetter MetricField = "delivery_dead_letter"
	MetricDeliveryStoreError MetricField = "delivery_store_error"
	MetricNATSDisconnected   MetricField = "nats_disconnected"
	MetricNATSReconnected    MetricField = "nats_reconnected"
)

var metricFields = map[MetricField]struct{}{
	MetricRequestTotal: {}, MetricStatus2xx: {}, MetricStatus3xx: {}, MetricStatus4xx: {}, MetricStatus5xx: {},
	MetricEventPublished: {}, MetricEventReplayed: {}, MetricEventRejected: {}, MetricEventStoreError: {},
	MetricDeliveryDelivered: {}, MetricDeliveryPending: {}, MetricDeliveryDeadLetter: {}, MetricDeliveryStoreError: {},
	MetricNATSDisconnected: {}, MetricNATSReconnected: {},
}

type MetricBucket struct {
	At     time.Time             `json:"at"`
	Values map[MetricField]int64 `json:"values"`
}

type DashboardInstanceState struct {
	InstanceID    string    `json:"instance_id"`
	Connections   int64     `json:"connections"`
	NATSConnected bool      `json:"nats_connected"`
	NATSChangedAt time.Time `json:"nats_changed_at"`
	HeartbeatAt   time.Time `json:"heartbeat_at"`
}

type DashboardMetricsStore struct {
	client *Client
	keys   Keyspace
}

type AsyncDashboardRecorder struct {
	store     *DashboardMetricsStore
	queue     chan MetricField
	cancel    context.CancelFunc
	done      chan struct{}
	onFailure func()
}

func NewAsyncDashboardRecorder(parent context.Context, store *DashboardMetricsStore, capacity int, onFailure func()) (*AsyncDashboardRecorder, error) {
	if parent == nil || store == nil || store.client == nil || capacity < 1 || capacity > 65_536 {
		return nil, ErrInvalidRecord
	}
	ctx, cancel := context.WithCancel(parent)
	recorder := &AsyncDashboardRecorder{store: store, queue: make(chan MetricField, capacity), cancel: cancel, done: make(chan struct{}), onFailure: onFailure}
	go recorder.run(ctx)
	return recorder, nil
}

func (recorder *AsyncDashboardRecorder) Record(raw string) {
	if recorder == nil {
		return
	}
	field := MetricField(raw)
	if _, ok := metricFields[field]; !ok {
		return
	}
	select {
	case recorder.queue <- field:
	default:
		if recorder.onFailure != nil {
			recorder.onFailure()
		}
	}
}

func (recorder *AsyncDashboardRecorder) Close(ctx context.Context) error {
	if recorder == nil {
		return nil
	}
	recorder.cancel()
	select {
	case <-recorder.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (recorder *AsyncDashboardRecorder) run(ctx context.Context) {
	defer close(recorder.done)
	for {
		select {
		case <-ctx.Done():
			return
		case field := <-recorder.queue:
			recordCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			if err := recorder.store.Record(recordCtx, field, 1); err != nil && recorder.onFailure != nil {
				recorder.onFailure()
			}
			cancel()
		}
	}
}

var recordMetricScript = redis.NewScript(`
local server_time = redis.call('TIME')
local minute = math.floor(tonumber(server_time[1]) / 60)
if minute ~= tonumber(ARGV[1]) then return {0, minute} end
redis.call('HINCRBY', KEYS[1], ARGV[2], ARGV[3])
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return {1, minute}
`)

var heartbeatDashboardScript = redis.NewScript(`
local server_time = redis.call('TIME')
local now_us = tonumber(server_time[1]) * 1000000 + tonumber(server_time[2])
local now_ms = math.floor(now_us / 1000)
local record = cjson.decode(ARGV[1])
record.heartbeat_at_us = string.format('%.0f', now_us)
redis.call('SET', KEYS[1], cjson.encode(record), 'PX', ARGV[2])
redis.call('ZADD', KEYS[2], now_ms + tonumber(ARGV[2]), record.instance_id)
redis.call('PEXPIRE', KEYS[2], ARGV[3])
return now_us
`)

var liveDashboardInstancesScript = redis.NewScript(`
local server_time = redis.call('TIME')
local now_ms = tonumber(server_time[1]) * 1000 + math.floor(tonumber(server_time[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now_ms)
return redis.call('ZRANGE', KEYS[1], 0, tonumber(ARGV[1]) - 1)
`)

type dashboardInstanceRecord struct {
	Version         int    `json:"version"`
	InstanceID      string `json:"instance_id"`
	Connections     string `json:"connections"`
	NATSConnected   bool   `json:"nats_connected"`
	NATSChangedAtUS string `json:"nats_changed_at_us"`
	HeartbeatAtUS   string `json:"heartbeat_at_us,omitempty"`
}

func NewDashboardMetricsStore(client *Client, keys Keyspace) *DashboardMetricsStore {
	return &DashboardMetricsStore{client: client, keys: keys}
}

func (store *DashboardMetricsStore) Record(ctx context.Context, field MetricField, delta int64) error {
	if store == nil || store.client == nil || delta < 1 || delta > 1_000_000 {
		return ErrInvalidRecord
	}
	if _, ok := metricFields[field]; !ok {
		return ErrInvalidRecord
	}
	minute := time.Now().Unix() / 60
	for attempt := 0; attempt < 2; attempt++ {
		key, err := store.keys.DashboardBucket(minute)
		if err != nil {
			return err
		}
		result, err := recordMetricScript.Run(ctx, store.client.Universal(), []string{key}, minute, string(field), delta, (25 * time.Hour).Milliseconds()).Slice()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return ErrUnavailable
		}
		if len(result) != 2 {
			return ErrUnavailable
		}
		accepted, parseErr := parseScriptInt(result[0])
		serverMinute, minuteErr := parseScriptInt(result[1])
		if parseErr != nil || minuteErr != nil || serverMinute <= 0 {
			return ErrUnavailable
		}
		if accepted == 1 {
			return nil
		}
		minute = serverMinute
	}
	return ErrUnavailable
}

func (store *DashboardMetricsStore) Read(ctx context.Context, end time.Time, window, step time.Duration) ([]MetricBucket, error) {
	if store == nil || store.client == nil || end.IsZero() || !validDashboardWindow(window, step) {
		return nil, ErrInvalidRecord
	}
	endMinute := end.UTC().Unix() / 60
	startMinute := end.Add(-window).UTC().Unix()/60 + 1
	keys := make([]string, 0, endMinute-startMinute+1)
	for minute := startMinute; minute <= endMinute; minute++ {
		key, err := store.keys.DashboardBucket(minute)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	pipe := store.client.Universal().Pipeline()
	commands := make([]*redis.MapStringStringCmd, len(keys))
	for i, key := range keys {
		commands[i] = pipe.HGetAll(ctx, key)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, ErrUnavailable
	}
	stepMinutes := int64(step / time.Minute)
	bucketCount := int64(window / step)
	result := make([]MetricBucket, bucketCount)
	firstAt := time.Unix((endMinute-int64(window/time.Minute)+1)*60, 0).UTC()
	for i := range result {
		result[i] = MetricBucket{At: firstAt.Add(time.Duration(i) * step), Values: map[MetricField]int64{}}
	}
	for index, command := range commands {
		values, err := command.Result()
		if err != nil {
			return nil, ErrUnavailable
		}
		target := int64(index) / stepMinutes
		if target >= int64(len(result)) {
			target = int64(len(result) - 1)
		}
		for rawField, rawValue := range values {
			field := MetricField(rawField)
			if _, ok := metricFields[field]; !ok {
				continue
			}
			value, err := strconv.ParseInt(rawValue, 10, 64)
			if err != nil || value < 0 {
				return nil, ErrCorruptRecord
			}
			result[target].Values[field] += value
		}
	}
	return result, nil
}

func validDashboardWindow(window, step time.Duration) bool {
	allowed := map[time.Duration][]time.Duration{
		5 * time.Minute: {time.Minute}, 15 * time.Minute: {time.Minute}, time.Hour: {time.Minute, 5 * time.Minute},
		6 * time.Hour: {5 * time.Minute, 15 * time.Minute}, 24 * time.Hour: {15 * time.Minute, time.Hour},
	}
	for _, candidate := range allowed[window] {
		if step == candidate {
			return true
		}
	}
	return false
}

func (store *DashboardMetricsStore) HeartbeatInstance(ctx context.Context, state DashboardInstanceState, ttl time.Duration) error {
	if store == nil || store.client == nil || !validKeyPart(state.InstanceID) || state.Connections < 0 || state.NATSChangedAt.IsZero() || ttl < 5*time.Second || ttl > 5*time.Minute {
		return ErrInvalidRecord
	}
	key, err := store.keys.DashboardInstance(state.InstanceID)
	if err != nil {
		return err
	}
	index := store.keys.DashboardInstanceIndex()
	if index == "" {
		return ErrInvalidKeyPart
	}
	record := dashboardInstanceRecord{Version: 1, InstanceID: state.InstanceID, Connections: strconv.FormatInt(state.Connections, 10), NATSConnected: state.NATSConnected, NATSChangedAtUS: strconv.FormatInt(state.NATSChangedAt.UTC().UnixMicro(), 10)}
	raw, err := json.Marshal(record)
	if err != nil {
		return ErrInvalidRecord
	}
	if _, err := heartbeatDashboardScript.Run(ctx, store.client.Universal(), []string{key, index}, raw, ttl.Milliseconds(), (25 * time.Hour).Milliseconds()).Result(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return ErrUnavailable
	}
	return nil
}

func (store *DashboardMetricsStore) LiveInstances(ctx context.Context, limit int64) ([]DashboardInstanceState, error) {
	if store == nil || store.client == nil || limit < 1 || limit > 1000 {
		return nil, ErrInvalidRecord
	}
	index := store.keys.DashboardInstanceIndex()
	ids, err := liveDashboardInstancesScript.Run(ctx, store.client.Universal(), []string{index}, limit).StringSlice()
	if err != nil {
		return nil, ErrUnavailable
	}
	if len(ids) == 0 {
		return []DashboardInstanceState{}, nil
	}
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i], err = store.keys.DashboardInstance(id)
		if err != nil {
			return nil, ErrCorruptRecord
		}
	}
	values, err := store.client.Universal().MGet(ctx, keys...).Result()
	if err != nil {
		return nil, ErrUnavailable
	}
	result := make([]DashboardInstanceState, 0, len(values))
	for i, value := range values {
		if value == nil {
			_ = store.client.Universal().ZRem(ctx, index, ids[i]).Err()
			continue
		}
		raw, ok := value.(string)
		if !ok {
			return nil, ErrCorruptRecord
		}
		var record dashboardInstanceRecord
		if json.Unmarshal([]byte(raw), &record) != nil || record.Version != 1 || record.InstanceID != ids[i] {
			return nil, ErrCorruptRecord
		}
		connections, cErr := strconv.ParseInt(record.Connections, 10, 64)
		changed, nErr := strconv.ParseInt(record.NATSChangedAtUS, 10, 64)
		heartbeat, hErr := strconv.ParseInt(record.HeartbeatAtUS, 10, 64)
		if cErr != nil || nErr != nil || hErr != nil || connections < 0 || changed <= 0 || heartbeat <= 0 {
			return nil, ErrCorruptRecord
		}
		result = append(result, DashboardInstanceState{InstanceID: record.InstanceID, Connections: connections, NATSConnected: record.NATSConnected, NATSChangedAt: time.UnixMicro(changed).UTC(), HeartbeatAt: time.UnixMicro(heartbeat).UTC()})
	}
	return result, nil
}
