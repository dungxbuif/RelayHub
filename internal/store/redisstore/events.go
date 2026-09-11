package redisstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/redis/go-redis/v9"
)

func eventKey(id string) string      { return "relayhub:event:" + id }
func jobKey(id string) string        { return "relayhub:job:" + id }
func queueKey(target string) string  { return "relayhub:queue:" + target }
func streamKey(target string) string { return "relayhub:stream:" + target }
func scopedKey(prefix, owner, key string) string {
	sum := sha256.Sum256([]byte(owner + "\x00" + key))
	return prefix + hex.EncodeToString(sum[:])
}
func acknowledgementKey(target, event string) string {
	return scopedKey("relayhub:ack:", target, event)
}
func idempotencyKey(source, key string) string {
	return scopedKey("relayhub:idempotency:", source, key)
}

// Redis WATCH/MULTI keeps the complete publication and its indexes atomic while
// preserving arbitrary JSON data exactly (including large integers and {}).
func (c *Client) PublishEvent(ctx context.Context, p store.Publication, key string, ret store.EventRetention) (store.Publication, bool, error) {
	result := store.Publication{}
	replay := false
	idem := c.idempotencyKey(p.Event.SourceAppID, key)
	keys := []string{idem, c.eventKey(p.Event.ID)}
	for _, j := range p.Jobs {
		keys = append(keys, c.applicationKey(j.TargetAppID), c.jobKey(j.ID))
	}
	err := c.transaction(ctx, keys, func(tx *redis.Tx) error {
		raw, err := tx.Get(ctx, idem).Bytes()
		if err == nil {
			if err := json.Unmarshal(raw, &result); err != nil {
				return err
			}
			replay = true
			return nil
		}
		if !errors.Is(err, redis.Nil) {
			return err
		}
		for i, j := range p.Jobs {
			enabled, err := tx.HGet(ctx, c.applicationKey(j.TargetAppID), "enabled").Result()
			if errors.Is(err, redis.Nil) || (err == nil && enabled != "1") {
				return store.ErrInvalidTarget
			}
			if err != nil {
				return err
			}
			fields, err := tx.HMGet(ctx, c.applicationKey(j.TargetAppID), "delivery_mode", "callback_url").Result()
			if err != nil {
				return err
			}
			mode, _ := fields[0].(string)
			url, _ := fields[1].(string)
			p.Jobs[i].Callback = url != "" && (mode == string(domain.DeliveryCallback) || mode == string(domain.DeliveryAll))
		}
		collision, err := tx.Exists(ctx, c.eventKey(p.Event.ID)).Result()
		if err != nil {
			return err
		}
		if collision != 0 {
			return store.ErrConflict
		}
		for _, j := range p.Jobs {
			exists, err := tx.Exists(ctx, c.jobKey(j.ID)).Result()
			if err != nil {
				return err
			}
			if exists != 0 {
				return store.ErrConflict
			}
		}
		eventJSON, err := json.Marshal(p.Event)
		if err != nil {
			return err
		}
		publicationJSON, err := json.Marshal(p)
		if err != nil {
			return err
		}
		jobJSON := make([][]byte, len(p.Jobs))
		for i, j := range p.Jobs {
			jobJSON[i], err = json.Marshal(j)
			if err != nil {
				return err
			}
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, c.eventKey(p.Event.ID), eventJSON, ret.Event)
			pipe.Set(ctx, idem, publicationJSON, ret.Idempotency)
			for i, j := range p.Jobs {
				pipe.Set(ctx, c.jobKey(j.ID), jobJSON[i], 0)
				if j.Callback {
					pipe.XAdd(ctx, &redis.XAddArgs{Stream: c.callbackStream(), Values: map[string]any{"job_id": j.ID, "generation": j.CallbackGeneration}})
				}
				pipe.Set(ctx, c.acknowledgementKey(j.TargetAppID, j.EventID), j.ID, 0)
				pipe.ZAdd(ctx, c.queueKey(j.TargetAppID), redis.Z{Score: float64(j.CreatedAt.UnixMilli()), Member: j.ID})
				pipe.XAdd(ctx, &redis.XAddArgs{Stream: c.streamKey(j.TargetAppID), Values: map[string]any{"event_id": j.EventID, "job_id": j.ID}})
				cutoff := p.Event.CreatedAt.Add(-ret.Event).UnixMilli()
				if cutoff < 0 {
					cutoff = 0
				}
				pipe.XTrimMinID(ctx, c.streamKey(j.TargetAppID), strconv.FormatInt(cutoff, 10)+"-0")
				pipe.PExpire(ctx, c.streamKey(j.TargetAppID), ret.Event)
			}
			return nil
		})
		if err == nil {
			result = p
			replay = false
		}
		return err
	})
	return result, replay, err
}
func (c *Client) GetEvent(ctx context.Context, id string) (domain.Event, error) {
	return readJSON[domain.Event](ctx, c.client, c.eventKey(id))
}
func (c *Client) GetJob(ctx context.Context, id string) (domain.Job, error) {
	return readJSON[domain.Job](ctx, c.client, c.jobKey(id))
}

type redisGetter interface {
	Get(context.Context, string) *redis.StringCmd
}

func readJSON[T any](ctx context.Context, r redisGetter, key string) (T, error) {
	var value T
	raw, err := r.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return value, store.ErrNotFound
	}
	if err != nil {
		return value, err
	}
	err = json.Unmarshal(raw, &value)
	return value, err
}

func (c *Client) LeaseJobs(ctx context.Context, target string, limit int, now time.Time, lease time.Duration) ([]store.LeasedEvent, error) {
	result := []store.LeasedEvent{}
	err := c.transaction(ctx, []string{c.queueKey(target)}, func(tx *redis.Tx) error {
		result = []store.LeasedEvent{}
		ids, err := tx.ZRangeByScore(ctx, c.queueKey(target), &redis.ZRangeBy{Min: "-inf", Max: strconv.FormatInt(now.UnixMilli(), 10), Offset: 0, Count: int64(limit)}).Result()
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		jobKeys := make([]string, 0, len(ids))
		for _, id := range ids {
			jobKeys = append(jobKeys, c.jobKey(id))
		}
		if err := tx.Watch(ctx, jobKeys...).Err(); err != nil {
			return err
		}
		updates := []domain.Job{}
		remove := []string{}
		for _, id := range ids {
			j, err := readJSON[domain.Job](ctx, tx, c.jobKey(id))
			if errors.Is(err, store.ErrNotFound) {
				remove = append(remove, id)
				continue
			}
			if err != nil {
				return err
			}
			if j.TargetAppID != target || (j.Status != domain.JobPending && j.Status != domain.JobLeased) {
				remove = append(remove, id)
				continue
			}
			if j.Status == domain.JobLeased && j.LeaseUntil != nil && j.LeaseUntil.After(now) {
				continue
			}
			if err := tx.Watch(ctx, c.eventKey(j.EventID)).Err(); err != nil {
				return err
			}
			event, err := readJSON[domain.Event](ctx, tx, c.eventKey(j.EventID))
			if errors.Is(err, store.ErrNotFound) {
				j.Status = domain.JobDeadLetter
				j.LeaseUntil = nil
				j.UpdatedAt = now
				updates = append(updates, j)
				remove = append(remove, id)
				continue
			}
			if err != nil {
				return err
			}
			end := now.Add(lease)
			j.LeaseUntil = &end
			j.Status = domain.JobLeased
			j.Attempts++
			j.UpdatedAt = now
			updates = append(updates, j)
			result = append(result, store.LeasedEvent{Event: event, Job: j})
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, id := range remove {
				pipe.ZRem(ctx, c.queueKey(target), id)
			}
			for _, j := range updates {
				raw, err := json.Marshal(j)
				if err != nil {
					return err
				}
				ttl := time.Duration(0)
				if j.Status == domain.JobDeadLetter {
					ttl = c.jobRetention
					pipe.PExpire(ctx, c.acknowledgementKey(j.TargetAppID, j.EventID), ttl)
				}
				pipe.Set(ctx, c.jobKey(j.ID), raw, ttl)
				if j.Status == domain.JobLeased {
					pipe.ZAdd(ctx, c.queueKey(target), redis.Z{Score: float64(j.LeaseUntil.UnixMilli()), Member: j.ID})
				}
			}
			return nil
		})
		return err
	})
	return result, err
}
func (c *Client) AckEvent(ctx context.Context, target, event string, now time.Time, retention time.Duration) error {
	id, err := c.client.Get(ctx, c.acknowledgementKey(target, event)).Result()
	if errors.Is(err, redis.Nil) {
		return store.ErrNotFound
	}
	if err != nil {
		return err
	}
	_, err = c.transition(ctx, id, domain.JobAcked, now, retention, target, event)
	return err
}
func (c *Client) TransitionJob(ctx context.Context, id string, status domain.JobStatus, now time.Time, retention time.Duration) (domain.Job, error) {
	return c.transition(ctx, id, status, now, retention, "", "")
}
func (c *Client) transition(ctx context.Context, id string, status domain.JobStatus, now time.Time, retention time.Duration, target, event string) (domain.Job, error) {
	result := domain.Job{}
	err := c.transaction(ctx, []string{c.jobKey(id)}, func(tx *redis.Tx) error {
		j, err := readJSON[domain.Job](ctx, tx, c.jobKey(id))
		if err != nil {
			return err
		}
		if target != "" && (j.TargetAppID != target || j.EventID != event) {
			return store.ErrNotFound
		}
		if !j.Status.CanTransition(status) {
			return store.ErrConflict
		}
		if j.Status == status {
			result = j
			return nil
		} // Idempotent terminal requests do not renew TTL.
		if status == domain.JobPending {
			if err := tx.Watch(ctx, c.eventKey(j.EventID)).Err(); err != nil {
				return err
			}
			if _, err := readJSON[domain.Event](ctx, tx, c.eventKey(j.EventID)); err != nil {
				return err
			}
		}
		j.CallbackGeneration++
		j.RetryAt = nil
		if status == domain.JobPending {
			j.Attempts = 0
			j.LastReason = ""
		}
		j.Status = status
		j.UpdatedAt = now
		j.LeaseUntil = nil
		raw, err := json.Marshal(j)
		if err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			ttl := time.Duration(0)
			if status == domain.JobAcked || status == domain.JobDeadLetter {
				ttl = retention
			}
			pipe.Set(ctx, c.jobKey(id), raw, ttl)
			pipe.ZRem(ctx, c.callbackRetries(), id)
			if ttl > 0 {
				pipe.PExpire(ctx, c.acknowledgementKey(j.TargetAppID, j.EventID), ttl)
			} else {
				pipe.Persist(ctx, c.acknowledgementKey(j.TargetAppID, j.EventID))
			}
			if status == domain.JobPending {
				if j.Callback {
					pipe.XAdd(ctx, &redis.XAddArgs{Stream: c.callbackStream(), Values: map[string]any{"job_id": j.ID, "generation": j.CallbackGeneration}})
				}
				pipe.ZAdd(ctx, c.queueKey(j.TargetAppID), redis.Z{Score: float64(now.UnixMilli()), Member: id})
			} else {
				pipe.ZRem(ctx, c.queueKey(j.TargetAppID), id)
			}
			return nil
		})
		if err == nil {
			result = j
		}
		return err
	})
	return result, err
}

// Retry optimistic conflicts; each attempt is cancellable and has bounded work.
func (c *Client) transaction(ctx context.Context, keys []string, fn func(*redis.Tx) error) error {
	for attempt := 0; attempt < 128; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := c.client.Watch(ctx, fn, keys...)
		if !errors.Is(err, redis.TxFailedErr) {
			return err
		}
	}
	return store.ErrConflict
}

var _ store.EventStore = (*Client)(nil)

func (c *Client) FindPublication(ctx context.Context, source, key string) (store.Publication, error) {
	return readJSON[store.Publication](ctx, c.client, c.idempotencyKey(source, key))
}
