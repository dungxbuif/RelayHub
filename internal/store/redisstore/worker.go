package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/dungxbuif/RelayHub/internal/delivery"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func (c *Client) callbackStream() string        { return c.key("callback:stream") }
func (c *Client) callbackGroup() string         { return c.key("callback:workers") }
func (c *Client) callbackRetries() string       { return c.key("callback:retries") }
func (c *Client) callbackLock(id string) string { return c.key("callback:lease:") + id }

var releaseCallback = redis.NewScript(`if redis.call('GET',KEYS[1]) == ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0`)

func (c *Client) ClaimCallback(ctx context.Context, consumer string, idle, lease time.Duration) (store.CallbackClaim, error) {
	if err := ctx.Err(); err != nil {
		return store.CallbackClaim{}, err
	}
	if idle <= 0 || lease <= 0 {
		return store.CallbackClaim{}, store.ErrConflict
	}
	err := c.client.XGroupCreateMkStream(ctx, c.callbackStream(), c.callbackGroup(), "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return store.CallbackClaim{}, err
	}

	// Preserve the server cursor between bounded scans. Redis scans up to COUNT*10
	// pending entries even when none are idle; restarting at zero can starve tails.
	for scan := 0; scan < 16; scan++ {
		c.reclaimMu.Lock()
		cursor := c.reclaimCursor
		if cursor == "" {
			cursor = "0-0"
		}
		messages, next, readErr := c.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{Stream: c.callbackStream(), Group: c.callbackGroup(), Consumer: consumer, MinIdle: idle, Start: cursor, Count: 1}).Result()
		if readErr == nil {
			c.reclaimCursor = next
		}
		c.reclaimMu.Unlock()
		if readErr != nil && !errors.Is(readErr, redis.Nil) {
			return store.CallbackClaim{}, readErr
		}
		if len(messages) > 0 {
			claim, reserveErr := c.reserveCallback(ctx, messages[0], lease)
			if reserveErr == nil {
				return claim, nil
			}
			if !errors.Is(reserveErr, store.ErrNotFound) {
				return store.CallbackClaim{}, reserveErr
			}
		}
		if next == "0-0" {
			break
		}
	}
	streams, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: c.callbackGroup(), Consumer: consumer, Streams: []string{c.callbackStream(), ">"}, Count: 1, Block: -1}).Result()
	if errors.Is(err, redis.Nil) {
		return store.CallbackClaim{}, store.ErrNotFound
	}
	if err != nil {
		return store.CallbackClaim{}, err
	}
	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return store.CallbackClaim{}, store.ErrNotFound
	}
	return c.reserveCallback(ctx, streams[0].Messages[0], lease)
}
func (c *Client) reserveCallback(ctx context.Context, message redis.XMessage, lease time.Duration) (store.CallbackClaim, error) {

	id, _ := message.Values["job_id"].(string)
	generation, _ := strconv.Atoi(fmtValue(message.Values["generation"]))
	claim := store.CallbackClaim{MessageID: message.ID, JobID: id, Token: uuid.NewString(), Generation: generation, ExpiresAt: time.Now().Add(lease)}
	acquired, err := c.client.SetNX(ctx, c.callbackLock(id), claim.Token, lease).Result()
	if err != nil {
		return store.CallbackClaim{}, err
	}
	if !acquired {
		return store.CallbackClaim{}, store.ErrNotFound
	}
	reserved := false
	stale := false
	err = c.transaction(ctx, []string{c.jobKey(id), c.callbackLock(id)}, func(tx *redis.Tx) error {
		reserved = false
		stale = false
		token, err := tx.Get(ctx, c.callbackLock(id)).Result()
		if err != nil || token != claim.Token {
			return store.ErrConflict
		}
		j, err := readJSON[domain.Job](ctx, tx, c.jobKey(id))
		if errors.Is(err, store.ErrNotFound) {
			stale = true
			return nil
		}
		if err != nil {
			return err
		}
		if !j.Callback || j.CallbackGeneration != generation || j.Status == domain.JobDelivered || j.Status == domain.JobAcked || j.Status == domain.JobDeadLetter {
			stale = true
			return nil
		}
		now := time.Now().UTC()
		if j.LeaseUntil != nil && j.LeaseUntil.After(now) {
			return store.ErrNotFound
		}
		until := claim.ExpiresAt
		j.Status = domain.JobLeased
		j.LeaseUntil = &until
		j.RetryAt = nil
		j.Attempts++
		j.UpdatedAt = now
		raw, err := json.Marshal(j)
		if err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.Set(ctx, c.jobKey(id), raw, 0)
			p.ZAdd(ctx, c.queueKey(j.TargetAppID), redis.Z{Score: float64(until.UnixMilli()), Member: id})
			return nil
		})
		reserved = err == nil
		return err
	})
	if !reserved {
		_, _ = releaseCallback.Run(ctx, c.client, []string{c.callbackLock(id)}, claim.Token).Result()
		if stale {
			_ = c.AckCallback(ctx, claim)
		}
		if err == nil {
			err = store.ErrNotFound
		}
		return store.CallbackClaim{}, err
	}
	return claim, nil
}
func fmtValue(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return "0"
}
func (c *Client) LoadCallback(ctx context.Context, claim store.CallbackClaim) (store.CallbackData, error) {
	var data store.CallbackData
	if !claim.ExpiresAt.After(time.Now()) {
		return data, store.ErrConflict
	}
	token, err := c.client.Get(ctx, c.callbackLock(claim.JobID)).Result()
	if errors.Is(err, redis.Nil) {
		return data, store.ErrConflict
	}
	if err != nil {
		return data, err
	}
	if token != claim.Token {
		return data, store.ErrConflict
	}
	j, err := c.GetJob(ctx, claim.JobID)
	if err != nil {
		return data, err
	}
	if j.CallbackGeneration != claim.Generation || j.Status != domain.JobLeased {
		return data, store.ErrConflict
	}
	data.Job = j
	data.App, err = c.GetApplication(ctx, j.TargetAppID)
	if errors.Is(err, store.ErrNotFound) {
		return data, nil
	}
	if err != nil {
		return data, err
	}
	data.Secret, err = c.client.HGet(ctx, c.applicationKey(j.TargetAppID), "hmac_secret").Bytes()
	if err != nil {
		return data, err
	}
	data.Body, err = c.client.Get(ctx, c.eventKey(j.EventID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return data, nil
	}
	if err != nil {
		return data, err
	}
	if err = json.Unmarshal(data.Body, &data.Event); err != nil {
		return data, err
	}
	return data, nil
}

// StartCallback rechecks ownership and the original lease immediately before HTTP.
// Marking the lease as started makes duplicate starts with the same token fail.
func (c *Client) StartCallback(ctx context.Context, claim store.CallbackClaim, timeout time.Duration) (domain.Job, error) {
	var result domain.Job
	err := c.transaction(ctx, []string{c.jobKey(claim.JobID), c.callbackLock(claim.JobID)}, func(tx *redis.Tx) error {
		if timeout <= 0 || time.Until(claim.ExpiresAt) < timeout+store.CallbackFinishMargin {
			return store.ErrConflict
		}
		token, err := tx.Get(ctx, c.callbackLock(claim.JobID)).Result()
		if err != nil || token != claim.Token {
			return store.ErrConflict
		}
		remaining, err := tx.PTTL(ctx, c.callbackLock(claim.JobID)).Result()
		if err != nil {
			return err
		}
		if remaining < timeout+store.CallbackFinishMargin {
			return store.ErrConflict
		}
		j, err := readJSON[domain.Job](ctx, tx, c.jobKey(claim.JobID))
		if err != nil {
			return err
		}
		if !j.Callback || j.Status != domain.JobLeased || j.CallbackGeneration != claim.Generation || j.LeaseUntil == nil || !j.LeaseUntil.Equal(claim.ExpiresAt) || j.CallbackAttempts >= delivery.MaxAttempts {
			return store.ErrConflict
		}
		j.CallbackAttempts++
		j.UpdatedAt = time.Now().UTC()
		raw, err := json.Marshal(j)
		if err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.Set(ctx, c.callbackLock(j.ID), claim.Token+":started", redis.KeepTTL)
			p.Set(ctx, c.jobKey(j.ID), raw, 0)
			return nil
		})
		if err == nil {
			result = j
		}
		return err
	})
	return result, err
}
func (c *Client) FinishCallback(ctx context.Context, claim store.CallbackClaim, tr store.CallbackTransition) (domain.Job, error) {
	var result domain.Job
	err := c.transaction(ctx, []string{c.jobKey(claim.JobID), c.callbackLock(claim.JobID)}, func(tx *redis.Tx) error {
		token, err := tx.Get(ctx, c.callbackLock(claim.JobID)).Result()
		if err != nil || (token != claim.Token && token != claim.Token+":started") {
			return store.ErrConflict
		}
		j, err := readJSON[domain.Job](ctx, tx, c.jobKey(claim.JobID))
		if err != nil {
			return err
		}
		if j.Status != domain.JobLeased || j.CallbackGeneration != claim.Generation {
			return store.ErrConflict
		}
		if tr.Status != domain.JobDelivered && tr.Status != domain.JobPending && tr.Status != domain.JobDeadLetter {
			return store.ErrConflict
		}
		j.Status = tr.Status
		j.UpdatedAt = tr.Now
		j.LeaseUntil = nil
		j.RetryAt = nil
		j.LastReason = tr.Reason
		j.CallbackGeneration++
		if tr.Disable {
			j.Callback = false
		}
		if !tr.RetryAt.IsZero() {
			j.RetryAt = &tr.RetryAt
		}
		raw, err := json.Marshal(j)
		if err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
			ttl := time.Duration(0)
			if j.Status == domain.JobDelivered || j.Status == domain.JobDeadLetter {
				ttl = c.jobRetention
				p.PExpire(ctx, c.acknowledgementKey(j.TargetAppID, j.EventID), ttl)
			}
			p.Set(ctx, c.jobKey(j.ID), raw, ttl)
			p.Del(ctx, c.callbackLock(j.ID))
			p.ZRem(ctx, c.callbackRetries(), j.ID)
			if j.Status == domain.JobPending && !tr.RetryAt.IsZero() {
				p.ZAdd(ctx, c.callbackRetries(), redis.Z{Score: float64(tr.RetryAt.UnixMilli()), Member: j.ID})
				p.ZAdd(ctx, c.queueKey(j.TargetAppID), redis.Z{Score: float64(tr.RetryAt.UnixMilli()), Member: j.ID})
			} else if tr.Disable {
				p.ZAdd(ctx, c.queueKey(j.TargetAppID), redis.Z{Score: float64(tr.Now.UnixMilli()), Member: j.ID})
			} else {
				p.ZRem(ctx, c.queueKey(j.TargetAppID), j.ID)
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

// AckCallback refuses an active, untransitioned generation. A crash between
// Finish and Ack is safe: reclaim sees the newer durable generation and cleans up.
func (c *Client) AckCallback(ctx context.Context, claim store.CallbackClaim) error {
	return c.transaction(ctx, []string{c.jobKey(claim.JobID)}, func(tx *redis.Tx) error {
		j, err := readJSON[domain.Job](ctx, tx, c.jobKey(claim.JobID))
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
		if err == nil && j.Callback && j.CallbackGeneration == claim.Generation && (j.Status == domain.JobLeased || j.Status == domain.JobPending) {
			return store.ErrConflict
		}
		_, err = tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.XAck(ctx, c.callbackStream(), c.callbackGroup(), claim.MessageID)
			p.XDel(ctx, c.callbackStream(), claim.MessageID)
			return nil
		})
		return err
	})
}
func (c *Client) PromoteCallbacks(ctx context.Context, now time.Time, limit int) error {
	if limit < 1 || limit > 1000 {
		return store.ErrConflict
	}
	return c.transaction(ctx, []string{c.callbackRetries()}, func(tx *redis.Tx) error {
		ids, err := tx.ZRangeByScore(ctx, c.callbackRetries(), &redis.ZRangeBy{Min: "-inf", Max: strconv.FormatInt(now.UnixMilli(), 10), Count: int64(limit)}).Result()
		if err != nil || len(ids) == 0 {
			return err
		}
		jobs := []domain.Job{}
		deferred := map[string]time.Time{}
		for _, id := range ids {
			if err := tx.Watch(ctx, c.jobKey(id)).Err(); err != nil {
				return err
			}
			j, err := readJSON[domain.Job](ctx, tx, c.jobKey(id))
			if errors.Is(err, store.ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if j.Callback && (j.Status == domain.JobPending || j.Status == domain.JobLeased) {
				if j.Status == domain.JobLeased && j.LeaseUntil != nil && j.LeaseUntil.After(now) {
					deferred[j.ID] = *j.LeaseUntil
				} else {
					jobs = append(jobs, j)
				}
			}
		}
		_, err = tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
			for _, id := range ids {
				if until, ok := deferred[id]; ok {
					p.ZAdd(ctx, c.callbackRetries(), redis.Z{Score: float64(until.UnixMilli()), Member: id})
				} else {
					p.ZRem(ctx, c.callbackRetries(), id)
				}
			}
			for _, j := range jobs {
				p.XAdd(ctx, &redis.XAddArgs{Stream: c.callbackStream(), Values: map[string]any{"job_id": j.ID, "generation": j.CallbackGeneration}})
			}
			return nil
		})
		return err
	})
}

var _ store.CallbackStore = (*Client)(nil)
