//go:build integration

package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	"strings"
	"sync"
	"testing"
	"time"
)

func publishFixture(t *testing.T, c *Client, id string, now time.Time, ret store.EventRetention) (store.Publication, bool, error) {
	t.Helper()
	e := domain.Event{ID: "evt_" + id, Type: "test", SourceAppID: "source", TargetAppIDs: []string{"a", "b"}, Data: json.RawMessage(`{"precise":9007199254740993,"empty":{},"array":[]}`), CreatedAt: now}
	jobs := []domain.Job{}
	for _, target := range e.TargetAppIDs {
		jobs = append(jobs, domain.Job{ID: "job_" + id + target, EventID: e.ID, SourceAppID: e.SourceAppID, TargetAppID: target, Status: domain.JobPending, CreatedAt: now, UpdatedAt: now})
	}
	return c.PublishEvent(context.Background(), store.Publication{Event: e, Jobs: jobs}, "same-key", ret)
}
func seedTargets(t *testing.T, c *Client) {
	t.Helper()
	for _, id := range []string{"a", "b"} {
		if err := c.CreateApplication(context.Background(), domain.App{ID: id, Name: id, Enabled: true, DeliveryMode: domain.DeliveryQueue}, store.AppCredential{AppID: id, APIKeyHash: id}); err != nil {
			t.Fatal(err)
		}
	}
}
func TestEventConcurrentPublication(t *testing.T) {
	c := integrationRedisClient(t)
	flushIntegrationRedis(t, c)
	seedTargets(t, c)
	ctx := context.Background()
	now := time.Now().UTC()
	ret := store.EventRetention{Event: 7 * 24 * time.Hour, Job: 7 * 24 * time.Hour, Idempotency: 24 * time.Hour}
	var wg sync.WaitGroup
	results := make(chan store.Publication, 16)
	replays := make(chan bool, 16)
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, replay, err := publishFixture(t, c, fmt.Sprint(i), now, ret)
			if err != nil {
				t.Error(err)
				return
			}
			results <- p
			replays <- replay
		}()
	}
	wg.Wait()
	close(results)
	close(replays)
	id := ""
	n := 0
	for p := range results {
		n++
		if id == "" {
			id = p.Event.ID
		}
		if p.Event.ID != id || len(p.Jobs) != 2 {
			t.Fatalf("duplicate result %#v", p)
		}
		if !strings.Contains(string(p.Event.Data), "9007199254740993") {
			t.Fatal("numeric precision lost")
		}
	}
	first := 0
	for replay := range replays {
		if !replay {
			first++
		}
	}
	if n != 16 || first != 1 {
		t.Fatalf("results=%d fresh=%d", n, first)
	}
	for pattern, want := range map[string]int{"relayhub:event:*": 1, "relayhub:job:*": 2, "relayhub:idempotency:*": 1} {
		keys, err := c.client.Keys(ctx, pattern).Result()
		if err != nil || len(keys) != want {
			t.Fatalf("%s %v %v", pattern, keys, err)
		}
	}
	for _, target := range []string{"a", "b"} {
		if n := c.client.ZCard(ctx, queueKey(target)).Val(); n != 1 {
			t.Fatalf("queue %s %d", target, n)
		}
		entries, err := c.client.XRange(ctx, streamKey(target), "-", "+").Result()
		if err != nil || len(entries) != 1 || entries[0].Values["event_id"] != id {
			t.Fatalf("stream %v %v", entries, err)
		}
	}
	keys, _ := c.client.Keys(ctx, "relayhub:idempotency:*").Result()
	for key, want := range map[string]time.Duration{eventKey(id): ret.Event, keys[0]: ret.Idempotency} {
		ttl := c.client.PTTL(ctx, key).Val()
		if ttl <= want-time.Minute || ttl > want {
			t.Fatalf("TTL %s %v", key, ttl)
		}
	}
}
func TestEventLeaseAckControlsAndIsolation(t *testing.T) {
	c := integrationRedisClient(t)
	flushIntegrationRedis(t, c)
	seedTargets(t, c)
	ctx := context.Background()
	now := time.Now().UTC()
	ret := store.EventRetention{Event: time.Hour, Job: time.Hour, Idempotency: time.Hour}
	p, _, err := publishFixture(t, c, "lease", now, ret)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	got := make(chan []store.LeasedEvent, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			items, err := c.LeaseJobs(ctx, "a", 20, now, time.Minute)
			if err != nil {
				t.Error(err)
			}
			got <- items
		}()
	}
	wg.Wait()
	close(got)
	n := 0
	for items := range got {
		n += len(items)
	}
	if n != 1 {
		t.Fatalf("competing leases %d", n)
	}
	items, err := c.LeaseJobs(ctx, "stranger", 20, now, time.Minute)
	if err != nil || len(items) != 0 {
		t.Fatal("cross target lease")
	}
	if err := c.AckEvent(ctx, "stranger", p.Event.ID, now, ret.Job); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cross ack %v", err)
	}
	items, err = c.LeaseJobs(ctx, "a", 20, now.Add(61*time.Second), time.Minute)
	if err != nil || len(items) != 1 || items[0].Job.Attempts != 2 || items[0].Job.LeaseUntil == nil {
		t.Fatalf("expired lease %v %v", items, err)
	}
	for range 2 {
		if err := c.AckEvent(ctx, "a", p.Event.ID, now.Add(62*time.Second), ret.Job); err != nil {
			t.Fatal(err)
		}
	}
	job, err := c.GetJob(ctx, p.Jobs[0].ID)
	if err != nil || job.Status != domain.JobAcked || job.LeaseUntil != nil {
		t.Fatalf("acked %v %v", job, err)
	}
	ttl := c.client.PTTL(ctx, jobKey(job.ID)).Val()
	if ttl < ret.Job-time.Minute || ttl > ret.Job {
		t.Fatalf("terminal TTL %v", ttl)
	}
	if _, err := c.TransitionJob(ctx, job.ID, domain.JobPending, now, ret.Job); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("illegal transition %v", err)
	}
	dead, err := c.TransitionJob(ctx, p.Jobs[1].ID, domain.JobDeadLetter, now, ret.Job)
	if err != nil || dead.Status != domain.JobDeadLetter {
		t.Fatal(err)
	}
	if err := c.AckEvent(ctx, "b", p.Event.ID, now, ret.Job); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("dead ack %v", err)
	}
	pending, err := c.TransitionJob(ctx, dead.ID, domain.JobPending, now, ret.Job)
	if err != nil || pending.Status != domain.JobPending {
		t.Fatal(err)
	}
	if ttl := c.client.PTTL(ctx, jobKey(dead.ID)).Val(); ttl != -1 {
		t.Fatalf("requeued retains terminal TTL %v", ttl)
	}
	items, err = c.LeaseJobs(ctx, "b", 20, now, time.Minute)
	if err != nil || len(items) != 1 {
		t.Fatalf("requeue lease %v %v", items, err)
	}
}
func TestEventExpiryAndAtomicTargetValidation(t *testing.T) {
	c := integrationRedisClient(t)
	flushIntegrationRedis(t, c)
	seedTargets(t, c)
	ctx := context.Background()
	now := time.Now().UTC()
	ret := store.EventRetention{Event: time.Hour, Job: time.Hour, Idempotency: time.Hour}
	if _, err := c.DisableApplication(ctx, "b", now); err != nil {
		t.Fatal(err)
	}
	if _, _, err := publishFixture(t, c, "disabled", now, ret); !errors.Is(err, store.ErrInvalidTarget) {
		t.Fatalf("disabled target %v", err)
	}
	keys, _ := c.client.Keys(ctx, "relayhub:event:*").Result()
	if len(keys) != 0 {
		t.Fatal("partial publish")
	}
	if err := c.client.HSet(ctx, applicationKey("b"), "enabled", "1").Err(); err != nil {
		t.Fatal(err)
	}
	p, _, err := publishFixture(t, c, "expiry", now, ret)
	if err != nil {
		t.Fatal(err)
	}
	idemKeys, _ := c.client.Keys(ctx, "relayhub:idempotency:*").Result()
	if err := c.client.PExpire(ctx, idemKeys[0], time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	fresh, replay, err := publishFixture(t, c, "after", now, ret)
	if err != nil || replay || fresh.Event.ID == p.Event.ID {
		t.Fatalf("expired idem %v %v", replay, err)
	}
	if err := c.client.PExpire(ctx, eventKey(p.Event.ID), time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	if _, err := c.GetEvent(ctx, p.Event.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("event ttl %v", err)
	}
	items, err := c.LeaseJobs(ctx, "a", 20, now, time.Minute)
	if err != nil || len(items) != 1 || items[0].Event.ID != fresh.Event.ID {
		t.Fatalf("expired event queue %v %v", items, err)
	}
	orphan, err := c.GetJob(ctx, p.Jobs[0].ID)
	if err != nil || orphan.Status != domain.JobDeadLetter {
		t.Fatalf("orphan cleanup %v %v", orphan, err)
	}
	if _, err := c.TransitionJob(ctx, orphan.ID, domain.JobPending, now, ret.Job); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("requeue expired event %v", err)
	}
}
