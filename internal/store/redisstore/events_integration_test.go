//go:build integration

package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/redis/go-redis/v9"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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
	// Set an independently short expiry so a renewal to the configured hour is
	// unambiguous without timing-sensitive millisecond comparisons.
	if err := c.client.PExpire(ctx, jobKey(job.ID), 10*time.Second).Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.AckEvent(ctx, "a", p.Event.ID, now.Add(time.Second), ret.Job); err != nil {
		t.Fatal(err)
	}
	if ttl := c.client.PTTL(ctx, jobKey(job.ID)).Val(); ttl <= 0 || ttl > 10*time.Second {
		t.Fatalf("repeated ack renewed TTL: %v", ttl)
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

func TestEventConcurrentAckAndLeaseLeavesTerminalState(t *testing.T) {
	c := integrationRedisClient(t)
	seedTargets(t, c)
	ctx := context.Background()
	now := time.Now().UTC()
	ret := store.EventRetention{Event: time.Hour, Job: time.Hour, Idempotency: time.Hour}
	for iteration := 0; iteration < 20; iteration++ {
		p, _, err := publishFixture(t, c, fmt.Sprint("ackrace", iteration), now, ret)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			if err := c.AckEvent(ctx, "a", p.Event.ID, now, ret.Job); err != nil {
				t.Error(err)
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			items, err := c.LeaseJobs(ctx, "a", 1, now, time.Minute)
			if err != nil {
				t.Error(err)
			}
			for _, item := range items {
				if item.Job.Status != domain.JobLeased || item.Job.Attempts != 1 {
					t.Error("invalid raced lease")
				}
			}
		}()
		close(start)
		wg.Wait()
		j, err := c.GetJob(ctx, p.Jobs[0].ID)
		if err != nil || j.Status != domain.JobAcked || j.LeaseUntil != nil || j.Attempts > 1 {
			t.Fatalf("nonterminal race outcome: %#v %v", j, err)
		}
		items, err := c.LeaseJobs(ctx, "a", 1, now.Add(2*time.Minute), time.Minute)
		if err != nil || len(items) != 0 {
			t.Fatalf("acked job redelivered: %v %v", items, err)
		}
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

func TestLeaseLongPollBoundsInFlightRedisIO(t *testing.T) {
	for _, mode := range []string{"cancel", "wait expires"} {
		t.Run(mode, func(t *testing.T) {
			client := integrationRedisClient(t)
			flushIntegrationRedis(t, client)
			options := *client.client.Options()
			options.ContextTimeoutEnabled = true // The control connection is test infrastructure.
			control := redis.NewClient(&options)
			defer control.Close()
			probe := &leaseIOProbe{firstPoll: make(chan struct{}), readStarted: make(chan struct{}, 1)}
			rawURL := os.Getenv("RELAYHUB_TEST_REDIS_URL")
			if rawURL == "" {
				rawURL = "redis://" + options.Addr + "/" + fmt.Sprint(options.DB)
			}
			fresh, err := NewClient(rawURL)
			if err != nil {
				t.Fatal(err)
			}
			defer fresh.Close()
			fresh.client.AddHook(probe)
			if err := fresh.Ping(context.Background()); err != nil {
				t.Fatal(err)
			}
			events := service.NewEventService(fresh, fresh, service.EventOptions{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			wait := 30 * time.Second
			if mode == "wait expires" {
				wait = 400 * time.Millisecond
			}
			type result struct {
				items []service.LeasedEvent
				err   error
			}
			done := make(chan result, 1)
			go func() { items, err := events.Lease(ctx, "idle", 20, wait); done <- result{items, err} }()
			select {
			case <-probe.firstPoll:
			case <-time.After(2 * time.Second):
				t.Fatal("first Redis poll did not complete")
			}
			// The first poll has completed. Pause responses to the next real Redis call.
			pauseCtx, pauseCancel := context.WithTimeout(context.Background(), time.Second)
			err = control.Do(pauseCtx, "CLIENT", "PAUSE", 2000, "ALL").Err()
			pauseCancel()
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				cancel()
				// Let the finite pause expire before returning a reused test Redis server.
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cleanupCancel()
				if err := control.Ping(cleanupCtx).Err(); err != nil {
					t.Errorf("Redis did not resume after finite pause: %v", err)
				}
			}()
			probe.armed.Store(true)
			select {
			case <-probe.readStarted:
			case <-time.After(time.Second):
				t.Fatal("did not observe socket Read while Redis was paused")
			}
			started := time.Now()
			if mode == "cancel" {
				cancel()
			}
			select {
			case got := <-done:
				if mode == "cancel" && !errors.Is(got.err, context.Canceled) {
					t.Fatalf("cancel error=%v", got.err)
				}
				if mode == "wait expires" && (got.err != nil || len(got.items) != 0) {
					t.Fatalf("wait result=%v error=%v", got.items, got.err)
				}
				t.Logf("%s returned %s after observing stalled socket read", mode, time.Since(started))
			case <-time.After(time.Second):
				cancel()
				// Drain the test goroutine even on RED; the finite server pause is 2s.
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Error("lease goroutine did not stop after Redis resumed")
				}
				t.Fatal("in-flight Redis read exceeded the 1s cancellation/wait bound")
			}
		})
	}
}

// This probe observes the real socket Read; it never fakes, delays or supplies replies.
type leaseIOProbe struct {
	armed       atomic.Bool
	once        sync.Once
	firstPoll   chan struct{}
	readStarted chan struct{}
}

func (p *leaseIOProbe) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := next(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &leaseProbeConn{Conn: conn, probe: p}, nil
	}
}
func (p *leaseIOProbe) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if cmd.Name() == "unwatch" && err == nil {
			p.once.Do(func() { close(p.firstPoll) })
		}
		return err
	}
}
func (p *leaseIOProbe) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

type leaseProbeConn struct {
	net.Conn
	probe *leaseIOProbe
}

func (c *leaseProbeConn) Read(data []byte) (int, error) {
	if c.probe.armed.Load() {
		select {
		case c.probe.readStarted <- struct{}{}:
		default:
		}
	}
	return c.Conn.Read(data)
}
