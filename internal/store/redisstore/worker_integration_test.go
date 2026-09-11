//go:build integration

package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/delivery"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/dungxbuif/RelayHub/internal/worker"
	"github.com/redis/go-redis/v9"
)

func callbackFixture(t *testing.T, c *Client, url string) domain.Job {
	t.Helper()
	ctx := context.Background()
	if err := c.CreateApplication(ctx, domain.App{ID: "target", Name: "target", Enabled: true, DeliveryMode: domain.DeliveryCallback, CallbackURL: &url}, store.AppCredential{AppID: "target", APIKeyHash: "hash", HMACSecret: []byte("target-secret")}); err != nil {
		t.Fatal(err)
	}
	s := service.NewEventService(c, c, service.EventOptions{})
	_, jobs, _, err := s.Publish(ctx, "source", service.PublishEvent{Type: "test", TargetAppIDs: []string{"target"}, Data: []byte(`{"n":9007199254740993}`)}, "key")
	if err != nil {
		t.Fatal(err)
	}
	return jobs[0]
}
func claim(t *testing.T, c *Client, consumer string) store.CallbackClaim {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	v, err := c.ClaimCallback(ctx, consumer, 10*time.Millisecond, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func TestCallbackRedisLifecycle(t *testing.T) {
	c := integrationRedisClient(t)
	flushIntegrationRedis(t, c)
	ctx := context.Background()
	job := callbackFixture(t, c, "https://receiver.example/events")
	first := claim(t, c, "one")
	if first.JobID != job.ID {
		t.Fatal("wrong claim")
	}
	data, err := c.LoadCallback(ctx, first)
	if err != nil || data.Job.Attempts != 1 || string(data.Secret) != "target-secret" {
		t.Fatalf("load err %v attempts %d", err, data.Job.Attempts)
	}
	persisted, _ := c.client.Get(ctx, c.eventKey(job.EventID)).Bytes()
	if string(data.Body) != string(persisted) {
		t.Fatal("persisted envelope changed")
	}
	if err := c.AckCallback(ctx, first); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("ack before transition %v", err)
	}
	time.Sleep(15 * time.Millisecond)
	if _, err := c.ClaimCallback(ctx, "two", 10*time.Millisecond, 50*time.Millisecond); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("active delivery claim: %v", err)
	}
	time.Sleep(65 * time.Millisecond)
	reclaimed := claim(t, c, "two")
	if reclaimed.MessageID != first.MessageID || reclaimed.Token == first.Token {
		t.Fatal("abandoned claim not fenced")
	}
	tr := store.CallbackTransition{Status: domain.JobPending, Now: time.Now().UTC(), RetryAt: time.Now().Add(time.Hour), Reason: "http_transient"}
	if _, err := c.FinishCallback(ctx, first, tr); !errors.Is(err, store.ErrConflict) {
		t.Fatal("old owner persisted")
	}
	retry, err := c.FinishCallback(ctx, reclaimed, tr)
	if err != nil || retry.Status != domain.JobPending || retry.RetryAt == nil {
		t.Fatalf("retry %v %+v", err, retry)
	}
	pending, _ := c.client.XPending(ctx, c.callbackStream(), c.callbackGroup()).Result()
	if pending.Count != 1 {
		t.Fatal("transition acknowledged early")
	}
	// Simulate crash after persistence, before XACK: stale generation cannot redeliver.
	time.Sleep(15 * time.Millisecond)
	if _, err := c.ClaimCallback(ctx, "three", 10*time.Millisecond, 50*time.Millisecond); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("stale stream redelivery %v", err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.PromoteCallbacks(ctx, tr.RetryAt.Add(time.Second), 10); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if n, _ := c.client.XLen(ctx, c.callbackStream()).Result(); n != 1 {
		t.Fatalf("retry promoted %d times", n)
	}
	next := claim(t, c, "four")
	done, err := c.FinishCallback(ctx, next, store.CallbackTransition{Status: domain.JobDelivered, Now: time.Now(), Reason: "http_success"})
	if err != nil || done.Status != domain.JobDelivered {
		t.Fatal(err)
	}
	if err := c.AckCallback(ctx, next); err != nil {
		t.Fatal(err)
	}
	if n, _ := c.client.XLen(ctx, c.callbackStream()).Result(); n != 0 {
		t.Fatal("stream not cleaned")
	}
	if _, err := c.ClaimCallback(ctx, "five", time.Millisecond, time.Second); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("completed redelivered")
	}
}
func TestCallbackRedisPrefixIsolationAndEligibility(t *testing.T) {
	base := integrationRedisClient(t)
	flushIntegrationRedis(t, base)
	ctx := context.Background()
	a := &Client{client: base.client, prefix: "worker_a", jobRetention: time.Hour}
	b := &Client{client: base.client, prefix: "worker_b", jobRetention: time.Hour}
	callbackFixture(t, a, "https://receiver.example")
	callbackFixture(t, b, "https://receiver.example")
	ca := claim(t, a, "same")
	cb := claim(t, b, "same")
	if ca.JobID == cb.JobID {
		t.Fatal("unexpected job collision")
	}
	for _, c := range []*Client{a, b} {
		keys, _ := c.client.Keys(ctx, c.prefix+":*").Result()
		if len(keys) == 0 {
			t.Fatal("no prefixed keys")
		}
		if _, err := c.client.XInfoGroups(ctx, c.callbackStream()).Result(); err != nil {
			t.Fatal(err)
		}
	}
	for _, mode := range []domain.DeliveryMode{domain.DeliveryQueue, domain.DeliveryWebSocket, domain.DeliveryCallback, domain.DeliveryAll} {
		for _, url := range []string{"", "https://receiver.example"} {
			prefix := fmt.Sprintf("elig_%s_%d", mode, len(url))
			c := &Client{client: base.client, prefix: prefix, jobRetention: time.Hour}
			callbackFixture(t, c, "https://receiver.example")
			app, _ := c.GetApplication(ctx, "target")
			app.DeliveryMode = mode
			app.CallbackURL = &url
			c.UpdateApplication(ctx, app)
			svc := service.NewEventService(c, c, service.EventOptions{})
			_, jobs, _, err := svc.Publish(ctx, "source", service.PublishEvent{Type: "test", TargetAppIDs: []string{"target"}, Data: []byte(`{}`)}, "another")
			if err != nil {
				t.Fatal(err)
			}
			want := url != "" && (mode == domain.DeliveryCallback || mode == domain.DeliveryAll)
			n, _ := c.client.XLen(ctx, c.callbackStream()).Result()
			expected := int64(1)
			if want {
				expected++
			}
			if jobs[0].Callback != want || n != expected {
				t.Fatal("ineligible work enqueued")
			}
		}
	}
}
func TestCallbackRedisTwoWorkersRuntimeRetryAndDLQ(t *testing.T) {
	c := integrationRedisClient(t)
	for _, terminal := range []bool{false, true} {
		t.Run(fmt.Sprint(terminal), func(t *testing.T) {
			flushIntegrationRedis(t, c)
			var count, active, max atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				v := active.Add(1)
				defer active.Add(-1)
				for old := max.Load(); v > old && !max.CompareAndSwap(old, v); old = max.Load() {
				}
				attempt := count.Add(1)
				time.Sleep(30 * time.Millisecond)
				if terminal {
					w.WriteHeader(400)
				} else if attempt == 1 {
					w.WriteHeader(503)
				} else {
					w.WriteHeader(204)
				}
			}))
			defer srv.Close()
			j := callbackFixture(t, c, srv.URL)
			ctx, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup
			for range 2 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					w := worker.New(c, delivery.NewCallback(time.Second), worker.Options{Concurrency: 2, AttemptTimeout: time.Second})
					w.Run(ctx)
				}()
			}
			deadline := time.Now().Add(5 * time.Second)
			want := domain.JobDelivered
			if terminal {
				want = domain.JobDeadLetter
			}
			var got domain.Job
			for time.Now().Before(deadline) {
				got, _ = c.GetJob(context.Background(), j.ID)
				if got.Status == want {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			cancel()
			wg.Wait()
			if got.Status != want || max.Load() != 1 {
				t.Fatalf("status=%s max=%d calls=%d", got.Status, max.Load(), count.Load())
			}
			expected := int32(2)
			if terminal {
				expected = 1
			}
			if count.Load() != expected {
				t.Fatalf("calls %d", count.Load())
			}
			pending, _ := c.client.XPending(context.Background(), c.callbackStream(), c.callbackGroup()).Result()
			if pending.Count != 0 {
				t.Fatal("completed pending")
			}
		})
	}
}
func TestCallbackRedisAdminRequeueResetsGeneration(t *testing.T) {
	c := integrationRedisClient(t)
	flushIntegrationRedis(t, c)
	ctx := context.Background()
	j := callbackFixture(t, c, "https://receiver.example")
	old := claim(t, c, "old")
	if _, err := c.TransitionJob(ctx, j.ID, domain.JobDeadLetter, time.Now(), time.Hour); err != nil {
		t.Fatal(err)
	}
	next, err := c.TransitionJob(ctx, j.ID, domain.JobPending, time.Now(), time.Hour)
	if err != nil || next.Attempts != 0 || next.CallbackGeneration == 0 {
		t.Fatalf("requeue %+v %v", next, err)
	}
	if _, err := c.FinishCallback(ctx, old, store.CallbackTransition{Status: domain.JobDelivered, Now: time.Now()}); !errors.Is(err, store.ErrConflict) {
		t.Fatal("old attempt overwrote requeue")
	}
	c.client.Del(ctx, c.callbackLock(j.ID))
	time.Sleep(15 * time.Millisecond)
	_, _ = c.ClaimCallback(ctx, "cleanup", 10*time.Millisecond, time.Second)
	if n, _ := c.client.ZCard(ctx, c.callbackRetries()).Result(); n != 0 {
		t.Fatal("requeue retained schedule")
	}
}

var _ = redis.Nil

func TestCallbackWorkerBinaryMetricsAndUsage(t *testing.T) {
	c := integrationRedisClient(t)
	flushIntegrationRedis(t, c)
	binary := t.TempDir() + "/relayhub"
	build := exec.Command("go", "build", "-o", binary, "../../../cmd/relayhub")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %s %v", out, err)
	}
	if out, err := exec.Command(binary, "invalid-command").CombinedOutput(); err == nil || !strings.Contains(string(out), "usage: relayhub [api|worker]") {
		t.Fatalf("invalid command usage missing: %s", out)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer srv.Close()
	job := callbackFixture(t, c, srv.URL)
	command := exec.Command(binary, "worker")
	command.Env = append(os.Environ(), "RELAYHUB_ADMIN_TOKEN=smoke-admin", "RELAYHUB_SIGNING_SECRET=smoke-signing", "RELAYHUB_REDIS_URL=redis://"+c.client.Options().Addr+"/0", "RELAYHUB_WORKER_HTTP_ADDR="+address, "RELAYHUB_REDIS_KEY_PREFIX=relayhub")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		command.Process.Signal(os.Interrupt)
		done := make(chan error, 1)
		go func() { done <- command.Wait() }()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("worker shutdown: %v", err)
			}
		case <-time.After(3 * time.Second):
			command.Process.Kill()
			<-done
			t.Error("worker shutdown unbounded")
		}
	}()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		j, _ := c.GetJob(context.Background(), job.ID)
		if j.Status == domain.JobDelivered {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	for path, want := range map[string]int{"/healthz": 200, "/readyz": 200, "/docs": 404, "/api/v1/apps": 404} {
		resp, err := http.Get("http://" + address + path)
		if err != nil {
			t.Fatalf("worker %s unavailable: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Fatalf("worker %s status=%d", path, resp.StatusCode)
		}
	}
	response, err := http.Get("http://" + address + "/metrics")
	if err != nil {
		t.Fatalf("worker metrics unavailable: %v", err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != 200 || !strings.Contains(string(body), `relayhub_callback_outcomes_total{outcome="delivered"} 1`) {
		t.Fatal("worker durable outcome counter unavailable")
	}
}

func TestCallbackRetrySurvivesQueueLease(t *testing.T) {
	c := integrationRedisClient(t)
	flushIntegrationRedis(t, c)
	ctx := context.Background()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(503)
		} else {
			w.WriteHeader(204)
		}
	}))
	defer srv.Close()
	j := callbackFixture(t, c, srv.URL)
	first := claim(t, c, "first")
	data, err := c.LoadCallback(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	response := delivery.NewCallback(time.Second).Deliver(ctx, delivery.Request{App: data.App, Event: data.Event, Body: data.Body, Secret: data.Secret})
	now := time.Now().Add(-time.Second)
	out := delivery.Classify(response.Status, response.Headers, response.Err, 1, now)
	if _, err = c.FinishCallback(ctx, first, store.CallbackTransition{Status: domain.JobPending, Now: now, RetryAt: out.RetryAt, Reason: out.Reason}); err != nil {
		t.Fatal(err)
	}
	if err = c.AckCallback(ctx, first); err != nil {
		t.Fatal(err)
	}
	leased, err := c.LeaseJobs(ctx, "target", 1, time.Now(), 80*time.Millisecond)
	if err != nil || len(leased) != 1 {
		t.Fatalf("queue lease %v %d", err, len(leased))
	}
	if err = c.PromoteCallbacks(ctx, time.Now(), 10); err != nil {
		t.Fatal(err)
	}
	if _, err = c.client.ZScore(ctx, c.callbackRetries(), j.ID).Result(); err != nil {
		t.Fatal("callback retry removed while queue lease active")
	}
	time.Sleep(100 * time.Millisecond)
	if err = c.PromoteCallbacks(ctx, time.Now(), 10); err != nil {
		t.Fatal(err)
	}
	second := claim(t, c, "second")
	data, err = c.LoadCallback(ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	response = delivery.NewCallback(time.Second).Deliver(ctx, delivery.Request{App: data.App, Event: data.Event, Body: data.Body, Secret: data.Secret})
	if response.Status != 204 || calls.Load() != 2 {
		t.Fatal("callback failed to recover after queue expiry")
	}
	if _, err = c.FinishCallback(ctx, second, store.CallbackTransition{Status: domain.JobDelivered, Now: time.Now(), Reason: "http_success"}); err != nil {
		t.Fatal(err)
	}
}
func TestCallbackReclaimScansPastBlockedHead(t *testing.T) {
	c := integrationRedisClient(t)
	flushIntegrationRedis(t, c)
	ctx := context.Background()
	callbackFixture(t, c, "https://receiver.example")
	svc := service.NewEventService(c, c, service.EventOptions{})
	tail := ""
	for i := 0; i < 15; i++ {
		_, jobs, _, err := svc.Publish(ctx, "source", service.PublishEvent{Type: "test", TargetAppIDs: []string{"target"}, Data: []byte(`{}`)}, fmt.Sprint(i))
		if err != nil {
			t.Fatal(err)
		}
		tail = jobs[0].ID
	}
	if err := c.client.XGroupCreateMkStream(ctx, c.callbackStream(), c.callbackGroup(), "0").Err(); err != nil {
		t.Fatal(err)
	}
	streams, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: c.callbackGroup(), Consumer: "abandoned", Streams: []string{c.callbackStream(), ">"}, Count: 16, Block: -1}).Result()
	if err != nil || len(streams[0].Messages) != 16 {
		t.Fatal("seed pending failed")
	}
	time.Sleep(30 * time.Millisecond)
	args := []any{"XCLAIM", c.callbackStream(), c.callbackGroup(), "active", 0}
	for _, message := range streams[0].Messages[:15] {
		args = append(args, message.ID)
	}
	args = append(args, "IDLE", 0)
	if err = c.client.Do(ctx, args...).Err(); err != nil {
		t.Fatal(err)
	}
	claimed, err := c.ClaimCallback(ctx, "reclaimer", 20*time.Millisecond, time.Second)
	if err != nil || claimed.JobID != tail {
		t.Fatalf("eligible tail starved: job=%s error=%v", claimed.JobID, err)
	}
}

// singleClaimStore models a worker paused after loading an event but before dispatch.
type singleClaimStore struct {
	store.CallbackStore
	claim  store.CallbackClaim
	taken  atomic.Bool
	loaded chan struct{}
	resume chan struct{}
}

func (s *singleClaimStore) ClaimCallback(ctx context.Context, consumer string, idle, lease time.Duration) (store.CallbackClaim, error) {
	if s.taken.Swap(true) {
		return store.CallbackClaim{}, store.ErrNotFound
	}
	return s.claim, nil
}
func (s *singleClaimStore) LoadCallback(ctx context.Context, claim store.CallbackClaim) (store.CallbackData, error) {
	data, err := s.CallbackStore.LoadCallback(ctx, claim)
	if s.loaded != nil {
		close(s.loaded)
		select {
		case <-s.resume:
		case <-ctx.Done():
			return store.CallbackData{}, ctx.Err()
		}
	}
	return data, err
}
func TestCallbackStaleLoadedClaimCannotDispatch(t *testing.T) {
	c := integrationRedisClient(t)
	flushIntegrationRedis(t, c)
	ctx := context.Background()
	var calls atomic.Int32
	active := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(active)
		}
		<-release
		w.WriteHeader(204)
	}))
	defer srv.Close()
	defer close(release)
	j := callbackFixture(t, c, srv.URL)
	old := claim(t, c, "old")
	paused := &singleClaimStore{CallbackStore: c, claim: old, loaded: make(chan struct{}), resume: make(chan struct{})}
	oldCtx, oldCancel := context.WithCancel(ctx)
	defer oldCancel()
	oldDone := make(chan error, 1)
	go func() {
		oldDone <- worker.New(paused, delivery.NewCallback(time.Second), worker.Options{Concurrency: 1, AttemptTimeout: time.Second}).Run(oldCtx)
	}()
	<-paused.loaded
	time.Sleep(80 * time.Millisecond)
	replacement, err := c.ClaimCallback(ctx, "replacement", 10*time.Millisecond, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	next := &singleClaimStore{CallbackStore: c, claim: replacement}
	newCtx, newCancel := context.WithCancel(ctx)
	defer newCancel()
	newDone := make(chan error, 1)
	go func() {
		newDone <- worker.New(next, delivery.NewCallback(time.Second), worker.Options{Concurrency: 1, AttemptTimeout: time.Second, ShutdownTimeout: 50 * time.Millisecond}).Run(newCtx)
	}()
	select {
	case <-active:
	case <-time.After(time.Second):
		t.Fatal("replacement did not deliver")
	}
	if _, err := c.LoadCallback(ctx, old); !errors.Is(err, store.ErrConflict) {
		t.Errorf("stale token load accepted: %v", err)
	}
	close(paused.resume)
	time.Sleep(50 * time.Millisecond)
	oldCancel()
	<-oldDone
	if calls.Load() != 1 {
		t.Errorf("old loaded claim dispatched alongside replacement: %d calls", calls.Load())
	}
	newCancel()
	<-newDone
	current, err := c.GetJob(ctx, j.ID)
	if err != nil || current.Status != domain.JobLeased {
		t.Fatalf("unfinished replacement lost: %s %v", current.Status, err)
	}
}

func TestCallbackDispatchCounterSeparateFromQueueLeases(t *testing.T) {
	c := integrationRedisClient(t)
	flushIntegrationRedis(t, c)
	ctx := context.Background()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(503) }))
	defer srv.Close()
	j := callbackFixture(t, c, srv.URL)
	// Repeated queue reservations must not advance callback_attempts.
	for i := 0; i < 9; i++ {
		items, err := c.LeaseJobs(ctx, "target", 1, time.Now(), time.Nanosecond)
		if err != nil || len(items) != 1 {
			t.Fatalf("queue reservation %d: %v", i, err)
		}
	}
	for attempt := 1; attempt <= 6; attempt++ {
		claim, err := c.ClaimCallback(ctx, "callback", time.Millisecond, 3*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		data, err := c.LoadCallback(ctx, claim)
		if err != nil {
			t.Fatal(err)
		}
		job, err := c.StartCallback(ctx, claim, time.Second)
		if err != nil || job.CallbackAttempts != attempt {
			t.Fatalf("dispatch counter %d: %+v %v", attempt, job, err)
		}
		result := delivery.NewCallback(time.Second).Deliver(ctx, delivery.Request{App: data.App, Event: data.Event, Body: data.Body, Secret: data.Secret})
		now := time.Now()
		out := delivery.Classify(result.Status, result.Headers, result.Err, job.CallbackAttempts, now)
		status := domain.JobPending
		if attempt == 6 {
			status = domain.JobDeadLetter
			if out.Kind != delivery.DeadLetter {
				t.Fatal("sixth failure not exhausted")
			}
		} else {
			want := []time.Duration{time.Second, 5 * time.Second, 15 * time.Second, 60 * time.Second, 300 * time.Second}[attempt-1]
			if out.Kind != delivery.Retry || !out.RetryAt.Equal(now.Add(want)) {
				t.Fatalf("queue changed callback delay: %+v", out)
			}
		}
		if _, err = c.FinishCallback(ctx, claim, store.CallbackTransition{Status: status, Now: now, RetryAt: out.RetryAt, Reason: out.Reason}); err != nil {
			t.Fatal(err)
		}
		if err = c.AckCallback(ctx, claim); err != nil {
			t.Fatal(err)
		}
		if attempt < 6 {
			// A queue lease races the due retry, then expires before promotion.
			items, err := c.LeaseJobs(ctx, "target", 1, out.RetryAt, time.Nanosecond)
			if err != nil || len(items) != 1 {
				t.Fatalf("mixed queue lease: %v", err)
			}
			if err = c.PromoteCallbacks(ctx, out.RetryAt.Add(time.Second), 10); err != nil {
				t.Fatal(err)
			}
			// Advance just this queue lease to expiry without sleeping through backoff.
			c.client.ZAdd(ctx, c.queueKey("target"), redis.Z{Score: float64(time.Now().UnixMilli()), Member: j.ID})
			current, _ := c.GetJob(ctx, j.ID)
			past := time.Now().Add(-time.Second)
			current.LeaseUntil = &past
			raw, _ := json.Marshal(current)
			c.client.Set(ctx, c.jobKey(j.ID), raw, 0)
		}
	}
	final, err := c.GetJob(ctx, j.ID)
	if err != nil || final.CallbackAttempts != 6 || calls.Load() != 6 || final.Attempts <= 6 || final.Status != domain.JobDeadLetter {
		t.Fatalf("mixed attempts: total=%d callbacks=%d calls=%d status=%s err=%v", final.Attempts, final.CallbackAttempts, calls.Load(), final.Status, err)
	}
	if _, err = c.ClaimCallback(ctx, "extra", time.Millisecond, time.Second); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("exhausted callback redelivered")
	}
}

func TestCallbackStartValidatesTokenDeadlineAndSingleDispatch(t *testing.T) {
	c := integrationRedisClient(t)
	flushIntegrationRedis(t, c)
	ctx := context.Background()
	j := callbackFixture(t, c, "https://receiver.example")
	claim, err := c.ClaimCallback(ctx, "owner", time.Millisecond, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.StartCallback(ctx, claim, 3*time.Second); !errors.Is(err, store.ErrConflict) {
		t.Fatal("insufficient lease allowed dispatch")
	}
	wrong := claim
	wrong.Token = "stale-token"
	if _, err = c.StartCallback(ctx, wrong, time.Second); !errors.Is(err, store.ErrConflict) {
		t.Fatal("wrong token allowed dispatch")
	}
	before, _ := c.GetJob(ctx, j.ID)
	if before.CallbackAttempts != 0 {
		t.Fatal("rejected dispatch consumed callback budget")
	}
	started, err := c.StartCallback(ctx, claim, time.Second)
	if err != nil || started.CallbackAttempts != 1 {
		t.Fatal("valid dispatch failed")
	}
	if _, err = c.StartCallback(ctx, claim, time.Second); !errors.Is(err, store.ErrConflict) {
		t.Fatal("same claim dispatched twice")
	}
	after, _ := c.GetJob(ctx, j.ID)
	if after.CallbackAttempts != 1 {
		t.Fatal("duplicate dispatch consumed budget")
	}
}
