package worker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/delivery"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
)

type memory struct {
	mu         sync.Mutex
	claims     []store.CallbackClaim
	data       map[string]store.CallbackData
	finished   map[string]store.CallbackTransition
	acked      int
	active     int
	max        int
	failFinish bool
}

func (m *memory) ClaimCallback(ctx context.Context, consumer string, idle, lease time.Duration) (store.CallbackClaim, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.claims) == 0 {
		return store.CallbackClaim{}, store.ErrNotFound
	}
	c := m.claims[0]
	m.claims = m.claims[1:]
	m.active++
	if m.active > m.max {
		m.max = m.active
	}
	return c, nil
}
func (m *memory) LoadCallback(ctx context.Context, c store.CallbackClaim) (store.CallbackData, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data[c.JobID], nil
}
func (m *memory) FinishCallback(ctx context.Context, c store.CallbackClaim, tr store.CallbackTransition) (domain.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failFinish {
		return domain.Job{}, errors.New("failure")
	}
	if ctx.Err() != nil {
		return domain.Job{}, ctx.Err()
	}
	m.finished[c.JobID] = tr
	j := m.data[c.JobID].Job
	j.Status = tr.Status
	return j, nil
}
func (m *memory) AckCallback(ctx context.Context, c store.CallbackClaim) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.finished[c.JobID]; !ok {
		return errors.New("ack before persistence")
	}
	m.active--
	m.acked++
	return nil
}
func (m *memory) PromoteCallbacks(context.Context, time.Time, int) error { return nil }

type notifier struct {
	m   *memory
	n   atomic.Int32
	bad atomic.Bool
}

func (n *notifier) PublishJob(ctx context.Context, j domain.Job) error {
	n.m.mu.Lock()
	_, ok := n.m.finished[j.ID]
	n.m.mu.Unlock()
	if !ok {
		n.bad.Store(true)
	}
	n.n.Add(1)
	return errors.New("notifier offline")
}
func fixture(url string, n int) *memory {
	m := &memory{data: map[string]store.CallbackData{}, finished: map[string]store.CallbackTransition{}}
	for i := 0; i < n; i++ {
		id := fmt.Sprint(i)
		m.claims = append(m.claims, store.CallbackClaim{JobID: id, MessageID: id, Token: id})
		m.data[id] = store.CallbackData{Job: domain.Job{ID: id, Attempts: 1, Callback: true}, App: domain.App{Enabled: true, CallbackURL: &url, DeliveryMode: domain.DeliveryCallback}, Event: domain.Event{ID: "evt"}, Body: []byte(`{"id":"evt","data":{}}`), Secret: []byte("secret")}
	}
	return m
}
func waitFor(t *testing.T, f func() bool) {
	t.Helper()
	limit := time.After(3 * time.Second)
	for !f() {
		select {
		case <-limit:
			t.Fatal("timed out")
		case <-time.After(time.Millisecond):
		}
	}
}
func TestWorkerOutcomesAndNotifications(t *testing.T) {
	for _, tc := range []struct {
		code, attempt int
		mode          domain.DeliveryMode
		status        domain.JobStatus
	}{{204, 1, domain.DeliveryCallback, domain.JobDelivered}, {503, 1, domain.DeliveryAll, domain.JobPending}, {400, 1, domain.DeliveryCallback, domain.JobDeadLetter}, {503, 6, domain.DeliveryCallback, domain.JobDeadLetter}, {204, 1, domain.DeliveryQueue, domain.JobPending}, {204, 1, domain.DeliveryWebSocket, domain.JobPending}} {
		t.Run(fmt.Sprint(tc.code, tc.attempt, tc.mode), func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(tc.code) }))
			defer srv.Close()
			m := fixture(srv.URL, 1)
			d := m.data["0"]
			d.Job.Attempts = tc.attempt
			d.App.DeliveryMode = tc.mode
			m.data["0"] = d
			n := &notifier{m: m}
			now := time.Unix(1000, 0)
			w := New(m, delivery.NewCallback(time.Second), Options{Concurrency: 1, Now: func() time.Time { return now }, Notifier: n})
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- w.Run(ctx) }()
			waitFor(t, func() bool { m.mu.Lock(); defer m.mu.Unlock(); return m.acked == 1 })
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			tr := m.finished["0"]
			if tr.Status != tc.status || n.bad.Load() || n.n.Load() != 1 {
				t.Fatalf("transition %+v notifications %d", tr, n.n.Load())
			}
			if tc.mode == domain.DeliveryQueue || tc.mode == domain.DeliveryWebSocket {
				if calls.Load() != 0 {
					t.Fatal("ineligible callback delivered")
				}
			} else if calls.Load() != 1 {
				t.Fatal("missing attempt")
			}
			if tc.code == 503 && tc.attempt == 1 && !tr.RetryAt.Equal(now.Add(time.Second)) {
				t.Fatal("retry clock wrong")
			}
		})
	}
}
func TestWorkerConcurrencyAndGracefulCancel(t *testing.T) {
	var active, max atomic.Int32
	gate := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v := active.Add(1)
		for old := max.Load(); v > old && !max.CompareAndSwap(old, v); old = max.Load() {
		}
		<-gate
		active.Add(-1)
		w.WriteHeader(204)
	}))
	defer srv.Close()
	m := fixture(srv.URL, 10)
	ctx, cancel := context.WithCancel(context.Background())
	w := New(m, delivery.NewCallback(time.Second), Options{Concurrency: 3, ShutdownTimeout: time.Second})
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	waitFor(t, func() bool { return active.Load() == 3 })
	cancel()
	close(gate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if max.Load() != 3 || m.acked != 3 || len(m.claims) != 7 {
		t.Fatalf("max=%d ack=%d left=%d", max.Load(), m.acked, len(m.claims))
	}
}
func TestWorkerShutdownLeavesUnfinishedReclaimable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(150 * time.Millisecond):
		}
	}))
	defer srv.Close()
	m := fixture(srv.URL, 1)
	ctx, cancel := context.WithCancel(context.Background())
	w := New(m, delivery.NewCallback(time.Second), Options{Concurrency: 1, ShutdownTimeout: 20 * time.Millisecond})
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	waitFor(t, func() bool { m.mu.Lock(); defer m.mu.Unlock(); return m.active == 1 })
	cancel()
	start := time.Now()
	<-done
	if time.Since(start) > 500*time.Millisecond || m.acked != 0 || len(m.finished) != 0 {
		t.Fatal("unfinished work not reclaimable")
	}
}
func TestWorkerPersistenceFailureNeverAcknowledges(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer srv.Close()
	m := fixture(srv.URL, 1)
	m.failFinish = true
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	w := New(m, delivery.NewCallback(time.Second), Options{Concurrency: 1})
	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if m.acked != 0 {
		t.Fatal("failed persistence acked")
	}
}
