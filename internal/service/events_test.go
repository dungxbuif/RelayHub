package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
)

func eventFixture() (*EventService, *eventMemory, *time.Time) {
	now := time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC)
	m := &eventMemory{events: map[string]domain.Event{}, jobs: map[string]domain.Job{}, idem: map[string]store.Publication{}}
	apps := newMemoryAppStore()
	apps.apps["a"] = domain.App{ID: "a", Enabled: true}
	apps.apps["b"] = domain.App{ID: "b", Enabled: true}
	apps.apps["off"] = domain.App{ID: "off", Enabled: false}
	n := 0
	s := NewEventService(m, apps, EventOptions{Now: func() time.Time { return now }, NewID: func(prefix string) (string, error) { n++; return fmt.Sprintf("%s%d", prefix, n), nil }})
	return s, m, &now
}
func validEvent() PublishEvent {
	return PublishEvent{Type: "order.created", TargetAppIDs: []string{"b", "a"}, Data: json.RawMessage(`{"n":9007199254740993}`)}
}
func TestEventValidation(t *testing.T) {
	cases := []struct {
		name   string
		change func(*PublishEvent)
		key    string
	}{
		{"empty type", func(p *PublishEvent) { p.Type = " " }, "k"},
		{"empty targets", func(p *PublishEvent) { p.TargetAppIDs = nil }, "k"},
		{"too many", func(p *PublishEvent) { p.TargetAppIDs = make([]string, 101) }, "k"},
		{"duplicates", func(p *PublishEvent) { p.TargetAppIDs = []string{"a", " a "} }, "k"},
		{"missing target", func(p *PublishEvent) { p.TargetAppIDs = []string{"missing"} }, "k"},
		{"disabled target", func(p *PublishEvent) { p.TargetAppIDs = []string{"off"} }, "k"},
		{"array", func(p *PublishEvent) { p.Data = json.RawMessage(`[]`) }, "k"},
		{"null", func(p *PublishEvent) { p.Data = json.RawMessage(`null`) }, "k"},
		{"malformed", func(p *PublishEvent) { p.Data = json.RawMessage(`{`) }, "k"},
		{"invalid UTF-8", func(p *PublishEvent) { p.Data = json.RawMessage("{\"text\":\"\xff\"}") }, "k"},
		{"missing data", func(p *PublishEvent) { p.Data = nil }, "k"},
		{"missing key", func(p *PublishEvent) {}, ""},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			s, m, _ := eventFixture()
			p := validEvent()
			tt.change(&p)
			_, _, _, err := s.Publish(context.Background(), "source", p, tt.key)
			if !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("got %v", err)
			}
			if len(m.events) != 0 {
				t.Fatal("invalid publish persisted")
			}
		})
	}
}
func TestPublishReplayEnvelopeAndOwnership(t *testing.T) {
	s, _, now := eventFixture()
	ctx := context.Background()
	p := validEvent()
	e, j, replay, err := s.Publish(ctx, "source", p, "key")
	if err != nil || replay || len(j) != 2 {
		t.Fatalf("publish %v %v %v", j, replay, err)
	}
	if e.SourceAppID != "source" || e.TargetAppIDs[0] != "a" || !e.CreatedAt.Equal(*now) || string(e.Data) != string(p.Data) {
		t.Fatalf("envelope %#v", e)
	}
	again, jobs, replay, err := s.Publish(ctx, "source", p, "key")
	if err != nil || !replay || again.ID != e.ID || jobs[0].ID != j[0].ID {
		t.Fatalf("replay %v %v", replay, err)
	}
	other, _, replay, err := s.Publish(ctx, "other", p, "key")
	if err != nil || replay || other.ID == e.ID {
		t.Fatal("idempotency namespace leaked")
	}
	for _, actor := range []string{"source", "a", "b"} {
		if _, err := s.GetEvent(ctx, actor, e.ID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.GetEvent(ctx, "stranger", e.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("event leaked")
	}
	if _, err := s.GetJob(ctx, "stranger", j[0].ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("job leaked")
	}
	for _, actor := range []string{"source", "a"} {
		if _, err := s.GetJob(ctx, actor, j[0].ID); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Ack(ctx, "stranger", e.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross ack %v", err)
	}
}
func TestLeaseRecoveryAckAndControls(t *testing.T) {
	s, _, now := eventFixture()
	ctx := context.Background()
	e, j, _, err := s.Publish(ctx, "source", validEvent(), "key")
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		limit int
		wait  time.Duration
	}{{0, 0}, {101, 0}, {1, -1}, {1, 31 * time.Second}} {
		if _, err := s.Lease(ctx, "a", tt.limit, tt.wait); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("bounds %v", err)
		}
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	count := 0
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			items, err := s.Lease(ctx, "a", 20, 0)
			if err != nil {
				t.Error(err)
			}
			mu.Lock()
			count += len(items)
			mu.Unlock()
		}()
	}
	wg.Wait()
	if count != 1 {
		t.Fatalf("competing leases=%d", count)
	}
	*now = now.Add(61 * time.Second)
	items, err := s.Lease(ctx, "a", 20, 0)
	if err != nil || len(items) != 1 || items[0].Event.ID != e.ID || items[0].Job.Status != domain.JobLeased {
		t.Fatalf("redelivery %v %v", items, err)
	}
	for range 2 {
		if err := s.Ack(ctx, "a", e.ID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.RequeueJob(ctx, j[0].ID); !errors.Is(err, ErrConflict) {
		t.Fatalf("acked requeue %v", err)
	}
	if _, err := s.DeadLetterJob(ctx, j[1].ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Ack(ctx, "b", e.ID); !errors.Is(err, ErrConflict) {
		t.Fatalf("dead ack %v", err)
	}
	if job, err := s.RequeueJob(ctx, j[1].ID); err != nil || job.Status != domain.JobPending {
		t.Fatalf("requeue %v %v", job, err)
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	start := time.Now()
	if _, err := s.Lease(cancelCtx, "empty", 20, 30*time.Second); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("cancellation delayed")
	}
}
func TestJobTransitionTable(t *testing.T) {
	states := []domain.JobStatus{domain.JobPending, domain.JobLeased, domain.JobDelivered, domain.JobAcked, domain.JobDeadLetter, "unknown"}
	allowed := map[domain.JobStatus][]domain.JobStatus{domain.JobPending: {domain.JobLeased, domain.JobAcked, domain.JobDeadLetter}, domain.JobLeased: {domain.JobPending, domain.JobDelivered, domain.JobAcked, domain.JobDeadLetter}, domain.JobDelivered: {domain.JobAcked, domain.JobDeadLetter}, domain.JobAcked: {domain.JobAcked}, domain.JobDeadLetter: {domain.JobPending, domain.JobDeadLetter}}
	for _, from := range states {
		for _, to := range states {
			want := false
			for _, v := range allowed[from] {
				if v == to {
					want = true
				}
			}
			if got := from.CanTransition(to); got != want {
				t.Errorf("%s -> %s = %v", from, to, got)
			}
		}
	}
}

// In-memory boundary for service policy tests; concurrency/atomicity are separately tested against Redis.
type eventMemory struct {
	mu     sync.Mutex
	events map[string]domain.Event
	jobs   map[string]domain.Job
	idem   map[string]store.Publication
}

func (m *eventMemory) PublishEvent(_ context.Context, p store.Publication, key string, _ store.EventRetention) (store.Publication, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := p.Event.SourceAppID + ":" + key
	if old, ok := m.idem[k]; ok {
		return old, true, nil
	}
	m.idem[k] = p
	m.events[p.Event.ID] = p.Event
	for _, j := range p.Jobs {
		m.jobs[j.ID] = j
	}
	return p, false, nil
}
func (m *eventMemory) GetEvent(_ context.Context, id string) (domain.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.events[id]
	if !ok {
		return e, store.ErrNotFound
	}
	return e, nil
}
func (m *eventMemory) GetJob(_ context.Context, id string) (domain.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return j, store.ErrNotFound
	}
	return j, nil
}
func (m *eventMemory) LeaseJobs(ctx context.Context, target string, limit int, now time.Time, lease time.Duration) ([]store.LeasedEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []store.LeasedEvent{}
	for id, j := range m.jobs {
		if j.TargetAppID == target && (j.Status == domain.JobPending || (j.Status == domain.JobLeased && j.LeaseUntil != nil && !j.LeaseUntil.After(now))) && len(out) < limit {
			j.Status = domain.JobLeased
			end := now.Add(lease)
			j.LeaseUntil = &end
			j.UpdatedAt = now
			j.Attempts++
			m.jobs[id] = j
			out = append(out, store.LeasedEvent{Event: m.events[j.EventID], Job: j})
		}
	}
	return out, nil
}
func (m *eventMemory) AckEvent(ctx context.Context, target, event string, now time.Time, retention time.Duration) error {
	m.mu.Lock()
	var id string
	for _, j := range m.jobs {
		if j.TargetAppID == target && j.EventID == event {
			id = j.ID
		}
	}
	m.mu.Unlock()
	if id == "" {
		return store.ErrNotFound
	}
	_, err := m.TransitionJob(ctx, id, domain.JobAcked, now, retention)
	return err
}
func (m *eventMemory) TransitionJob(_ context.Context, id string, status domain.JobStatus, now time.Time, _ time.Duration) (domain.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return j, store.ErrNotFound
	}
	if !j.Status.CanTransition(status) {
		return j, store.ErrConflict
	}
	j.Status = status
	j.UpdatedAt = now
	j.LeaseUntil = nil
	m.jobs[id] = j
	return j, nil
}

func TestReplaySurvivesTargetDisableAndReturnsOriginalPayload(t *testing.T) {
	s, _, _ := eventFixture()
	ctx := context.Background()
	original, _, _, err := s.Publish(ctx, "source", validEvent(), "k")
	if err != nil {
		t.Fatal(err)
	}
	apps := s.apps.(*memoryAppStore)
	if _, err := apps.DisableApplication(ctx, "a", time.Now()); err != nil {
		t.Fatal(err)
	}
	changed := validEvent()
	changed.Type = "changed"
	changed.Data = json.RawMessage(`{"changed":true}`)
	got, _, replay, err := s.Publish(ctx, "source", changed, "k")
	if err != nil || !replay || got.ID != original.ID || got.Type != original.Type {
		t.Fatalf("replay after disable %#v %v %v", got, replay, err)
	}
}
func TestLongPollTimeoutAndWakeup(t *testing.T) {
	s, _, _ := eventFixture()
	ctx := context.Background()
	start := time.Now()
	items, err := s.Lease(ctx, "empty", 20, 30*time.Millisecond)
	if err != nil || len(items) != 0 || time.Since(start) > time.Second {
		t.Fatalf("timeout %v %v", items, err)
	}
	result := make(chan []LeasedEvent, 1)
	go func() {
		items, err := s.Lease(ctx, "a", 1, time.Second)
		if err != nil {
			t.Error(err)
		}
		result <- items
	}()
	if _, _, _, err := s.Publish(ctx, "source", validEvent(), "k"); err != nil {
		t.Fatal(err)
	}
	select {
	case items := <-result:
		if len(items) != 1 {
			t.Fatal("poll missed newly available job")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("long poll never returned")
	}
}

func (m *eventMemory) FindPublication(_ context.Context, source, key string) (store.Publication, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.idem[source+":"+key]
	if !ok {
		return p, store.ErrNotFound
	}
	return p, nil
}

func TestReplayStillRejectsMalformedTargetList(t *testing.T) {
	s, _, _ := eventFixture()
	ctx := context.Background()
	if _, _, _, err := s.Publish(ctx, "source", validEvent(), "k"); err != nil {
		t.Fatal(err)
	}
	for _, targets := range [][]string{{"a", " a "}, {" "}} {
		input := validEvent()
		input.TargetAppIDs = targets
		if _, _, _, err := s.Publish(ctx, "source", input, "k"); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("malformed replay targets %v: %v", targets, err)
		}
	}
}

// Notification checks observe committed repository state, not only mock calls.
type stateNotifier struct {
	t                      *testing.T
	m                      *eventMemory
	events, jobs, failures int
	fail                   bool
}

func (n *stateNotifier) PublishEvent(ctx context.Context, e domain.Event) error {
	n.events++
	got, err := n.m.GetEvent(ctx, e.ID)
	if err != nil || got.ID != e.ID {
		n.t.Fatal("notified before event commit")
	}
	if n.fail {
		return errors.New("notify failed")
	}
	return nil
}
func (n *stateNotifier) PublishJob(ctx context.Context, j domain.Job) error {
	n.jobs++
	got, err := n.m.GetJob(ctx, j.ID)
	if err != nil || got.Status != j.Status {
		n.t.Fatal("notified before job commit")
	}
	if n.fail {
		return errors.New("notify failed")
	}
	return nil
}
func (m *eventMemory) GetEventJob(_ context.Context, target, event string) (domain.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, j := range m.jobs {
		if j.TargetAppID == target && j.EventID == event {
			return j, nil
		}
	}
	return domain.Job{}, store.ErrNotFound
}
func TestEventNotificationsFollowDurabilityAndFailuresDoNotRollback(t *testing.T) {
	s, m, _ := eventFixture()
	n := &stateNotifier{t: t, m: m, fail: true}
	s.options.Notifier = n
	s.options.NotificationError = func(error) { n.failures++ }
	ctx := context.Background()
	e, jobs, _, err := s.Publish(ctx, "source", validEvent(), "notify")
	if err != nil || n.events != 1 || n.jobs != 2 || n.failures != 3 {
		t.Fatalf("publish %v counts %#v", err, n)
	}
	if _, _, replay, err := s.Publish(ctx, "source", validEvent(), "notify"); err != nil || !replay || n.events != 1 {
		t.Fatal("replay re-notified")
	}
	if _, err := s.Lease(ctx, "a", 1, 0); err != nil {
		t.Fatal(err)
	}
	if n.jobs != 3 {
		t.Fatal("lease not notified")
	}
	if err := s.Ack(ctx, "a", e.ID); err != nil || n.jobs != 4 {
		t.Fatal("ack notification")
	}
	var b domain.Job
	for _, j := range jobs {
		if j.TargetAppID == "b" {
			b = j
		}
	}
	if _, err := s.DeadLetterJob(ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RequeueJob(ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	before := n.jobs
	if _, err := s.DeadLetterJob(ctx, "missing"); err == nil || n.jobs != before {
		t.Fatal("failed transition notified")
	}
	if err := s.Ack(ctx, "outsider", e.ID); err == nil || n.jobs != before {
		t.Fatal("failed ack notified")
	}
	if _, _, _, err := s.Publish(ctx, "source", PublishEvent{}, "invalid"); err == nil || n.events != 1 {
		t.Fatal("invalid publish notified")
	}
}

func TestPublishMarksCallbackEligibility(t *testing.T) {
	for _, mode := range []domain.DeliveryMode{domain.DeliveryQueue, domain.DeliveryWebSocket, domain.DeliveryCallback, domain.DeliveryAll} {
		for _, url := range []string{"", "https://receiver.example/events"} {
			s, _, _ := eventFixture()
			apps := s.apps.(*memoryAppStore)
			app := apps.apps["a"]
			app.CallbackURL = &url
			app.DeliveryMode = mode
			apps.apps["a"] = app
			p := validEvent()
			p.TargetAppIDs = []string{"a"}
			_, jobs, _, err := s.Publish(context.Background(), "source", p, "key")
			if err != nil {
				t.Fatal(err)
			}
			want := url != "" && (mode == domain.DeliveryCallback || mode == domain.DeliveryAll)
			if jobs[0].Callback != want {
				t.Fatalf("mode=%s url=%s callback=%t", mode, url, jobs[0].Callback)
			}
		}
	}
}
