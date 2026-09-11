package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/broker"
	"github.com/dungxbuif/RelayHub/internal/delivery"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
)

type callbackRepository struct {
	mu                          sync.Mutex
	dispatch                    store.CallbackDispatch
	disposition                 store.CallbackDispatchDisposition
	transition                  *store.CallbackAttemptTransition
	beginErr, finishErr, dlqErr error
	dlqMarked                   bool
}

func (r *callbackRepository) BeginCallbackAttempt(context.Context, string, string, time.Time, time.Duration) (store.CallbackDispatch, store.CallbackDispatchDisposition, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dispatch, r.disposition, r.beginErr
}
func (r *callbackRepository) FinishCallbackAttempt(_ context.Context, _ string, token string, attempt int, transition store.CallbackAttemptTransition) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if token != r.dispatch.Token || attempt != r.dispatch.Attempt {
		return store.ErrConflict
	}
	r.transition = &transition
	return r.finishErr
}
func (r *callbackRepository) MarkCallbackDLQPublished(context.Context, string, time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dlqMarked = true
	return r.dlqErr
}

type callbackMessage struct {
	data     []byte
	acked    int
	nacks    []time.Duration
	progress int
}

func (m *callbackMessage) Data() []byte              { return m.data }
func (m *callbackMessage) Ack(context.Context) error { m.acked++; return nil }
func (m *callbackMessage) Nack(delay time.Duration) error {
	m.nacks = append(m.nacks, delay)
	return nil
}
func (m *callbackMessage) Progress() error { m.progress++; return nil }

type callbackPublisher struct {
	publications []broker.Publication
	err          error
}

func (p *callbackPublisher) Publish(_ context.Context, publication broker.Publication) (broker.PublishAck, error) {
	p.publications = append(p.publications, publication)
	return broker.PublishAck{}, p.err
}

type fixedDeliverer struct {
	result   delivery.Result
	requests []delivery.Request
}

func (d *fixedDeliverer) Deliver(_ context.Context, request delivery.Request) delivery.Result {
	d.requests = append(d.requests, request)
	return d.result
}

func callbackFixture(now time.Time) (*callbackRepository, *callbackMessage) {
	url := "https://new-receiver.example/callback"
	event := domain.Event{ID: "evt_1", Type: "order.created", SourceAppID: "app_source", TargetAppIDs: []string{"app_target"}, Data: json.RawMessage(`{"amount":42}`), CreatedAt: now.Add(-time.Minute)}
	body, _ := json.Marshal(event)
	repository := &callbackRepository{disposition: store.CallbackDispatchReady, dispatch: store.CallbackDispatch{
		DeliveryID: "dlv_1", PublicJobID: "job_1", TargetAppID: "app_target", Token: "lease-token", Attempt: 1,
		LeaseExpiresAt: now.Add(time.Minute), CredentialVersion: 7,
		App:   domain.App{ID: "app_target", Enabled: true, DeliveryMode: domain.DeliveryCallback, CallbackURL: &url},
		Event: event, Body: body, Secret: []byte("rotated-secret"),
	}}
	payload, _ := json.Marshal(map[string]any{"delivery_id": "dlv_1", "event": event})
	return repository, &callbackMessage{data: payload}
}

func TestJetStreamCallbackPersistsSuccessBeforeAck(t *testing.T) {
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	repository, message := callbackFixture(now)
	deliverer := &fixedDeliverer{result: delivery.Result{Status: http.StatusNoContent}}
	worker := NewJetStream(repository, nil, &callbackPublisher{}, deliverer, JetStreamOptions{Now: func() time.Time { return now }, NewToken: func() string { return "lease-token" }})
	worker.process(context.Background(), message)
	if repository.transition == nil || repository.transition.Status != domain.JobDelivered || message.acked != 1 || len(message.nacks) != 0 {
		t.Fatalf("transition=%+v ack=%d nacks=%v", repository.transition, message.acked, message.nacks)
	}
	if len(deliverer.requests) != 1 || string(deliverer.requests[0].Secret) != "rotated-secret" || *deliverer.requests[0].App.CallbackURL != "https://new-receiver.example/callback" {
		t.Fatalf("dispatch did not use freshly validated callback state: %#v", deliverer.requests)
	}
}

func TestJetStreamCallbackRetryAfterUsesDelayedNack(t *testing.T) {
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	repository, message := callbackFixture(now)
	deliverer := &fixedDeliverer{result: delivery.Result{Status: http.StatusTooManyRequests, Headers: http.Header{"Retry-After": []string{"17"}}}}
	worker := NewJetStream(repository, nil, &callbackPublisher{}, deliverer, JetStreamOptions{Now: func() time.Time { return now }, NewToken: func() string { return "lease-token" }})
	worker.process(context.Background(), message)
	if repository.transition == nil || repository.transition.Status != domain.JobPending || !repository.transition.RetryAt.Equal(now.Add(17*time.Second)) || message.acked != 0 || len(message.nacks) != 1 || message.nacks[0] != 17*time.Second {
		t.Fatalf("transition=%+v ack=%d nacks=%v", repository.transition, message.acked, message.nacks)
	}
}

func TestJetStreamCallbackPersistsAndPublishesDLQBeforeAck(t *testing.T) {
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	repository, message := callbackFixture(now)
	publisher := &callbackPublisher{}
	worker := NewJetStream(repository, nil, publisher, &fixedDeliverer{result: delivery.Result{Status: http.StatusBadRequest}}, JetStreamOptions{Now: func() time.Time { return now }, NewToken: func() string { return "lease-token" }})
	worker.process(context.Background(), message)
	if repository.transition == nil || repository.transition.Status != domain.JobDeadLetter || len(publisher.publications) != 1 || !repository.dlqMarked || message.acked != 1 {
		t.Fatalf("transition=%+v publishes=%d marked=%v ack=%d", repository.transition, len(publisher.publications), repository.dlqMarked, message.acked)
	}
	if publisher.publications[0].MessageID != "rh-v1-callback-dlq-dlv_1" {
		t.Fatalf("message id=%q", publisher.publications[0].MessageID)
	}
}

func TestJetStreamCallbackCrashRecoveryAndStaleLease(t *testing.T) {
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name        string
		disposition store.CallbackDispatchDisposition
		dlq         bool
		delay       time.Duration
		ack         int
	}{
		{"completed before broker ack", store.CallbackDispatchComplete, false, 0, 1},
		{"persisted dead letter before publish", store.CallbackDispatchDeadLetter, true, 0, 1},
		{"live stale-worker fence", store.CallbackDispatchBusy, false, 9 * time.Second, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repository, message := callbackFixture(now)
			repository.disposition = tc.disposition
			repository.dispatch.RetryAt = now.Add(tc.delay)
			publisher := &callbackPublisher{}
			worker := NewJetStream(repository, nil, publisher, &fixedDeliverer{}, JetStreamOptions{Now: func() time.Time { return now }, NewToken: func() string { return "replacement" }})
			worker.process(context.Background(), message)
			if message.acked != tc.ack || (tc.delay > 0 && (len(message.nacks) != 1 || message.nacks[0] != tc.delay)) || (len(publisher.publications) == 1) != tc.dlq {
				t.Fatalf("ack=%d nacks=%v publications=%d", message.acked, message.nacks, len(publisher.publications))
			}
		})
	}
}

type callbackConsumer struct {
	config  broker.ConsumerConfig
	handler broker.Handler
	sub     *callbackSubscription
	ready   chan struct{}
	ctx     context.Context
}

func (c *callbackConsumer) Consume(ctx context.Context, config broker.ConsumerConfig, handler broker.Handler) (broker.Subscription, error) {
	c.ctx = ctx
	c.config = config
	c.handler = handler
	close(c.ready)
	return c.sub, nil
}

type callbackSubscription struct {
	drained chan struct{}
	onDrain func(context.Context) error
}

func (s *callbackSubscription) Drain(ctx context.Context) error {
	close(s.drained)
	if s.onDrain != nil {
		return s.onDrain(ctx)
	}
	return nil
}

func TestJetStreamCallbackRunUsesBoundedExplicitConsumerAndDrains(t *testing.T) {
	consumer := &callbackConsumer{sub: &callbackSubscription{drained: make(chan struct{})}, ready: make(chan struct{})}
	consumer.sub.onDrain = func(context.Context) error {
		if consumer.ctx.Err() != nil {
			return errors.New("receive context canceled before drain")
		}
		return nil
	}
	repository, _ := callbackFixture(time.Now())
	worker := NewJetStream(repository, consumer, &callbackPublisher{}, &fixedDeliverer{}, JetStreamOptions{Concurrency: 3})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	<-consumer.ready
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if consumer.config.Stream != "RH_CALLBACKS" || consumer.config.Filter != "rh.v1.callback.*" || consumer.config.MaxPending != 3 {
		t.Fatalf("config=%+v", consumer.config)
	}
	select {
	case <-consumer.sub.drained:
	default:
		t.Fatal("subscription was not drained")
	}
}

func TestJetStreamCallbackBoundsConcurrencyAndWaitsForInflightShutdown(t *testing.T) {
	consumer := &callbackConsumer{sub: &callbackSubscription{drained: make(chan struct{})}, ready: make(chan struct{})}
	repository, fixtureMessage := callbackFixture(time.Now())
	gate := make(chan struct{})
	var active, maximum atomic.Int32
	deliverer := deliverFunc(func(context.Context, delivery.Request) delivery.Result {
		current := active.Add(1)
		for previous := maximum.Load(); current > previous && !maximum.CompareAndSwap(previous, current); previous = maximum.Load() {
		}
		<-gate
		active.Add(-1)
		return delivery.Result{Status: http.StatusNoContent}
	})
	worker := NewJetStream(repository, consumer, &callbackPublisher{}, deliverer, JetStreamOptions{Concurrency: 3, ShutdownTimeout: time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	<-consumer.ready
	for range 10 {
		message := &callbackMessage{data: append([]byte(nil), fixtureMessage.data...)}
		// The production NATS adapter invokes callbacks serially. The callback
		// must return after scheduling bounded work so deliveries can overlap.
		consumer.handler(consumer.ctx, message)
	}
	deadline := time.After(time.Second)
	for active.Load() != 3 {
		select {
		case <-deadline:
			t.Fatalf("active=%d max=%d", active.Load(), maximum.Load())
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("returned before inflight completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(gate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if maximum.Load() != 3 {
		t.Fatalf("maximum concurrency=%d", maximum.Load())
	}
}

func TestJetStreamCallbackTerminatesStalePoisonAndNacksTransientStoreFailure(t *testing.T) {
	now := time.Now()
	for _, test := range []struct {
		name      string
		err       error
		ack, nack int
	}{
		{"missing stale delivery", store.ErrNotFound, 1, 0},
		{"irrecoverable conflicting delivery", store.ErrConflict, 1, 0},
		{"temporary database outage", errors.New("temporary"), 0, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository, message := callbackFixture(now)
			repository.beginErr = test.err
			NewJetStream(repository, nil, &callbackPublisher{}, &fixedDeliverer{}, JetStreamOptions{Now: func() time.Time { return now }}).process(context.Background(), message)
			if message.acked != test.ack || len(message.nacks) != test.nack {
				t.Fatalf("ack=%d nacks=%v", message.acked, message.nacks)
			}
		})
	}
}

type deliverFunc func(context.Context, delivery.Request) delivery.Result

func (function deliverFunc) Deliver(ctx context.Context, request delivery.Request) delivery.Result {
	return function(ctx, request)
}
