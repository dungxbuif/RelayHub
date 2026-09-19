package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/dungxbuif/RelayHub/internal/broker"
	natsbroker "github.com/dungxbuif/RelayHub/internal/broker/nats"
	"github.com/dungxbuif/RelayHub/internal/delivery"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/google/uuid"
)

type JetStreamOptions struct {
	Logger                                         *slog.Logger
	Concurrency                                    int
	AttemptTimeout, LeaseDuration, ShutdownTimeout time.Duration
	Now                                            func() time.Time
	NewToken                                       func() string
	Observe                                        func(string)
}

type JetStreamWorker struct {
	store     store.CallbackAttemptStore
	consumer  broker.Consumer
	publisher broker.Publisher
	delivery  Deliverer
	options   JetStreamOptions
	slots     chan struct{}
	lifecycle sync.Mutex
	stopping  bool
	inflight  sync.WaitGroup
}

func NewJetStream(repository store.CallbackAttemptStore, consumer broker.Consumer, publisher broker.Publisher, deliverer Deliverer, options JetStreamOptions) *JetStreamWorker {
	if options.Concurrency <= 0 {
		options.Concurrency = 8
	}
	if options.AttemptTimeout <= 0 {
		options.AttemptTimeout = 10 * time.Second
	}
	if options.LeaseDuration < options.AttemptTimeout+5*time.Second {
		options.LeaseDuration = options.AttemptTimeout + 20*time.Second
	}
	if options.ShutdownTimeout <= 0 {
		options.ShutdownTimeout = 10 * time.Second
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.NewToken == nil {
		options.NewToken = uuid.NewString
	}
	return &JetStreamWorker{store: repository, consumer: consumer, publisher: publisher, delivery: deliverer, options: options, slots: make(chan struct{}, options.Concurrency)}
}

func (worker *JetStreamWorker) Run(ctx context.Context) error {
	if worker.consumer == nil {
		return errors.New("callback consumer is required")
	}
	receiveCtx, cancelReceive := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelReceive()
	subscription, err := worker.consumer.Consume(receiveCtx, broker.ConsumerConfig{Stream: "RH_CALLBACKS", DurableName: "relayhub-callbacks-v1", Filter: "rh.v1.callback.*", MaxPending: worker.options.Concurrency}, worker.handle)
	if err != nil {
		return err
	}
	<-ctx.Done()
	worker.lifecycle.Lock()
	worker.stopping = true
	worker.lifecycle.Unlock()
	drainCtx, cancel := context.WithTimeout(context.Background(), worker.options.ShutdownTimeout)
	defer cancel()
	drainErr := subscription.Drain(drainCtx)
	done := make(chan struct{})
	go func() { worker.inflight.Wait(); close(done) }()
	select {
	case <-done:
		if drainErr != nil && !errors.Is(drainErr, context.Canceled) {
			return drainErr
		}
		return nil
	case <-drainCtx.Done():
		cancelReceive()
		return drainCtx.Err()
	}
}

func (worker *JetStreamWorker) handle(ctx context.Context, message broker.Message) {
	worker.lifecycle.Lock()
	if worker.stopping {
		worker.lifecycle.Unlock()
		_ = message.Nack(0)
		return
	}
	select {
	case worker.slots <- struct{}{}:
		// Reserve capacity before returning to the serial NATS callback loop.
	default:
		worker.lifecycle.Unlock()
		_ = message.Nack(100 * time.Millisecond)
		return
	}
	worker.inflight.Add(1)
	worker.lifecycle.Unlock()
	go func() {
		defer worker.inflight.Done()
		defer func() { <-worker.slots }()
		worker.process(ctx, message)
	}()
}

type callbackEnvelope struct {
	DeliveryID string       `json:"delivery_id"`
	Generation int64        `json:"generation"`
	Event      domain.Event `json:"event"`
}

func (worker *JetStreamWorker) process(ctx context.Context, message broker.Message) {
	var envelope callbackEnvelope
	if err := json.Unmarshal(message.Data(), &envelope); err != nil || envelope.DeliveryID == "" || envelope.Event.ID == "" {
		worker.observe("invalid_message")
		_ = message.Ack(ctx)
		return
	}
	if envelope.Generation == 0 {
		envelope.Generation = 1
	}
	now := worker.options.Now().UTC()
	dispatch, disposition, err := worker.store.BeginCallbackAttempt(ctx, envelope.DeliveryID, envelope.Generation, worker.options.NewToken(), now, worker.options.LeaseDuration)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrConflict) {
			worker.observe("invalid_message")
			_ = message.Ack(ctx)
		} else {
			worker.observe("store_error")
			_ = message.Nack(time.Second)
		}
		return
	}
	switch disposition {
	case store.CallbackDispatchBusy:
		delay := dispatch.RetryAt.Sub(now)
		if delay < 0 {
			delay = 0
		}
		_ = message.Nack(delay)
		return
	case store.CallbackDispatchComplete:
		_ = message.Ack(ctx)
		return
	case store.CallbackDispatchDeadLetter:
		worker.publishDLQ(ctx, message, dispatch)
		return
	case store.CallbackDispatchReady:
	default:
		worker.observe("store_error")
		return
	}
	_ = message.Progress()
	deadline := dispatch.LeaseExpiresAt.Add(-store.CallbackFinishMargin)
	attemptCtx, cancel := context.WithDeadline(ctx, deadline)
	result := worker.delivery.Deliver(attemptCtx, delivery.Request{App: dispatch.App, Event: dispatch.Event, Body: dispatch.Body, Secret: dispatch.Secret})
	cancel()
	if ctx.Err() != nil {
		return
	}
	now = worker.options.Now().UTC()
	outcome := delivery.Classify(result.Status, result.Headers, result.Err, dispatch.Attempt, now)
	transition := store.CallbackAttemptTransition{Now: now, RetryAt: outcome.RetryAt, Reason: outcome.Reason}
	switch outcome.Kind {
	case delivery.Delivered:
		transition.Status = domain.JobDelivered
	case delivery.Retry:
		transition.Status = domain.JobPending
	default:
		transition.Status = domain.JobDeadLetter
	}
	if err := worker.store.FinishCallbackAttempt(ctx, dispatch.DeliveryID, dispatch.Generation, dispatch.Token, dispatch.Attempt, transition); err != nil {
		worker.observe("store_error")
		return
	}
	worker.observe(string(transition.Status))
	if worker.options.Logger != nil {
		worker.options.Logger.Info("Callback operation", "app_id", dispatch.TargetAppID, "event_id", dispatch.Event.ID, "job_id", dispatch.PublicJobID, "attempt", dispatch.Attempt, "outcome", string(transition.Status))
	}
	if transition.Status == domain.JobDelivered {
		_ = message.Ack(ctx)
		return
	}
	if transition.Status == domain.JobPending {
		delay := transition.RetryAt.Sub(now)
		if delay < 0 {
			delay = 0
		}
		_ = message.Nack(delay)
		return
	}
	dispatch.Reason = transition.Reason
	worker.publishDLQ(ctx, message, dispatch)
}

func (worker *JetStreamWorker) publishDLQ(ctx context.Context, message broker.Message, dispatch store.CallbackDispatch) {
	if !dispatch.DLQPublished {
		if worker.publisher == nil {
			worker.observe("broker_error")
			return
		}
		subjects, err := natsbroker.SubjectsForApp(dispatch.TargetAppID)
		if err != nil {
			worker.observe("store_error")
			return
		}
		payload, err := json.Marshal(struct {
			DeliveryID string       `json:"delivery_id"`
			Generation int64        `json:"generation"`
			Event      domain.Event `json:"event"`
			Reason     string       `json:"reason"`
		}{dispatch.DeliveryID, dispatch.Generation, dispatch.Event, dispatch.Reason})
		if err != nil {
			worker.observe("store_error")
			return
		}
		_, err = worker.publisher.Publish(ctx, broker.Publication{Subject: subjects.DeadLetters, Data: payload, MessageID: "rh-v1-callback-dlq-" + dispatch.DeliveryID + "-g" + strconv.FormatInt(dispatch.Generation, 10)})
		if err != nil {
			worker.observe("broker_error")
			return
		}
		if err := worker.store.MarkCallbackDLQPublished(ctx, dispatch.DeliveryID, dispatch.Generation, worker.options.Now().UTC()); err != nil {
			worker.observe("store_error")
			return
		}
	}
	_ = message.Ack(ctx)
}

func (worker *JetStreamWorker) observe(outcome string) {
	if worker.options.Observe != nil {
		worker.options.Observe(outcome)
	}
}
