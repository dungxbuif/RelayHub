// Package worker runs bounded callback attempts against durable consumer-group work.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/dungxbuif/RelayHub/internal/delivery"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/google/uuid"
)

type Deliverer interface {
	Deliver(context.Context, delivery.Request) delivery.Result
}
type Notifier interface {
	PublishJob(context.Context, domain.Job) error
}
type Options struct {
	Logger                                       *slog.Logger
	Concurrency                                  int
	ShutdownTimeout, ReclaimIdle, AttemptTimeout time.Duration
	Now                                          func() time.Time
	Notifier                                     Notifier
	NotificationError                            func()
	Observe                                      func(string)
}
type Worker struct {
	store    store.CallbackStore
	delivery Deliverer
	options  Options
	consumer string
}

func New(s store.CallbackStore, d Deliverer, o Options) *Worker {
	if o.Concurrency <= 0 {
		o.Concurrency = 8
	}
	if o.ShutdownTimeout <= 0 {
		o.ShutdownTimeout = 10 * time.Second
	}
	if o.AttemptTimeout <= 0 {
		o.AttemptTimeout = 10 * time.Second
	}
	if o.ReclaimIdle < o.AttemptTimeout+5*time.Second {
		o.ReclaimIdle = o.AttemptTimeout + 20*time.Second
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return &Worker{store: s, delivery: d, options: o, consumer: uuid.NewString()}
}
func (w *Worker) Run(ctx context.Context) error {
	active, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < w.options.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				ioctx, stop := context.WithTimeout(ctx, 250*time.Millisecond)
				err := w.store.PromoteCallbacks(ioctx, w.options.Now().UTC(), 100)
				var claim store.CallbackClaim
				if err == nil && ctx.Err() == nil {
					claim, err = w.store.ClaimCallback(ioctx, w.consumer, w.options.ReclaimIdle, w.options.ReclaimIdle)
				}
				stop()
				if err != nil {
					if !errors.Is(err, store.ErrNotFound) && ctx.Err() == nil && w.options.Observe != nil {
						w.options.Observe("store_error")
					}
					select {
					case <-ctx.Done():
						return
					case <-time.After(25 * time.Millisecond):
					}
					continue
				}
				if ctx.Err() != nil {
					return
				} // leave a just-reserved claim reclaimable
				w.process(active, claim)
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	<-ctx.Done()
	timer := time.NewTimer(w.options.ShutdownTimeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		cancel()
		<-done
		return nil
	}
}
func (w *Worker) process(ctx context.Context, claim store.CallbackClaim) {
	ioctx, stop := context.WithTimeout(ctx, time.Second)
	data, err := w.store.LoadCallback(ioctx, claim)
	stop()
	if err != nil {
		w.observeStoreError(ctx, err)
		return
	}
	transition := store.CallbackTransition{Now: w.options.Now().UTC()}
	if !data.App.Enabled || data.App.CallbackURL == nil || *data.App.CallbackURL == "" || (data.App.DeliveryMode != domain.DeliveryCallback && data.App.DeliveryMode != domain.DeliveryAll) || !data.Job.Callback {
		transition.Status = domain.JobPending
		transition.Reason = "callback_disabled"
		transition.Disable = true
	} else if data.Job.CallbackAttempts >= delivery.MaxAttempts {
		transition.Status = domain.JobDeadLetter
		transition.Reason = "attempts_exhausted"
	} else if len(data.Body) == 0 {
		transition.Status = domain.JobDeadLetter
		transition.Reason = "event_expired"
	} else {
		// An absolute deadline prevents a paused old worker from extending a
		// request past the original lease when it resumes after reclaim.
		if time.Until(claim.ExpiresAt) < w.options.AttemptTimeout+store.CallbackFinishMargin {
			return
		}
		deadline := claim.ExpiresAt.Add(-store.CallbackFinishMargin)
		attempt, cancel := context.WithDeadline(ctx, deadline)
		startCtx, startCancel := context.WithTimeout(attempt, time.Second)
		job, startErr := w.store.StartCallback(startCtx, claim, w.options.AttemptTimeout)
		startCancel()
		if startErr != nil || attempt.Err() != nil {
			w.observeStoreError(ctx, startErr)
			cancel()
			return
		}
		data.Job = job
		callCtx, callCancel := context.WithTimeout(attempt, w.options.AttemptTimeout)
		result := w.delivery.Deliver(callCtx, delivery.Request{App: data.App, Event: data.Event, Body: data.Body, Secret: data.Secret})
		callCancel()
		cancel()
		if ctx.Err() != nil {
			return
		}
		transition.Now = w.options.Now().UTC()
		outcome := delivery.Classify(result.Status, result.Headers, result.Err, data.Job.CallbackAttempts, transition.Now)
		transition.Reason = outcome.Reason
		transition.RetryAt = outcome.RetryAt
		switch outcome.Kind {
		case delivery.Delivered:
			transition.Status = domain.JobDelivered
		case delivery.Retry:
			transition.Status = domain.JobPending
		default:
			transition.Status = domain.JobDeadLetter
		}
	}
	ioctx, stop = context.WithTimeout(ctx, time.Second)
	job, err := w.store.FinishCallback(ioctx, claim, transition)
	stop()
	if err != nil {
		w.observeStoreError(ctx, err)
		return
	}
	if w.options.Logger != nil {
		w.options.Logger.Info("Callback operation", "app_id", job.TargetAppID, "event_id", job.EventID, "job_id", job.ID, "attempt", job.CallbackAttempts, "outcome", string(job.Status))
	}
	if w.options.Observe != nil {
		w.options.Observe(string(job.Status))
	}
	if w.options.Notifier != nil {
		nctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		err := w.options.Notifier.PublishJob(nctx, job)
		cancel()
		if err != nil && w.options.NotificationError != nil {
			w.options.NotificationError()
		}
	}
	ioctx, stop = context.WithTimeout(ctx, time.Second)
	err = w.store.AckCallback(ioctx, claim)
	stop()
	w.observeStoreError(ctx, err)
}

func (w *Worker) observeStoreError(ctx context.Context, err error) {
	if err != nil && ctx.Err() == nil && !errors.Is(err, store.ErrNotFound) && !errors.Is(err, store.ErrConflict) && w.options.Observe != nil {
		w.options.Observe("store_error")
	}
}
