package worker

import (
	"context"
	"errors"
	"time"

	"github.com/dungxbuif/RelayHub/internal/delivery"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/google/uuid"
)

type QueueResultCallbackWorker struct {
	repository store.QueueRepository
	deliverer  *delivery.Callback
	now        func() time.Time
}

func NewQueueResultCallback(repository store.QueueRepository, deliverer *delivery.Callback, now func() time.Time) *QueueResultCallbackWorker {
	if now == nil {
		now = time.Now
	}
	return &QueueResultCallbackWorker{repository: repository, deliverer: deliverer, now: now}
}

func (worker *QueueResultCallbackWorker) Run(ctx context.Context) error {
	if worker == nil || worker.repository == nil || worker.deliverer == nil {
		return errors.New("queue callback dependency unavailable")
	}
	for {
		if ctx.Err() != nil {
			return nil
		}
		now := worker.now().UTC()
		claim, err := worker.repository.ClaimQueueResultCallback(ctx, now, now.Add(worker.deliverer.Timeout+5*time.Second), "qcallback_"+uuid.NewString())
		if errors.Is(err, store.ErrNotFound) {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		if err != nil {
			return err
		}
		url := claim.URL
		result := worker.deliverer.Deliver(ctx, delivery.Request{App: domain.App{CallbackURL: &url}, Event: domain.Event{ID: claim.EventID}, Body: claim.Body, Secret: claim.Secret})
		now = worker.now().UTC()
		classified := delivery.Classify(result.Status, result.Headers, result.Err, claim.Attempt, now)
		transition := store.QueueResultCallbackTransition{CallbackID: claim.ID, Attempt: claim.Attempt, ClaimToken: claim.ClaimToken, ClaimGeneration: claim.ClaimGeneration, Reason: classified.Reason, RetryAt: classified.RetryAt, Now: now}
		switch classified.Kind {
		case delivery.Delivered:
			transition.Status = "delivered"
		case delivery.Retry:
			transition.Status = "pending"
		default:
			transition.Status = "failed"
		}
		if err := worker.repository.FinishQueueResultCallback(ctx, transition); err != nil && ctx.Err() == nil {
			return err
		}
	}
}
