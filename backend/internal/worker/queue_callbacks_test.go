package worker

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/auth"
	"github.com/dungxbuif/RelayHub/internal/delivery"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
)

type queueCallbackRepository struct {
	store.QueueRepository
	mu         sync.Mutex
	claim      *domain.QueueResultCallback
	transition store.QueueResultCallbackTransition
	cancel     context.CancelFunc
}

func (repository *queueCallbackRepository) ClaimQueueResultCallback(context.Context, time.Time, time.Time, string) (domain.QueueResultCallback, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.claim == nil {
		return domain.QueueResultCallback{}, store.ErrNotFound
	}
	claim := *repository.claim
	repository.claim = nil
	return claim, nil
}
func (repository *queueCallbackRepository) FinishQueueResultCallback(_ context.Context, transition store.QueueResultCallbackTransition) error {
	repository.mu.Lock()
	repository.transition = transition
	repository.mu.Unlock()
	repository.cancel()
	return nil
}

func TestQueueResultCallbackWorkerSignsExactSafeBodyAndFinishes(t *testing.T) {
	body := []byte(`{"app_id":"app_1","delivery_id":"qdl_1","outcome":"success"}`)
	secret := []byte("queue-callback-secret")
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		raw, _ := io.ReadAll(request.Body)
		if string(raw) != string(body) || request.Header.Get("X-RelayHub-Signature") != auth.Sign(secret, request.Header.Get("X-RelayHub-Timestamp"), http.MethodPost, request.URL.RequestURI(), raw) {
			t.Fatalf("body=%s signature=%q", raw, request.Header.Get("X-RelayHub-Signature"))
		}
		response.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	repository := &queueCallbackRepository{cancel: cancel, claim: &domain.QueueResultCallback{ID: "qcb_1", EventID: "evt_1", URL: server.URL + "/result", Body: body, Secret: secret, Attempt: 1, ClaimToken: "claim", ClaimGeneration: 1}}
	callback := delivery.NewCallback(time.Second)
	callback.Transport = server.Client().Transport
	worker := NewQueueResultCallback(repository, callback, func() time.Time { return time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC) })
	if err := worker.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if repository.transition.Status != "delivered" || repository.transition.CallbackID != "qcb_1" || repository.transition.Attempt != 1 {
		t.Fatalf("transition=%#v", repository.transition)
	}
}
