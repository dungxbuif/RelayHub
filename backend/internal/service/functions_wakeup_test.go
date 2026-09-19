package service

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
)

type countedInvocationStore struct {
	*functionMemory
	reads chan struct{}
}

func (s *countedInvocationStore) GetInvocation(ctx context.Context, id string) (domain.Invocation, error) {
	s.reads <- struct{}{}
	return s.functionMemory.GetInvocation(ctx, id)
}

type resultWakeupTransport struct {
	functionNotify
	mu            sync.Mutex
	watches       map[string][]chan struct{}
	published     chan string
	beforePublish func(string)
	drop          bool
}

func (n *resultWakeupTransport) WatchInvocation(_ context.Context, id string) (store.InvocationWatch, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	ch := make(chan struct{}, 1)
	n.watches[id] = append(n.watches[id], ch)
	return silentWatch{ch: ch, close: func() {}}, nil
}

func (n *resultWakeupTransport) PublishInvocationResult(_ context.Context, id string) error {
	if n.beforePublish != nil {
		n.beforePublish(id)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.drop {
		for _, ch := range n.watches[id] {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
	n.published <- id
	return errors.New("lost broker acknowledgement")
}

func TestFunctionWaitsForResultWakeupWithoutPeriodicReads(t *testing.T) {
	memory := newFunctionMemory()
	repository := &countedInvocationStore{functionMemory: memory, reads: make(chan struct{}, 100)}
	invoked := make(chan domain.Invocation, 1)
	notifier := &resultWakeupTransport{watches: make(map[string][]chan struct{}), published: make(chan string, 1)}
	var svc *FunctionService
	notifier.functionNotify = func(ctx context.Context, v domain.Invocation) error {
		if err := svc.ClaimInvocation(ctx, "owner", "connection", v.ID); err != nil {
			return err
		}
		if err := svc.AcknowledgeInvocation(ctx, "owner", "connection", v.ID); err != nil {
			return err
		}
		invoked <- v
		return nil
	}
	svc = NewFunctionService(repository, FunctionOptions{Notifier: notifier})
	f, err := svc.Register(context.Background(), "owner", RegisterFunction{Name: "calculate", TimeoutSeconds: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, _, err := svc.Invoke(ctx, "caller", f.ID, "wait-key", json.RawMessage(`{}`)); done <- err }()
	v := <-invoked
	<-repository.reads // Initial persisted-state read after subscription/dispatch.
	select {
	case <-repository.reads:
		t.Fatal("idle claimed invocation polled the database before wakeup/deadline")
	case <-time.After(100 * time.Millisecond):
	}
	notifier.beforePublish = func(id string) {
		stored, err := memory.GetInvocation(ctx, id)
		if err != nil || !stored.Terminal() {
			t.Errorf("result hint preceded persistence: %#v %v", stored, err)
		}
	}
	if err := svc.CompleteResult(ctx, "owner", "connection", domain.RPCResult{InvocationID: v.ID, OK: true, Result: json.RawMessage(`{"value":42}`)}); err != nil {
		t.Fatalf("persisted result was rejected after hint failure: %v", err)
	}
	select {
	case <-notifier.published:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("persisted result emitted no wakeup")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("result wakeup did not finish caller")
	}
}

func TestFunctionCompletionPublishesOnlyPersistedResults(t *testing.T) {
	memory := newFunctionMemory()
	notifier := &resultWakeupTransport{watches: make(map[string][]chan struct{}), published: make(chan string, 2)}
	svc := NewFunctionService(memory, FunctionOptions{Notifier: notifier})
	memory.calls["inv_result"] = domain.Invocation{ID: "inv_result", OwnerAppID: "owner", ConnectionID: "connection", State: domain.InvocationClaimed, Deadline: time.Now().Add(time.Second)}
	result := domain.RPCResult{InvocationID: "inv_result", OK: true, Result: json.RawMessage(`{}`)}
	if err := svc.CompleteResult(context.Background(), "other", "connection", result); !errors.Is(err, store.ErrInvalidResult) {
		t.Fatalf("invalid result: %v", err)
	}
	select {
	case <-notifier.published:
		t.Fatal("invalid result published wakeup")
	default:
	}
	notifier.beforePublish = func(id string) {
		v, _ := memory.GetInvocation(context.Background(), id)
		if !v.Terminal() {
			t.Error("hint before durable result")
		}
	}
	if err := svc.CompleteResult(context.Background(), "owner", "connection", result); err != nil {
		t.Fatal(err)
	}
	select {
	case <-notifier.published:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("stored result did not publish wakeup")
	}
}
