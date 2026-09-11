package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
)

// The in-memory boundary replaces external Redis only; service validation,
// subscription ordering, cancellation and terminal-response mapping are real.
type functionMemory struct {
	store.FunctionStore
	mu        sync.Mutex
	functions map[string]domain.Function
	calls     map[string]domain.Invocation
	keys      map[string]string
	watches   map[string]int
}

func newFunctionMemory() *functionMemory {
	return &functionMemory{functions: map[string]domain.Function{}, calls: map[string]domain.Invocation{}, keys: map[string]string{}, watches: map[string]int{}}
}
func (m *functionMemory) CreateFunction(_ context.Context, f domain.Function) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, old := range m.functions {
		if old.AppID == f.AppID && old.Name == f.Name {
			return store.ErrConflict
		}
	}
	m.functions[f.ID] = f
	return nil
}
func (m *functionMemory) GetFunction(_ context.Context, id string) (domain.Function, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.functions[id]
	if !ok {
		return f, store.ErrNotFound
	}
	return f, nil
}
func (m *functionMemory) ListFunctions(_ context.Context, app string) ([]domain.Function, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []domain.Function{}
	for _, f := range m.functions {
		if f.AppID == app {
			out = append(out, f)
		}
	}
	return out, nil
}
func (m *functionMemory) DeleteFunction(_ context.Context, app, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.functions[id]
	if !ok || f.AppID != app {
		return store.ErrNotFound
	}
	delete(m.functions, id)
	return nil
}
func (m *functionMemory) FindInvocation(_ context.Context, app, key string) (domain.Invocation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, ok := m.keys[app+":"+key]
	if !ok {
		return domain.Invocation{}, store.ErrNotFound
	}
	return m.expire(id), nil
}
func (m *functionMemory) CreateInvocation(_ context.Context, v domain.Invocation, key string) (domain.Invocation, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := v.CallerAppID + ":" + key
	if id, ok := m.keys[k]; ok {
		return m.expire(id), true, nil
	}
	m.keys[k] = v.ID
	m.calls[v.ID] = v
	return v, false, nil
}
func (m *functionMemory) expire(id string) domain.Invocation {
	v := m.calls[id]
	if !v.Terminal() {
		if v.State == domain.InvocationClaimed && !time.Now().Before(v.Deadline) {
			v.State = domain.InvocationTimeout
		} else if v.State != domain.InvocationClaimed && !time.Now().Before(v.ClaimBy) {
			v.State = domain.InvocationUnavailable
		}
		m.calls[id] = v
	}
	return v
}
func (m *functionMemory) GetInvocation(_ context.Context, id string) (domain.Invocation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.calls[id]; !ok {
		return domain.Invocation{}, store.ErrNotFound
	}
	return m.expire(id), nil
}
func (m *functionMemory) ClaimInvocation(_ context.Context, app, conn, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := m.expire(id)
	if v.OwnerAppID != app || v.State != domain.InvocationPending {
		return store.ErrInvalidResult
	}
	v.State = domain.InvocationReserved
	v.ConnectionID = conn
	m.calls[id] = v
	return nil
}
func (m *functionMemory) AcknowledgeInvocation(_ context.Context, app, conn, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := m.expire(id)
	if v.OwnerAppID != app || v.ConnectionID != conn || v.State != domain.InvocationReserved {
		return store.ErrInvalidResult
	}
	v.State = domain.InvocationClaimed
	m.calls[id] = v
	return nil
}
func (m *functionMemory) ReleaseInvocation(_ context.Context, app, conn, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := m.expire(id)
	if v.OwnerAppID != app || v.ConnectionID != conn || v.Terminal() {
		return store.ErrInvalidResult
	}
	v.State = domain.InvocationPending
	v.ConnectionID = ""
	m.calls[id] = v
	return nil
}
func (m *functionMemory) CompleteInvocation(_ context.Context, app, conn string, r domain.RPCResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := m.expire(r.InvocationID)
	if v.OwnerAppID != app || v.ConnectionID != conn || v.State != domain.InvocationClaimed {
		return store.ErrInvalidResult
	}
	v.Reply = &r
	v.State = domain.InvocationSuccess
	if !r.OK {
		v.State = domain.InvocationHandlerError
	}
	m.calls[v.ID] = v
	return nil
}

type silentWatch struct {
	ch    chan struct{}
	close func()
}

func (w silentWatch) Updates() <-chan struct{} { return w.ch }
func (w silentWatch) Close()                   { w.close() }
func (m *functionMemory) WatchInvocation(_ context.Context, id string) (store.InvocationWatch, error) {
	m.mu.Lock()
	m.watches[id]++
	m.mu.Unlock()
	return silentWatch{make(chan struct{}, 1), func() { m.mu.Lock(); m.watches[id]--; m.mu.Unlock() }}, nil
}

type functionNotify func(context.Context, domain.Invocation) error

func (f functionNotify) PublishInvocation(c context.Context, v domain.Invocation) error {
	return f(c, v)
}

func TestFunctionRegistrationValidationAndOwnerScope(t *testing.T) {
	m := newFunctionMemory()
	s := NewFunctionService(m, FunctionOptions{})
	for _, name := range []string{"", "1calculate", "with space", "../calc", strings.Repeat("x", 65)} {
		if _, e := s.Register(context.Background(), "owner", RegisterFunction{Name: name, TimeoutSeconds: 1}); !errors.Is(e, ErrInvalidInput) {
			t.Fatalf("name %q: %v", name, e)
		}
	}
	for _, seconds := range []int{0, -1, 31} {
		if _, e := s.Register(context.Background(), "owner", RegisterFunction{Name: "calculate", TimeoutSeconds: seconds}); !errors.Is(e, ErrInvalidInput) {
			t.Fatalf("timeout %d: %v", seconds, e)
		}
	}
	f, e := s.Register(context.Background(), "owner", RegisterFunction{Name: "calculate", TimeoutSeconds: 30})
	if e != nil || !strings.HasPrefix(f.ID, "fn_") || !f.Enabled || f.AppID != "owner" || f.CreatedAt.IsZero() {
		t.Fatalf("function %#v %v", f, e)
	}
	if _, e = s.Register(context.Background(), "owner", RegisterFunction{Name: "calculate", TimeoutSeconds: 1}); !errors.Is(e, ErrConflict) {
		t.Fatalf("duplicate: %v", e)
	}
	if _, e = s.Register(context.Background(), "other", RegisterFunction{Name: "calculate", TimeoutSeconds: 1}); e != nil {
		t.Fatal(e)
	}
	items, e := s.List(context.Background(), "owner")
	if e != nil || len(items) != 1 || items[0].ID != f.ID {
		t.Fatalf("list %#v %v", items, e)
	}
	if e = s.Delete(context.Background(), "other", f.ID); !errors.Is(e, ErrNotFound) {
		t.Fatalf("owner mutation: %v", e)
	}
	if e = s.Delete(context.Background(), "owner", f.ID); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Register(context.Background(), "owner", RegisterFunction{Name: "calculate", TimeoutSeconds: 1}); e != nil {
		t.Fatalf("name not released: %v", e)
	}
}
func TestFunctionInvokeValidationOfflineAndCancellation(t *testing.T) {
	m := newFunctionMemory()
	s := NewFunctionService(m, FunctionOptions{ClaimTimeout: 20 * time.Millisecond})
	f, _ := s.Register(context.Background(), "owner", RegisterFunction{Name: "calculate", TimeoutSeconds: 1})
	for _, raw := range []string{"", "null", "[]", `"text"`, `{`, `{"x":"` + strings.Repeat("x", 65536) + `"}`} {
		if _, _, e := s.Invoke(context.Background(), "caller", f.ID, "bad", json.RawMessage(raw)); !errors.Is(e, ErrInvalidInput) {
			t.Fatalf("input %.20q: %v", raw, e)
		}
	}
	if _, _, e := s.Invoke(context.Background(), "caller", f.ID, "", json.RawMessage(`{}`)); !errors.Is(e, ErrInvalidInput) {
		t.Fatal(e)
	}
	if _, _, e := s.Invoke(context.Background(), "caller", "fn_unknown", "key", json.RawMessage(`{}`)); !errors.Is(e, ErrNotFound) {
		t.Fatal(e)
	}
	_, replay, e := s.Invoke(context.Background(), "caller", f.ID, "offline", json.RawMessage(`{}`))
	if !errors.Is(e, ErrFunctionUnavailable) || replay {
		t.Fatalf("offline %v %v", replay, e)
	}
	_, replay, e = s.Invoke(context.Background(), "caller", f.ID, "offline", json.RawMessage(`{}`))
	if !errors.Is(e, ErrFunctionUnavailable) || !replay {
		t.Fatalf("offline replay %v %v", replay, e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, e = s.Invoke(ctx, "caller", f.ID, "cancel", json.RawMessage(`{}`)); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}
func TestFunctionFastResultConcurrentReplayAndResponderFencing(t *testing.T) {
	for _, success := range []bool{true, false} {
		t.Run(map[bool]string{true: "success", false: "handler_error"}[success], func(t *testing.T) {
			m := newFunctionMemory()
			var s *FunctionService
			var dispatched atomic.Int32
			s = NewFunctionService(m, FunctionOptions{Notifier: functionNotify(func(ctx context.Context, v domain.Invocation) error {
				dispatched.Add(1)
				m.mu.Lock()
				watchers := m.watches[v.ID]
				m.mu.Unlock()
				if watchers == 0 {
					t.Error("published before waiter subscribed")
				}
				if e := s.ClaimInvocation(ctx, "intruder", "evil", v.ID); e == nil {
					t.Error("intruder claimed")
				}
				if e := s.ClaimInvocation(ctx, "owner", "conn_one", v.ID); e != nil {
					return e
				}
				if e := s.AcknowledgeInvocation(ctx, "owner", "conn_one", v.ID); e != nil {
					return e
				}
				r := domain.RPCResult{InvocationID: v.ID, OK: success}
				if success {
					r.Result = json.RawMessage(`{"n":9007199254740993}`)
				} else {
					r.Error = json.RawMessage(`{"code":"declined","message":"Cannot calculate"}`)
				}
				for _, identity := range [][2]string{{"intruder", "conn_one"}, {"owner", "conn_wrong"}} {
					if e := s.CompleteResult(ctx, identity[0], identity[1], r); !errors.Is(e, store.ErrInvalidResult) {
						t.Errorf("mismatched result: %v", e)
					}
				}
				if e := s.CompleteResult(ctx, "owner", "conn_one", r); e != nil {
					return e
				}
				if e := s.CompleteResult(ctx, "owner", "conn_one", r); !errors.Is(e, store.ErrInvalidResult) {
					t.Errorf("duplicate: %v", e)
				}
				return nil
			})})
			f, _ := s.Register(context.Background(), "owner", RegisterFunction{Name: "calculate", TimeoutSeconds: 1})
			var wg sync.WaitGroup
			results := make(chan domain.RPCResult, 20)
			for range 20 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					r, _, e := s.Invoke(context.Background(), "caller", f.ID, "same-key", json.RawMessage(`{}`))
					if e != nil {
						t.Error(e)
					}
					results <- r
				}()
			}
			wg.Wait()
			close(results)
			var id string
			for r := range results {
				if r.OK != success || !strings.HasPrefix(r.InvocationID, "inv_") {
					t.Fatalf("result %#v", r)
				}
				if id != "" && id != r.InvocationID {
					t.Fatal("replays diverged")
				}
				id = r.InvocationID
				if success && string(r.Result) != `{"n":9007199254740993}` {
					t.Fatalf("number changed %s", r.Result)
				}
			}
			if dispatched.Load() != 1 {
				t.Fatalf("dispatched %d", dispatched.Load())
			}
			_ = s.Delete(context.Background(), "owner", f.ID)
			if r, replay, e := s.Invoke(context.Background(), "caller", f.ID, "same-key", json.RawMessage(`{}`)); e != nil || !replay || r.InvocationID != id {
				t.Fatalf("deleted replay %#v %v %v", r, replay, e)
			}
		})
	}
}
func TestFunctionTimeoutAndCancelledWaiterDoNotAcceptLateResult(t *testing.T) {
	m := newFunctionMemory()
	var s *FunctionService
	calls := make(chan domain.Invocation, 2)
	s = NewFunctionService(m, FunctionOptions{Notifier: functionNotify(func(ctx context.Context, v domain.Invocation) error {
		if e := s.ClaimInvocation(ctx, "owner", "conn", v.ID); e != nil {
			return e
		}
		if e := s.AcknowledgeInvocation(ctx, "owner", "conn", v.ID); e != nil {
			return e
		}
		calls <- v
		return nil
	})})
	f, _ := s.Register(context.Background(), "owner", RegisterFunction{Name: "calculate", TimeoutSeconds: 1})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, _, e := s.Invoke(ctx, "caller", f.ID, "cancel", json.RawMessage(`{}`)); done <- e }()
	v := <-calls
	cancel()
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatalf("cancel %v", e)
	}
	if e := s.CompleteResult(context.Background(), "owner", "conn", domain.RPCResult{InvocationID: v.ID, OK: true, Result: json.RawMessage(`{}`)}); e != nil {
		t.Fatalf("cancel should not revoke claimed work: %v", e)
	}
	if _, replay, e := s.Invoke(context.Background(), "caller", f.ID, "cancel", json.RawMessage(`{}`)); e != nil || !replay {
		t.Fatal("cancel replay", e)
	}
	start := time.Now()
	_, _, e := s.Invoke(context.Background(), "caller", f.ID, "timeout", json.RawMessage(`{}`))
	if !errors.Is(e, ErrFunctionTimeout) || time.Since(start) > 1500*time.Millisecond {
		t.Fatalf("persisted deadline: %v %s", e, time.Since(start))
	}
	v = <-calls
	if e := s.CompleteResult(context.Background(), "owner", "conn", domain.RPCResult{InvocationID: v.ID, OK: true, Result: json.RawMessage(`{}`)}); !errors.Is(e, store.ErrInvalidResult) {
		t.Fatalf("late result %v", e)
	}
	if _, replay, e := s.Invoke(context.Background(), "caller", f.ID, "timeout", json.RawMessage(`{}`)); !errors.Is(e, ErrFunctionTimeout) || !replay {
		t.Fatalf("timeout replay %v %v", replay, e)
	}
	if e := s.CompleteResult(context.Background(), "owner", "conn", domain.RPCResult{InvocationID: "inv_unknown", OK: true, Result: json.RawMessage(`{}`)}); !errors.Is(e, store.ErrInvalidResult) {
		t.Fatalf("unknown result %v", e)
	}
}

func TestFunctionWireLimitAndCallerKeyNamespace(t *testing.T) {
	m := newFunctionMemory()
	var s *FunctionService
	var delivered atomic.Int32
	s = NewFunctionService(m, FunctionOptions{Notifier: functionNotify(func(ctx context.Context, v domain.Invocation) error {
		raw, e := json.Marshal(v.Frame())
		if e != nil || len(raw) > 65536 {
			t.Errorf("oversized dispatched %d %v", len(raw), e)
		}
		delivered.Add(1)
		if e = s.ClaimInvocation(ctx, "owner", "conn", v.ID); e != nil {
			return e
		}
		if e = s.AcknowledgeInvocation(ctx, "owner", "conn", v.ID); e != nil {
			return e
		}
		return s.CompleteResult(ctx, "owner", "conn", domain.RPCResult{InvocationID: v.ID, OK: true, Result: json.RawMessage(`{}`)})
	})})
	f, _ := s.Register(context.Background(), "owner", RegisterFunction{Name: "calculate", TimeoutSeconds: 1})
	// Payload close to the boundary is accepted; JSON HTML escaping must be
	// included when calculating the complete outbound message's byte budget.
	r, _, e := s.Invoke(context.Background(), "caller", f.ID, "same", json.RawMessage(`{"x":"`+strings.Repeat("x", 65200)+`"}`))
	if e != nil {
		t.Fatal(e)
	}
	other, replay, e := s.Invoke(context.Background(), "another-caller", f.ID, "same", json.RawMessage(`{}`))
	if e != nil || replay || other.InvocationID == r.InvocationID || delivered.Load() != 2 {
		t.Fatal("caller/key namespaces merged", e)
	}
	if _, _, e = s.Invoke(context.Background(), "caller", f.ID, "escape", json.RawMessage(`{"x":"`+strings.Repeat("<", 12000)+`"}`)); !errors.Is(e, ErrInvalidInput) {
		t.Fatalf("escaped frame exceeds limit: %v", e)
	}
	if delivered.Load() != 2 {
		t.Fatal("oversized frame dispatched")
	}
	disabled := false
	off, _ := s.Register(context.Background(), "owner", RegisterFunction{Name: "disabled", TimeoutSeconds: 1, Enabled: &disabled})
	if _, _, e = s.Invoke(context.Background(), "caller", off.ID, "disabled", json.RawMessage(`{}`)); !errors.Is(e, ErrNotFound) {
		t.Fatalf("disabled function %v", e)
	}
}

func TestFunctionRejectsInvalidUTF8Results(t *testing.T) {
	for _, success := range []bool{true, false} {
		t.Run(map[bool]string{true: "result", false: "error"}[success], func(t *testing.T) {
			memory := newFunctionMemory()
			svc := NewFunctionService(memory, FunctionOptions{})
			now := time.Now()
			memory.calls["inv_utf8"] = domain.Invocation{ID: "inv_utf8", OwnerAppID: "owner", ConnectionID: "connection", State: domain.InvocationClaimed, Deadline: now.Add(time.Second), ClaimBy: now.Add(250 * time.Millisecond)}
			reply := domain.RPCResult{InvocationID: "inv_utf8", OK: success}
			if success {
				reply.Result = append(append([]byte(`{"text":"`), 0xff), []byte(`"}`)...)
			} else {
				reply.Error = append(append([]byte(`{"code":"failed","message":"`), 0xff), []byte(`"}`)...)
			}
			if e := svc.CompleteResult(context.Background(), "owner", "connection", reply); !errors.Is(e, store.ErrInvalidResult) {
				t.Fatalf("malformed UTF-8 completed invocation: %v", e)
			}
			if v, _ := memory.GetInvocation(context.Background(), "inv_utf8"); v.Terminal() {
				t.Fatal("invalid result changed state")
			}
			if success {
				reply.Result = json.RawMessage(`{"text":"€"}`)
			} else {
				reply.Error = json.RawMessage(`{"code":"failed","message":"€"}`)
			}
			if e := svc.CompleteResult(context.Background(), "owner", "connection", reply); e != nil {
				t.Fatalf("valid Unicode result rejected: %v", e)
			}
		})
	}
}
