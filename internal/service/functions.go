package service

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/google/uuid"
)

var ErrFunctionUnavailable = errors.New("function unavailable")
var ErrFunctionTimeout = errors.New("function timeout")

type RegisterFunction struct {
	Name           string `json:"name"`
	TimeoutSeconds int    `json:"timeout_seconds"`
	Enabled        *bool  `json:"enabled,omitempty"`
}
type FunctionNotifier interface {
	PublishInvocation(context.Context, domain.Invocation) error
}
type FunctionOptions struct {
	Notifier     FunctionNotifier
	ClaimTimeout time.Duration
	Observe      func(string, time.Duration)
}
type FunctionService struct {
	repository store.FunctionStore
	options    FunctionOptions
}

func NewFunctionService(repository store.FunctionStore, options FunctionOptions) *FunctionService {
	if options.ClaimTimeout <= 0 || options.ClaimTimeout > 250*time.Millisecond {
		options.ClaimTimeout = 250 * time.Millisecond
	}
	return &FunctionService{repository, options}
}
func (s *FunctionService) Register(ctx context.Context, owner string, in RegisterFunction) (domain.Function, error) {
	if owner == "" || !domain.ValidFunctionName(in.Name) || in.TimeoutSeconds < 1 || in.TimeoutSeconds > 30 {
		return domain.Function{}, ErrInvalidInput
	}
	enabled := true
	if in.Enabled != nil {
		enabled = *in.Enabled
	}
	now := time.Now().UTC()
	f := domain.Function{ID: "fn_" + uuid.NewString(), AppID: owner, Name: in.Name, TimeoutSeconds: in.TimeoutSeconds, Enabled: enabled, CreatedAt: now, UpdatedAt: now}
	if err := s.repository.CreateFunction(ctx, f); err != nil {
		return domain.Function{}, mapStoreError(err)
	}
	s.observe("registered", 0)
	return f, nil
}
func (s *FunctionService) List(ctx context.Context, owner string) ([]domain.Function, error) {
	if owner == "" {
		return nil, ErrInvalidInput
	}
	items, e := s.repository.ListFunctions(ctx, owner)
	if e != nil {
		return nil, mapStoreError(e)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	return items, nil
}
func (s *FunctionService) Delete(ctx context.Context, owner, id string) error {
	if owner == "" || id == "" {
		return ErrInvalidInput
	}
	return mapStoreError(s.repository.DeleteFunction(ctx, owner, id))
}
func (s *FunctionService) Invoke(ctx context.Context, caller, id, key string, input json.RawMessage) (domain.RPCResult, bool, error) {
	if err := ctx.Err(); err != nil {
		return domain.RPCResult{}, false, err
	}
	if caller == "" || id == "" || len(key) > 256 || strings.TrimSpace(key) == "" || len(input) > 1<<20 || !domain.JSONObject(input) {
		return domain.RPCResult{}, false, ErrInvalidInput
	}
	v, e := s.repository.FindInvocation(ctx, caller, key)
	replay := e == nil
	if e != nil && !errors.Is(e, store.ErrNotFound) {
		return domain.RPCResult{}, false, mapStoreError(e)
	}
	if !replay {
		f, err := s.repository.GetFunction(ctx, id)
		if err != nil {
			return domain.RPCResult{}, false, mapStoreError(err)
		}
		if !f.Enabled {
			return domain.RPCResult{}, false, ErrNotFound
		}
		now := time.Now().UTC()
		v = domain.Invocation{ID: "inv_" + uuid.NewString(), FunctionID: f.ID, OwnerAppID: f.AppID, CallerAppID: caller, Name: f.Name, Input: input, CreatedAt: now, Deadline: now.Add(time.Duration(f.TimeoutSeconds) * time.Second), ClaimBy: now.Add(s.options.ClaimTimeout), State: domain.InvocationPending}
		frame, err := json.Marshal(v.Frame())
		if err != nil || len(frame) > domain.FunctionFrameLimit {
			return domain.RPCResult{}, false, ErrInvalidInput
		}
		v, replay, e = s.repository.CreateInvocation(ctx, v, key)
		if e != nil {
			return domain.RPCResult{}, false, mapStoreError(e)
		}
	}
	if v.Terminal() {
		return invocationResponse(v, replay)
	}
	// Subscribe before publishing and always inspect persisted state. Pub/Sub is a
	// wakeup hint; polling recovers dropped messages and interrupted subscriptions.
	watch, e := s.repository.WatchInvocation(ctx, v.ID)
	if e != nil {
		return domain.RPCResult{}, replay, e
	}
	defer watch.Close()
	if !replay {
		s.observe("invoked", 0)
		if s.options.Notifier != nil {
			_ = s.options.Notifier.PublishInvocation(ctx, v)
		}
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if e := ctx.Err(); e != nil {
			return domain.RPCResult{}, replay, e
		}
		current, e := s.repository.GetInvocation(ctx, v.ID)
		if e != nil {
			return domain.RPCResult{}, replay, mapStoreError(e)
		}
		if current.Terminal() {
			if !replay {
				s.observe(string(current.State), time.Since(current.CreatedAt))
			}
			return invocationResponse(current, replay)
		}
		select {
		case <-ctx.Done():
			return domain.RPCResult{}, replay, ctx.Err()
		case <-watch.Updates():
		case <-ticker.C:
		}
	}
}
func invocationResponse(v domain.Invocation, replay bool) (domain.RPCResult, bool, error) {
	switch v.State {
	case domain.InvocationUnavailable:
		return domain.RPCResult{}, replay, ErrFunctionUnavailable
	case domain.InvocationTimeout:
		return domain.RPCResult{}, replay, ErrFunctionTimeout
	case domain.InvocationSuccess, domain.InvocationHandlerError:
		if v.Reply != nil {
			return *v.Reply, replay, nil
		}
	}
	return domain.RPCResult{}, replay, ErrConflict
}
func (s *FunctionService) ClaimInvocation(ctx context.Context, owner, conn, id string) error {
	return s.repository.ClaimInvocation(ctx, owner, conn, id)
}
func (s *FunctionService) AcknowledgeInvocation(ctx context.Context, owner, conn, id string) error {
	return s.repository.AcknowledgeInvocation(ctx, owner, conn, id)
}
func (s *FunctionService) ReleaseInvocation(ctx context.Context, owner, conn, id string) error {
	return s.repository.ReleaseInvocation(ctx, owner, conn, id)
}
func (s *FunctionService) CompleteResult(ctx context.Context, owner, conn string, result domain.RPCResult) error {
	if owner == "" || conn == "" || !domain.ValidRPCResult(result) {
		return store.ErrInvalidResult
	}
	return s.repository.CompleteInvocation(ctx, owner, conn, result)
}
func (s *FunctionService) observe(outcome string, elapsed time.Duration) {
	if s.options.Observe != nil {
		s.options.Observe(outcome, elapsed)
	}
}
