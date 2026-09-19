package runtime

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/dungxbuif/RelayHub/internal/broker"
	natsbroker "github.com/dungxbuif/RelayHub/internal/broker/nats"
	"github.com/dungxbuif/RelayHub/internal/config"
	"github.com/dungxbuif/RelayHub/internal/platform"
	"github.com/dungxbuif/RelayHub/internal/redisstate"
	postgresstore "github.com/dungxbuif/RelayHub/internal/store/postgres"
)

const instanceHeartbeatTTL = 30 * time.Second

type Dependencies struct {
	Postgres *postgresstore.Client
	NATS     *natsbroker.Client
	Redis    *redisstate.Client
	Instance platform.Instance
}

type API struct {
	Dependencies
	Sessions  *redisstate.RedisSessionStore
	Limiter   *redisstate.RateLimiter
	Ownership *redisstate.OwnershipStore
	base      *base
}

type Worker struct {
	Dependencies
	Ownership *redisstate.OwnershipStore
	base      *base
}

type base struct {
	health    broker.CompositeHealth
	ownership *redisstate.OwnershipStore
	instance  redisstate.Instance
	cancel    context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func NewAPI(ctx context.Context, cfg config.Config, deps Dependencies, logger *slog.Logger) (*API, error) {
	_ = logger
	baseRuntime, err := newBase(ctx, cfg, deps)
	if err != nil {
		return nil, err
	}
	keys := redisstate.Keyspace{Prefix: cfg.Redis.KeyPrefix}
	return &API{
		Dependencies: deps,
		Sessions:     redisstate.NewRedisSessionStore(deps.Redis, keys),
		Limiter:      redisstate.NewRateLimiter(deps.Redis),
		Ownership:    baseRuntime.ownership,
		base:         baseRuntime,
	}, nil
}

func NewWorker(ctx context.Context, cfg config.Config, deps Dependencies, logger *slog.Logger) (*Worker, error) {
	_ = logger
	baseRuntime, err := newBase(ctx, cfg, deps)
	if err != nil {
		return nil, err
	}
	return &Worker{Dependencies: deps, Ownership: baseRuntime.ownership, base: baseRuntime}, nil
}

func (a *API) Ready(ctx context.Context) error {
	if a == nil || a.base == nil {
		return errors.New("API runtime unavailable")
	}
	return a.base.health.Ping(ctx)
}

func (a *API) Ping(ctx context.Context) error { return a.Ready(ctx) }

func (a *API) Close(ctx context.Context) error {
	if a == nil || a.base == nil {
		return nil
	}
	return a.base.close(ctx)
}

func (w *Worker) Ready(ctx context.Context) error {
	if w == nil || w.base == nil {
		return errors.New("worker runtime unavailable")
	}
	return w.base.health.Ping(ctx)
}

func (w *Worker) Ping(ctx context.Context) error { return w.Ready(ctx) }

func (w *Worker) Close(ctx context.Context) error {
	if w == nil || w.base == nil {
		return nil
	}
	return w.base.close(ctx)
}

func newBase(ctx context.Context, cfg config.Config, deps Dependencies) (*base, error) {
	if deps.Postgres == nil || deps.NATS == nil || deps.Redis == nil || deps.Instance.ID == "" || deps.Instance.Generation == 0 || deps.Instance.StartedAt.IsZero() || (deps.Instance.Role != "api" && deps.Instance.Role != "worker") {
		return nil, errors.New("invalid runtime dependencies")
	}
	keys := redisstate.Keyspace{Prefix: cfg.Redis.KeyPrefix}
	ownership := redisstate.NewOwnershipStore(deps.Redis, keys)
	instance := redisstate.Instance{ID: deps.Instance.ID, Role: deps.Instance.Role, Generation: deps.Instance.Generation, StartedAt: deps.Instance.StartedAt}
	if err := ownership.Heartbeat(ctx, instance, instanceHeartbeatTTL); err != nil {
		return nil, errors.New("initialize runtime heartbeat")
	}
	heartbeatCtx, cancel := context.WithCancel(ctx)
	runtime := &base{
		health: broker.CompositeHealth{deps.NATS, deps.Postgres, deps.Redis}, ownership: ownership,
		instance: instance, cancel: cancel, done: make(chan struct{}),
	}
	go runtime.heartbeat(heartbeatCtx)
	return runtime, nil
}

func (r *base) heartbeat(ctx context.Context) {
	defer close(r.done)
	ticker := time.NewTicker(instanceHeartbeatTTL / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			r.closeErr = r.release(cleanup)
			cancel()
			return
		case <-ticker.C:
			_ = r.ownership.Heartbeat(ctx, r.instance, instanceHeartbeatTTL)
		}
	}
}

func (r *base) close(ctx context.Context) error {
	r.closeOnce.Do(r.cancel)
	select {
	case <-r.done:
		return r.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *base) release(ctx context.Context) error {
	err := r.ownership.ReleaseInstance(ctx, r.instance)
	if errors.Is(err, redisstate.ErrNotFound) || errors.Is(err, redisstate.ErrOwnershipLost) || errors.Is(err, redisstate.ErrUnavailable) {
		return nil
	}
	return err
}
