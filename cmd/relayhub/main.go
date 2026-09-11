package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dungxbuif/RelayHub/internal/auth"
	"github.com/dungxbuif/RelayHub/internal/config"
	"github.com/dungxbuif/RelayHub/internal/delivery"
	"github.com/dungxbuif/RelayHub/internal/httpapi"
	"github.com/dungxbuif/RelayHub/internal/observability"
	"github.com/dungxbuif/RelayHub/internal/realtime"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/dungxbuif/RelayHub/internal/store/redisstore"
	"github.com/dungxbuif/RelayHub/internal/worker"
	"github.com/dungxbuif/RelayHub/web"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("RelayHub stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	command := "api"
	if len(os.Args) > 1 {
		command = os.Args[1]
	}
	if len(os.Args) > 2 || (command != "api" && command != "worker") {
		return errors.New("usage: relayhub [api|worker]")
	}

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	redisClient, err := redisstore.NewClientWithPrefix(cfg.RedisURL, cfg.RedisKeyPrefix, cfg.JobRetention)
	if err != nil {
		return errors.New("create Redis client: invalid RELAYHUB_REDIS_URL")
	}
	defer func() {
		if err := redisClient.Close(); err != nil {
			logger.Warn("close Redis client", "error", err)
		}
	}()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	hub := realtime.NewHub()
	defer hub.Close()
	bridge, err := redisstore.NewBridge(ctx, redisClient, hub)
	if err != nil {
		return errors.New("start Redis notification bridge: Redis unavailable")
	}
	defer bridge.Close()
	if command == "worker" {
		callback := delivery.NewCallback(cfg.CallbackTimeout)
		runtime := worker.New(redisClient, callback, worker.Options{Concurrency: cfg.WorkerConcurrency, AttemptTimeout: cfg.CallbackTimeout, ReclaimIdle: cfg.WorkerReclaimIdle, ShutdownTimeout: cfg.ShutdownTimeout, Notifier: bridge, NotificationError: observability.NotificationFailed, Observe: observability.CallbackOutcome})
		logger.Info("RelayHub worker running", "concurrency", cfg.WorkerConcurrency)
		return runWorker(ctx, runtime, redisClient, cfg)
	}
	appService := service.NewAppService(redisClient, service.AppOptions{
		Now:                    time.Now,
		AllowInsecureCallbacks: cfg.AllowInsecureCallbacks,
	})
	eventService := service.NewEventService(redisClient, redisClient, service.EventOptions{
		Notifier: bridge, NotificationError: func(error) {
			observability.NotificationFailed()
			logger.Warn("Realtime notification failed; recover durable work through the queue")
		},
		Now: time.Now, Retention: store.EventRetention{Event: cfg.EventRetention, Job: cfg.JobRetention, Idempotency: cfg.IdempotencyRetention},
	})
	functionService := service.NewFunctionService(redisClient, service.FunctionOptions{Notifier: bridge, Observe: func(outcome string, elapsed time.Duration) {
		observability.FunctionOutcome(outcome, elapsed)
		logger.Info("Function operation", "outcome", outcome, "latency_ms", elapsed.Milliseconds())
	}})
	hub.SetFunctions(functionService)
	tokenIssuer := auth.NewTokenIssuer([]byte(cfg.SigningSecret), time.Now)

	handler := httpapi.NewRouter(httpapi.Dependencies{
		Health:   redisClient,
		Realtime: hub, AllowedOrigins: cfg.AllowedOrigins,
		Docs:        web.Public,
		Metrics:     observability.MetricsHandler(),
		Apps:        appService,
		Events:      eventService,
		Functions:   functionService,
		AdminToken:  cfg.AdminToken,
		TokenIssuer: tokenIssuer,
		Now:         time.Now,
		SigningSkew: cfg.SigningSkew,
	})
	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("RelayHub API listening", "address", cfg.HTTPAddr)
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case err := <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve HTTP: %w", err)
	case <-ctx.Done():
		hub.Close()
		bridge.Close()
		shutdownContext, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			_ = server.Close()
			return fmt.Errorf("shutdown HTTP server: %w", err)
		}
		if err := <-serverErrors; !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve HTTP during shutdown: %w", err)
		}
		return nil
	}
}

// The worker's operations listener is independent of the API listener and never
// registers application or documentation routes.
func runWorker(ctx context.Context, runtime *worker.Worker, health store.HealthChecker, cfg config.Config) error {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", observability.MetricsHandler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ping, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := health.Ping(ping); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	listener, err := net.Listen("tcp", cfg.WorkerHTTPAddr)
	if err != nil {
		return errors.New("start worker operations listener failed")
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	serverErrors := make(chan error, 1)
	go func() { err := server.Serve(listener); serverErrors <- err; cancel() }()
	runErr := runtime.Run(workerCtx)
	shutdown, stop := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer stop()
	if err := server.Shutdown(shutdown); err != nil {
		_ = server.Close()
		return errors.New("worker operations shutdown failed")
	}
	if err := <-serverErrors; !errors.Is(err, http.ErrServerClosed) {
		return errors.New("worker operations listener failed")
	}
	return runErr
}
