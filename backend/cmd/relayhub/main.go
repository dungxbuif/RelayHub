package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dungxbuif/RelayHub/internal/auth"
	"github.com/dungxbuif/RelayHub/internal/broker"
	natsbroker "github.com/dungxbuif/RelayHub/internal/broker/nats"
	"github.com/dungxbuif/RelayHub/internal/config"
	secretcrypto "github.com/dungxbuif/RelayHub/internal/crypto"
	"github.com/dungxbuif/RelayHub/internal/delivery"
	"github.com/dungxbuif/RelayHub/internal/httpapi"
	"github.com/dungxbuif/RelayHub/internal/observability"
	"github.com/dungxbuif/RelayHub/internal/outbox"
	"github.com/dungxbuif/RelayHub/internal/realtime"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/dungxbuif/RelayHub/internal/store"
	postgresstore "github.com/dungxbuif/RelayHub/internal/store/postgres"
	"github.com/dungxbuif/RelayHub/internal/streamgateway"
	"github.com/dungxbuif/RelayHub/internal/worker"
	"github.com/dungxbuif/RelayHub/web"
	"github.com/google/uuid"
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
	if command == "healthcheck" && len(os.Args) == 3 {
		return healthcheck(os.Args[2])
	}
	if len(os.Args) > 2 || (command != "api" && command != "worker") {
		return errors.New("usage: relayhub [api|worker] or relayhub healthcheck http://127.0.0.1:PORT/readyz")
	}

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	natsClient, err := natsbroker.Connect(natsbroker.Options{
		URL: cfg.NATSURL, Name: "relayhub-" + command,
		Username: cfg.NATSUsername, Password: cfg.NATSPassword,
		ConnectTimeout: cfg.NATSConnectTimeout, ReconnectWait: cfg.NATSReconnectWait,
		MaxReconnects: cfg.NATSMaxReconnects, DrainTimeout: cfg.NATSDrainTimeout,
		Hooks: natsbroker.Hooks{
			Disconnected: func() { observability.NATSConnected(false); observability.NATSEvent("disconnected") },
			Reconnected:  func() { observability.NATSConnected(true); observability.NATSEvent("reconnected") },
			SlowConsumer: func() { observability.NATSEvent("slow_consumer") },
			AsyncError:   func() { observability.NATSEvent("async_error") },
			Drained:      func() { observability.NATSConnected(false); observability.NATSEvent("drained") },
		},
	})
	if err != nil {
		return errors.New("connect NATS: NATS unavailable")
	}
	observability.NATSConnected(true)
	defer shutdownNATS(natsClient, logger)
	bootstrapCtx, cancelBootstrap := context.WithTimeout(ctx, max(cfg.NATSConnectTimeout, 5*time.Second))
	err = natsClient.Bootstrap(bootstrapCtx, natsbroker.StreamSettings{MaxAge: cfg.NATSStreamMaxAge, DuplicateWindow: cfg.NATSDuplicateWindow, Replicas: cfg.NATSReplicas})
	cancelBootstrap()
	if err != nil {
		observability.NATSEvent("bootstrap_error")
		return fmt.Errorf("bootstrap NATS: %w", err)
	}

	return runPostgresRuntime(ctx, command, cfg, natsClient, logger)
}

func runPostgresRuntime(ctx context.Context, command string, cfg config.Config, natsClient *natsbroker.Client, logger *slog.Logger) error {
	cipher, err := secretcrypto.NewSecretCipher(cfg.SecretEncryptionKey)
	if err != nil {
		return errors.New("configure PostgreSQL secret encryption")
	}
	postgresClient, err := postgresstore.NewClient(ctx, postgresstore.Config{DatabaseURL: cfg.PostgresURL, MaxConnections: 10, MinConnections: 1}, cipher)
	if err != nil {
		return errors.New("connect PostgreSQL: PostgreSQL unavailable")
	}
	defer postgresClient.Close()
	if err := postgresClient.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate PostgreSQL: %w", err)
	}
	health := broker.CompositeHealth{natsClient, postgresClient}
	hub := realtime.NewHub()
	defer hub.Close()
	bridge, err := realtime.NewNATSBridge(ctx, natsClient.Conn(), hub, "relayhub-"+command+"-"+uuid.NewString())
	if err != nil {
		return errors.New("start NATS realtime bridge: NATS unavailable")
	}
	defer bridge.Close()
	if command == "worker" {
		dispatcher, err := outbox.NewDispatcher(postgresClient, natsClient, outbox.Options{
			BatchSize: 100, ClaimTTL: cfg.WorkerReclaimIdle, BaseRetry: time.Second, MaxRetry: 30 * time.Second, MaxAttempts: 10, MaxPendingAge: 5 * time.Minute,
		})
		if err != nil {
			return fmt.Errorf("configure outbox dispatcher: %w", err)
		}
		health = append(health, dispatcher)
		callback := delivery.NewCallback(cfg.CallbackTimeout)
		callbackWorker := worker.NewJetStream(postgresClient, natsClient, natsClient, callback, worker.JetStreamOptions{
			Logger: logger, Concurrency: cfg.WorkerConcurrency, AttemptTimeout: cfg.CallbackTimeout, LeaseDuration: cfg.WorkerReclaimIdle, ShutdownTimeout: cfg.ShutdownTimeout, Observe: observability.CallbackOutcome,
		})
		logger.Info("RelayHub worker running", "mode", "postgres-nats", "concurrency", cfg.WorkerConcurrency)
		return runWorker(ctx, combinedWorker{callbacks: callbackWorker, dispatcher: dispatcher, outboxInterval: 500 * time.Millisecond}, health, cfg)
	}
	durableStream, err := streamgateway.New(streamgateway.Options{Consumer: natsClient, Assignments: postgresClient, NewID: func(prefix string) (string, error) { return prefix + uuid.NewString(), nil }})
	if err != nil {
		return fmt.Errorf("configure durable stream: %w", err)
	}
	defer drainStream(durableStream, cfg.ShutdownTimeout, logger)
	routingService := service.NewRoutingService(postgresClient, postgresClient, service.RoutingOptions{})
	appService := service.NewAppService(postgresClient, service.AppOptions{Now: time.Now, AllowInsecureCallbacks: cfg.AllowInsecureCallbacks})
	eventService, err := service.NewEventServiceWithStores(postgresClient, postgresClient, nil, postgresClient, service.EventOptions{
		Notifier: bridge, Router: routingService, Realtime: bridge, NotificationError: func(error) {
			observability.NotificationFailed()
			logger.Warn("Realtime notification failed; recover durable work through NATS and PostgreSQL")
		},
		Now: time.Now, Retention: store.EventRetention{Event: cfg.EventRetention, Job: cfg.JobRetention, Idempotency: cfg.IdempotencyRetention},
	})
	if err != nil {
		return fmt.Errorf("configure event service: %w", err)
	}
	functionService := service.NewFunctionService(postgresClient, service.FunctionOptions{Notifier: bridge, Observe: func(outcome string, elapsed time.Duration) {
		observability.FunctionOutcome(outcome, elapsed)
		logger.Info("Function operation", "outcome", outcome, "latency_ms", elapsed.Milliseconds())
	}})
	hub.SetFunctions(functionService)
	return serveAPI(ctx, logger, cfg, health, hub, bridge, appService, eventService, functionService, routingService, durableStream)
}

func serveAPI(ctx context.Context, logger *slog.Logger, cfg config.Config, health store.HealthChecker, hub *realtime.Hub, realtimePub service.RealtimePublisher, appService *service.AppService, eventService *service.EventService, functionService *service.FunctionService, routingService *service.RoutingService, durableStream *streamgateway.Gateway) error {
	tokenIssuer := auth.NewTokenIssuer([]byte(cfg.SigningSecret), time.Now)
	handler := httpapi.NewRouter(httpapi.Dependencies{
		Logger: logger, Health: health, Realtime: hub, RealtimePub: realtimePub, AllowedOrigins: cfg.AllowedOrigins,
		Admin: web.Admin, Metrics: observability.MetricsHandler(), Apps: appService, Events: eventService, Functions: functionService, Routing: routingService,
		AdminToken: cfg.AdminToken, TokenIssuer: tokenIssuer, Stream: durableStream, Now: time.Now, SigningSkew: cfg.SigningSkew,
	})
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: handler, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
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
		if durableStream != nil {
			drainStream(durableStream, cfg.ShutdownTimeout, logger)
		}
		hub.Close()
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

func drainStream(durableStream *streamgateway.Gateway, timeout time.Duration, logger *slog.Logger) {
	drainCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if drainErr := durableStream.Drain(drainCtx); drainErr != nil {
		logger.Warn("drain durable stream", "error", drainErr)
	}
}

type combinedWorker struct {
	callbacks      *worker.JetStreamWorker
	dispatcher     *outbox.Dispatcher
	outboxInterval time.Duration
}

func (worker combinedWorker) Run(ctx context.Context) error {
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errorsCh := make(chan error, 2)
	go func() { errorsCh <- worker.callbacks.Run(workerCtx) }()
	go func() { errorsCh <- worker.dispatcher.Run(workerCtx, worker.outboxInterval) }()
	for completed := 0; completed < 2; completed++ {
		err := <-errorsCh
		if ctx.Err() != nil {
			cancel()
			continue
		}
		if err != nil {
			cancel()
			return err
		}
	}
	return nil
}

type natsShutdown interface {
	Drain() error
	Close()
}

func shutdownNATS(client natsShutdown, logger *slog.Logger) {
	if err := client.Drain(); err != nil {
		logger.Warn("drain NATS client", "error", err)
	}
	client.Close()
}

// The worker's operations listener is independent of the API listener and never
// registers application or documentation routes.
type runtimeWorker interface {
	Run(context.Context) error
}

func runWorker(ctx context.Context, runtime runtimeWorker, health store.HealthChecker, cfg config.Config) error {
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

// healthcheck is self-contained so distroless containers need no shell or curl.
// Errors deliberately omit the URL and response body, which may contain secrets.
func healthcheck(target string) error {
	u, err := url.Parse(target)
	if err != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("invalid healthcheck target")
	}
	host := u.Hostname()
	if host != "localhost" && (net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback()) {
		return errors.New("healthcheck target must be loopback")
	}
	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Get(target)
	if err != nil {
		return errors.New("healthcheck request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return errors.New("healthcheck endpoint is not ready")
	}
	return nil
}
