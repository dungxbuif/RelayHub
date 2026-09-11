package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dungxbuif/RelayHub/internal/auth"
	"github.com/dungxbuif/RelayHub/internal/config"
	"github.com/dungxbuif/RelayHub/internal/httpapi"
	"github.com/dungxbuif/RelayHub/internal/observability"
	"github.com/dungxbuif/RelayHub/internal/realtime"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/dungxbuif/RelayHub/internal/store/redisstore"
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
	tokenIssuer := auth.NewTokenIssuer([]byte(cfg.SigningSecret), time.Now)

	handler := httpapi.NewRouter(httpapi.Dependencies{
		Health:   redisClient,
		Realtime: hub, AllowedOrigins: cfg.AllowedOrigins,
		Docs:        web.Public,
		Metrics:     observability.MetricsHandler(),
		Apps:        appService,
		Events:      eventService,
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
