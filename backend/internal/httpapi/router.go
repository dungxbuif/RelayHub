package httpapi

import (
	"bytes"
	"io/fs"
	"log/slog"
	"mime"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/dungxbuif/RelayHub/internal/auth"
	"github.com/dungxbuif/RelayHub/internal/realtime"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

const maxRequestBodyBytes int64 = 1 << 20

type Dependencies struct {
	Logger          *slog.Logger
	Realtime        *realtime.Hub
	RealtimePub     realtimePublisher
	AllowedOrigins  []string
	Health          store.HealthChecker
	Admin           fs.FS
	Metrics         http.Handler
	Apps            *service.AppService
	Events          *service.EventService
	Functions       *service.FunctionService
	Routing         *service.RoutingService
	Queue           *service.QueueService
	Files           *service.RealtimeFileService
	Push            *service.RealtimePushService
	AdminToken      string
	AdminSessions   *service.AdminSessionService
	AdminReads      *service.AdminReadService
	AdminLifecycle  *service.AdminLifecycleService
	RealtimeControl realtimeAdmin
	TokenIssuer     *auth.TokenIssuer
	Stream          StreamServer
	Now             func() time.Time
	SigningSkew     time.Duration
}

func NewRouter(dependencies Dependencies) http.Handler {
	_ = mime.AddExtensionType(".md", "text/markdown; charset=utf-8")
	_ = mime.AddExtensionType(".yaml", "application/yaml; charset=utf-8")
	_ = mime.AddExtensionType(".zip", "application/zip")

	router := chi.NewRouter()
	router.Use(middleware.RequestID)
	router.Use(requestMetrics)
	if dependencies.Logger != nil {
		router.Use(requestLog(dependencies.Logger))
	}
	router.Use(recoverJSON)
	router.Use(cors(dependencies.AllowedOrigins))
	router.Use(limitRequestBody)

	if dependencies.Realtime != nil && dependencies.Functions != nil {
		dependencies.Realtime.SetFunctions(dependencies.Functions)
	}
	router.Get("/healthz", healthHandler)
	if dependencies.Realtime != nil && dependencies.TokenIssuer != nil {
		router.Get("/ws", websocketHandler(dependencies))
		router.Get("/api/v2/realtime/channels/{channel}/history", realtimeHistoryHandler(dependencies))
	}
	if dependencies.TokenIssuer != nil {
		router.Get("/api/v1/stream", streamHandler(dependencies))
	}
	router.Get("/readyz", readyHandler(dependencies.Health))
	router.Method(http.MethodGet, "/metrics", dependencies.Metrics)
	router.Get("/admin", func(response http.ResponseWriter, request *http.Request) {
		http.Redirect(response, request, "/admin/", http.StatusPermanentRedirect)
	})
	router.Method(http.MethodGet, "/admin/*", http.StripPrefix("/admin", adminHandler(dependencies.Admin)))
	adminSessions := adminSessionHandlers{sessions: dependencies.AdminSessions}
	admin := adminAuthentication(dependencies.AdminToken, dependencies.AdminSessions)
	router.Post("/api/v1/admin/session", adminSessions.exchange)
	router.With(admin).Delete("/api/v1/admin/session", adminSessions.logout)
	reads := adminReadHandlers{reads: dependencies.AdminReads}
	lifecycle := adminLifecycleHandlers{lifecycle: dependencies.AdminLifecycle}
	control := adminControlHandlers{apps: dependencies.Apps, issuer: dependencies.TokenIssuer, publisher: dependencies.RealtimePub}
	realtimeControl := adminRealtimeHandlers{control: dependencies.RealtimeControl}
	adminQueue := adminQueueHandlers{queue: dependencies.Queue}
	if control.publisher == nil {
		control.publisher = dependencies.Realtime
	}
	router.Route("/api/v1/admin", func(api chi.Router) {
		api.Use(admin)
		api.Get("/dashboard", reads.dashboard)
		api.Get("/metrics", reads.metrics)
		api.Get("/events", reads.events)
		api.Get("/events/{eventID}", reads.event)
		api.Get("/events/{eventID}/timeline", lifecycle.timeline)
		api.Get("/dlq", reads.deadLetters)
		api.Post("/dlq/replay", lifecycle.replayBatch)
		api.Post("/dlq/{deliveryID}/replay", lifecycle.replayOne)
		api.Get("/dlq/{deliveryID}", reads.deadLetter)
		api.Get("/audit", reads.audit)
		api.Patch("/apps/{appID}", control.updateApp)
		api.Post("/studio/token", control.studioToken)
		api.Post("/studio/publish", control.studioPublish)
		api.Get("/connections", realtimeControl.list)
		api.Delete("/connections/{connectionID}", realtimeControl.disconnect)
		if dependencies.Queue != nil {
			api.Get("/apps/{appID}/subscriptions", adminQueue.subscriptions)
			api.Get("/apps/{appID}/subscriptions/{subscriptionID}/metrics", adminQueue.metrics)
			api.Get("/apps/{appID}/subscriptions/{subscriptionID}/schedules", adminQueue.schedules)
			api.Get("/apps/{appID}/subscriptions/{subscriptionID}/callbacks", adminQueue.callbacks)
			api.Post("/apps/{appID}/subscriptions/{subscriptionID}/drain", adminQueue.drain)
		}
	})

	if dependencies.Apps != nil {
		if dependencies.Now == nil {
			dependencies.Now = time.Now
		}
		if dependencies.SigningSkew == 0 {
			dependencies.SigningSkew = 5 * time.Minute
		}
		handlers := appHandlers{apps: dependencies.Apps, tokenIssuer: dependencies.TokenIssuer}
		signed := func(next http.Handler) http.Handler {
			return signedAuthentication(dependencies.Apps, dependencies.Now, dependencies.SigningSkew)(authenticatedLog(next))
		}
		router.Route("/api/v1", func(api chi.Router) {
			api.With(admin).Post("/apps", handlers.create)
			api.With(admin).Get("/apps", handlers.list)
			api.With(signed).Get("/apps/{appID}", handlers.get)
			api.With(signed).Patch("/apps/{appID}", handlers.update)
			api.With(admin).Delete("/apps/{appID}", handlers.disable)
			api.With(admin).Post("/apps/{appID}/rotate-secret", handlers.rotate)
			api.With(signed).Post("/socket/token", handlers.socketToken)
			if dependencies.Functions != nil {
				functions := functionHandlers{functions: dependencies.Functions}
				api.With(signed).Post("/functions", functions.register)
				api.With(signed).Get("/functions", functions.list)
				api.With(signed).Delete("/functions/{functionID}", functions.delete)
				api.With(signed).Post("/functions/{functionID}/invoke", functions.invoke)
			}
			if dependencies.Events != nil {
				events := eventHandlers{events: dependencies.Events}
				api.With(signed).Post("/events", events.publish)
				api.With(signed).Get("/events/{eventID}", events.getEvent)
				api.With(signed).Get("/jobs/{jobID}", events.getJob)
			}
			if dependencies.Routing != nil {
				routing := routingHandlers{routing: dependencies.Routing}
				api.With(admin).Post("/routing/rules", routing.create)
				api.With(admin).Get("/routing/rules", routing.list)
				api.With(admin).Patch("/routing/rules/{ruleID}", routing.update)
				api.With(admin).Delete("/routing/rules/{ruleID}", routing.delete)
			}
			if dependencies.Realtime != nil {
				realtimePublisher := dependencies.RealtimePub
				if realtimePublisher == nil {
					realtimePublisher = dependencies.Realtime
				}
				realtime := realtimeHandlers{publisher: realtimePublisher}
				api.With(signed).Post("/realtime/channels/{channel}/publish", realtime.publish)
			}
		})
		files := realtimeFileHandlers{files: dependencies.Files}
		router.Group(func(api chi.Router) {
			api.Use(signed)
			api.Post("/api/v2/realtime/files", files.create)
			api.Post("/api/v2/realtime/files/{fileID}/complete", files.complete)
			api.Get("/api/v2/realtime/files/{fileID}/download", files.download)
		})
		push := realtimePushHandlers{push: dependencies.Push}
		router.Group(func(api chi.Router) {
			api.Use(signed)
			api.Post("/api/v2/realtime/push/devices", push.register)
			api.Delete("/api/v2/realtime/push/devices/{deviceID}", push.delete)
			api.Put("/api/v2/realtime/push/channels/{channel}/devices/{deviceID}", push.bind(true))
			api.Delete("/api/v2/realtime/push/channels/{channel}/devices/{deviceID}", push.bind(false))
			api.Post("/api/v2/realtime/push/channels/{channel}/notifications", push.publish)
		})
		if dependencies.Queue != nil {
			queue := queueHandlers{queue: dependencies.Queue}
			router.Route("/api/v2", func(api chi.Router) {
				api.Use(signed)
				api.Post("/subscriptions", queue.create)
				api.Get("/subscriptions", queue.list)
				api.Get("/subscriptions/{subscriptionID}", queue.get)
				api.Put("/subscriptions/{subscriptionID}", queue.update)
				api.Delete("/subscriptions/{subscriptionID}", queue.delete)
				api.Post("/subscriptions/{subscriptionID}/pause", queue.pause(true))
				api.Post("/subscriptions/{subscriptionID}/resume", queue.pause(false))
				api.Post("/subscriptions/{subscriptionID}/pull", queue.pull)
				api.Post("/subscriptions/{subscriptionID}/settle", queue.settle)
				api.Post("/subscriptions/{subscriptionID}/leases/extend", queue.extend)
				api.Get("/subscriptions/{subscriptionID}/metrics", queue.depth)
				api.Post("/subscriptions/{subscriptionID}/drain", queue.beginDrain)
				api.Get("/subscriptions/{subscriptionID}/drain", queue.drainStatus)
				api.Post("/subscriptions/{subscriptionID}/schedules", queue.createSchedule)
				api.Get("/subscriptions/{subscriptionID}/schedules", queue.listSchedules)
				api.Get("/subscriptions/{subscriptionID}/schedules/{scheduleID}", queue.getSchedule)
				api.Put("/subscriptions/{subscriptionID}/schedules/{scheduleID}", queue.updateSchedule)
				api.Delete("/subscriptions/{subscriptionID}/schedules/{scheduleID}", queue.deleteSchedule)
				api.Get("/subscriptions/{subscriptionID}/dead-letters", queue.deadLetters)
				api.Get("/subscriptions/{subscriptionID}/dead-letters/export", queue.exportDeadLetters)
				api.Post("/subscriptions/{subscriptionID}/dead-letters/replay", queue.replayDeadLetters)
				api.Post("/subscriptions/{subscriptionID}/dead-letters/delete", queue.deleteDeadLetters)
			})
		}
	}

	router.NotFound(func(response http.ResponseWriter, _ *http.Request) {
		writeError(response, http.StatusNotFound, "not_found", "The requested resource was not found.")
	})
	router.MethodNotAllowed(func(response http.ResponseWriter, _ *http.Request) {
		writeError(response, http.StatusMethodNotAllowed, "method_not_allowed", "The requested method is not allowed.")
	})

	return router
}

func adminHandler(admin fs.FS) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		name := strings.TrimPrefix(path.Clean(request.URL.Path), "/")
		if name == "" || name == "." {
			name = "index.html"
		}
		entry, err := fs.Stat(admin, name)
		if err != nil || entry.IsDir() {
			if !adminSPAFallback(request, name) {
				writeError(response, http.StatusNotFound, "not_found", "The requested resource was not found.")
				return
			}
			name = "index.html"
		}
		serveAdminFile(response, request, admin, name)
	})
}

func adminSPAFallback(request *http.Request, name string) bool {
	return (request.Method == http.MethodGet || request.Method == http.MethodHead) &&
		!strings.HasPrefix(name, "assets/") && path.Ext(name) == "" &&
		strings.Contains(request.Header.Get("Accept"), "text/html")
}

func serveAdminFile(response http.ResponseWriter, request *http.Request, admin fs.FS, name string) {
	data, err := fs.ReadFile(admin, name)
	if err != nil {
		writeError(response, http.StatusNotFound, "not_found", "The requested resource was not found.")
		return
	}
	entry, err := fs.Stat(admin, name)
	if err != nil {
		writeError(response, http.StatusNotFound, "not_found", "The requested resource was not found.")
		return
	}
	http.ServeContent(response, request, name, entry.ModTime(), bytes.NewReader(data))
}

func limitRequestBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.ContentLength > maxRequestBodyBytes {
			writeError(response, http.StatusRequestEntityTooLarge, "request_too_large", "Request body exceeds the 1 MiB limit.")
			return
		}
		request.Body = http.MaxBytesReader(response, request.Body, maxRequestBodyBytes)
		next.ServeHTTP(response, request)
	})
}

func recoverJSON(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				writeError(response, http.StatusInternalServerError, "internal_error", "An internal error occurred.")
			}
		}()
		next.ServeHTTP(response, request)
	})
}
