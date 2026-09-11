package httpapi

import (
	"io/fs"
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
	Realtime       *realtime.Hub
	AllowedOrigins []string
	Health         store.HealthChecker
	Docs           fs.FS
	Metrics        http.Handler
	Apps           *service.AppService
	Events         *service.EventService
	Functions      *service.FunctionService
	AdminToken     string
	TokenIssuer    *auth.TokenIssuer
	Now            func() time.Time
	SigningSkew    time.Duration
}

func NewRouter(dependencies Dependencies) http.Handler {
	_ = mime.AddExtensionType(".md", "text/markdown; charset=utf-8")
	_ = mime.AddExtensionType(".zip", "application/zip")

	router := chi.NewRouter()
	router.Use(middleware.RequestID)
	router.Use(recoverJSON)
	router.Use(limitRequestBody)

	if dependencies.Realtime != nil && dependencies.Functions != nil {
		dependencies.Realtime.SetFunctions(dependencies.Functions)
	}
	router.Get("/healthz", healthHandler)
	if dependencies.Realtime != nil && dependencies.TokenIssuer != nil {
		router.Get("/ws", websocketHandler(dependencies))
	}
	router.Get("/readyz", readyHandler(dependencies.Health))
	router.Method(http.MethodGet, "/metrics", dependencies.Metrics)
	router.Get("/docs", func(response http.ResponseWriter, request *http.Request) {
		http.Redirect(response, request, "/docs/", http.StatusPermanentRedirect)
	})
	router.Method(http.MethodGet, "/docs/*", http.StripPrefix("/docs", docsHandler(dependencies.Docs)))

	if dependencies.Apps != nil {
		if dependencies.Now == nil {
			dependencies.Now = time.Now
		}
		if dependencies.SigningSkew == 0 {
			dependencies.SigningSkew = 5 * time.Minute
		}
		handlers := appHandlers{apps: dependencies.Apps, tokenIssuer: dependencies.TokenIssuer}
		admin := adminAuthentication(dependencies.AdminToken)
		signed := signedAuthentication(dependencies.Apps, dependencies.Now, dependencies.SigningSkew)
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
				api.With(signed).Get("/queue", events.queue)
				api.With(signed).Get("/events/{eventID}", events.getEvent)
				api.With(signed).Post("/events/{eventID}/ack", events.ack)
				api.With(signed).Get("/jobs/{jobID}", events.getJob)
				api.With(admin).Post("/jobs/{jobID}/requeue", events.requeue)
				api.With(admin).Post("/jobs/{jobID}/dead-letter", events.deadLetter)
			}
		})
	}

	router.NotFound(func(response http.ResponseWriter, _ *http.Request) {
		writeError(response, http.StatusNotFound, "not_found", "The requested resource was not found.")
	})
	router.MethodNotAllowed(func(response http.ResponseWriter, _ *http.Request) {
		writeError(response, http.StatusMethodNotAllowed, "method_not_allowed", "The requested method is not allowed.")
	})

	return router
}

func docsHandler(docs fs.FS) http.Handler {
	files := http.FileServerFS(docs)
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		name := strings.TrimPrefix(path.Clean(request.URL.Path), "/")
		if name == "" {
			name = "."
		}
		if _, err := fs.Stat(docs, name); err != nil {
			writeError(response, http.StatusNotFound, "not_found", "The requested resource was not found.")
			return
		}
		if path.Ext(name) == ".zip" {
			response.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": path.Base(name)}))
		}
		files.ServeHTTP(response, request)
	})
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
