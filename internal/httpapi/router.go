package httpapi

import (
	"io/fs"
	"mime"
	"net/http"

	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

const maxRequestBodyBytes int64 = 1 << 20

type Dependencies struct {
	Health  store.HealthChecker
	Docs    fs.FS
	Metrics http.Handler
}

func NewRouter(dependencies Dependencies) http.Handler {
	_ = mime.AddExtensionType(".md", "text/markdown; charset=utf-8")

	router := chi.NewRouter()
	router.Use(middleware.RequestID)
	router.Use(recoverJSON)
	router.Use(limitRequestBody)

	router.Get("/healthz", healthHandler)
	router.Get("/readyz", readyHandler(dependencies.Health))
	router.Method(http.MethodGet, "/metrics", dependencies.Metrics)
	router.Get("/docs", func(response http.ResponseWriter, request *http.Request) {
		http.Redirect(response, request, "/docs/", http.StatusPermanentRedirect)
	})
	router.Handle("/docs/*", http.StripPrefix("/docs", http.FileServerFS(dependencies.Docs)))

	router.NotFound(func(response http.ResponseWriter, _ *http.Request) {
		writeError(response, http.StatusNotFound, "not_found", "The requested resource was not found.")
	})
	router.MethodNotAllowed(func(response http.ResponseWriter, _ *http.Request) {
		writeError(response, http.StatusMethodNotAllowed, "method_not_allowed", "The requested method is not allowed.")
	})

	return router
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
