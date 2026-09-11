package httpapi

import (
	"net/http"
	"time"

	"github.com/dungxbuif/RelayHub/internal/observability"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func requestMetrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		response := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(response, r)
		route := "unmatched"
		if ctx := chi.RouteContext(r.Context()); ctx != nil && ctx.RoutePattern() != "" {
			route = ctx.RoutePattern()
		}
		status := response.Status()
		if status == 0 {
			status = http.StatusOK
			// Successful Gorilla upgrades write 101 on the hijacked connection.
			if route == "/ws" {
				status = http.StatusSwitchingProtocols
			}
		}
		observability.HTTPRequest(r.Method, route, status, time.Since(started))
	})
}
