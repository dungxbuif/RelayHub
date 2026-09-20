package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
)

// requestLog records only generated identity and bounded protocol metadata.
// Client-supplied request IDs, raw targets, headers and bodies are never fields.
func requestLog(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			started := time.Now()
			id := uuid.NewString()
			state := &requestLogState{logger: logger.With("request_id", id)}
			r = r.WithContext(context.WithValue(r.Context(), requestLogKey{}, state))
			response := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(response, r)
			status := response.Status()
			route := "unmatched"
			if ctx := chi.RouteContext(r.Context()); ctx != nil && ctx.RoutePattern() != "" {
				route = ctx.RoutePattern()
			}
			if status == 0 {
				status = http.StatusOK
				// Gorilla writes 101 directly to its hijacked connection. Every rejected
				// /ws handshake writes an HTTP status through the wrapper before returning.
				if route == "/ws" {
					status = http.StatusSwitchingProtocols
				}
			}
			method := r.Method
			switch method {
			case "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "CONNECT", "TRACE":
			default:
				method = "OTHER"
			}
			outcome := "success"
			if status >= 500 {
				outcome = "server_error"
			} else if status >= 400 {
				outcome = "client_error"
			}
			requestLogger := state.logger
			if state.appID != "" {
				requestLogger = requestLogger.With("app_id", state.appID)
			}
			requestLogger.Info("HTTP request", "method", method, "route", route, "status", status, "latency_ms", time.Since(started).Milliseconds(), "outcome", outcome)
		})
	}
}

type requestLogKey struct{}
type requestLogState struct {
	logger *slog.Logger
	appID  string
}

// Runs after successful HMAC authentication and before application handlers.
func authenticatedLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if state, ok := r.Context().Value(requestLogKey{}).(*requestLogState); ok {
			state.appID = authenticatedApp(r).App.ID
		}
		next.ServeHTTP(w, r)
	})
}

// This deliberately has no arbitrary attribute map, URL, name, body, or error.
type operationFields struct {
	AppID, EventID, JobID, FunctionID, InvocationID, Outcome string
	Attempt                                                  int
}

func logOperation(r *http.Request, fields operationFields) {
	state, ok := r.Context().Value(requestLogKey{}).(*requestLogState)
	if !ok {
		return
	}
	if fields.AppID == "" {
		fields.AppID = state.appID
	}
	attrs := []any{"outcome", fields.Outcome}
	for _, field := range []struct{ key, value string }{{"app_id", fields.AppID}, {"event_id", fields.EventID}, {"job_id", fields.JobID}, {"function_id", fields.FunctionID}, {"invocation_id", fields.InvocationID}} {
		if field.value != "" {
			attrs = append(attrs, field.key, field.value)
		}
	}
	if fields.Attempt > 0 {
		attrs = append(attrs, "attempt", fields.Attempt)
	}
	state.logger.Info("Application operation", attrs...)
}
