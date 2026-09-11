// Package httpapi exposes only bootstrap health and explicit unavailable routes.
package httpapi

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
)

type apiError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}
type errorResponse struct {
	Error     apiError `json:"error"`
	RequestID string   `json:"requestId"`
}

func NewHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var bytes [16]byte
		if _, err := rand.Read(bytes[:]); err != nil {
			panic("request ID entropy unavailable")
		}
		id := hex.EncodeToString(bytes[:])
		w.Header().Set("X-Request-ID", id)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		fail := func(status int, code, message string, retryable bool) {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(errorResponse{apiError{code, message, retryable}, id})
		}
		switch r.URL.Path {
		case "/healthz", "/readyz":
			if r.Method != http.MethodGet {
				w.Header().Set("Allow", "GET")
				fail(405, "METHOD_NOT_ALLOWED", "Use GET for health checks", false)
				return
			}
			if r.URL.Path == "/readyz" {
				fail(503, "NOT_READY", "Provider dependencies are not implemented", true)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		case "/api/v1/jobs", "/api/v1/workers/claim",
			"/api/v1/realtime/sessions", "/api/v1/realtime/grants",
			"/api/v1/realtime/publish", "/connection/websocket":
			fail(501, "NOT_IMPLEMENTED", "This capability is not available in the bootstrap server", false)
		default:
			fail(404, "NOT_FOUND", "Resource not found", false)
		}
	})
}
