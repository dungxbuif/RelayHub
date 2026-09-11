package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/dungxbuif/RelayHub/internal/store"
)

type statusResponse struct {
	Status string `json:"status"`
}

type errorEnvelope struct {
	Error errorResponse `json:"error"`
}

type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func healthHandler(response http.ResponseWriter, _ *http.Request) {
	writeJSON(response, http.StatusOK, statusResponse{Status: "ok"})
}

func readyHandler(health store.HealthChecker) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		if health == nil || health.Ping(request.Context()) != nil {
			writeError(response, http.StatusServiceUnavailable, "not_ready", "Redis is unavailable.")
			return
		}
		writeJSON(response, http.StatusOK, statusResponse{Status: "ok"})
	}
}

func writeError(response http.ResponseWriter, status int, code, message string) {
	writeJSON(response, status, errorEnvelope{Error: errorResponse{Code: code, Message: message}})
}

func writeJSON(response http.ResponseWriter, status int, payload any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(payload)
}
