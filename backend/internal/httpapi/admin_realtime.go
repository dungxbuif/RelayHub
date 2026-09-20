package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/dungxbuif/RelayHub/internal/realtime"
	"github.com/dungxbuif/RelayHub/internal/redisstate"
	"github.com/go-chi/chi/v5"
)

type realtimeAdmin interface {
	List(context.Context, string, int64) ([]redisstate.RealtimeConnection, error)
	Disconnect(context.Context, string, string) error
}

type adminRealtimeHandlers struct{ control realtimeAdmin }

func (handlers adminRealtimeHandlers) list(response http.ResponseWriter, request *http.Request) {
	appID := request.URL.Query().Get("app_id")
	limit := int64(100)
	if raw := request.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			writeError(response, http.StatusBadRequest, "invalid_request", "limit must be between 1 and 500.")
			return
		}
		limit = parsed
	}
	if handlers.control == nil || appID == "" || limit < 1 || limit > 500 {
		writeError(response, http.StatusBadRequest, "invalid_request", "app_id and a limit between 1 and 500 are required.")
		return
	}
	connections, err := handlers.control.List(request.Context(), appID, limit)
	if err != nil {
		writeError(response, http.StatusServiceUnavailable, "not_ready", "The realtime connection registry is unavailable.")
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"connections": connections})
}

func (handlers adminRealtimeHandlers) disconnect(response http.ResponseWriter, request *http.Request) {
	appID := request.URL.Query().Get("app_id")
	connectionID := chi.URLParam(request, "connectionID")
	if handlers.control == nil || appID == "" || connectionID == "" {
		writeError(response, http.StatusBadRequest, "invalid_request", "app_id and connection_id are required.")
		return
	}
	err := handlers.control.Disconnect(request.Context(), appID, connectionID)
	switch {
	case errors.Is(err, realtime.ErrConnectionNotFound):
		writeError(response, http.StatusNotFound, "not_found", "The realtime connection was not found.")
	case err != nil:
		writeError(response, http.StatusServiceUnavailable, "not_ready", "The realtime command route is unavailable.")
	default:
		response.WriteHeader(http.StatusNoContent)
	}
}
