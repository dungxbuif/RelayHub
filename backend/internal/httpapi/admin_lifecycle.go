package httpapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/go-chi/chi/v5"
)

type adminLifecycleHandlers struct {
	lifecycle *service.AdminLifecycleService
}

type adminReplayRequest struct {
	DeliveryIDs []string `json:"delivery_ids"`
}

func (handlers adminLifecycleHandlers) available(response http.ResponseWriter) bool {
	if handlers.lifecycle == nil {
		writeError(response, http.StatusServiceUnavailable, "unavailable", "Admin lifecycle operations are unavailable.")
		return false
	}
	return true
}

func (handlers adminLifecycleHandlers) timeline(response http.ResponseWriter, request *http.Request) {
	if !handlers.available(response) {
		return
	}
	if !onlyQueryKeys(request) {
		writeAdminLifecycleError(response, service.ErrInvalidInput)
		return
	}
	result, err := handlers.lifecycle.Timeline(request.Context(), chi.URLParam(request, "eventID"))
	if err != nil {
		writeAdminLifecycleError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, result)
}

func (handlers adminLifecycleHandlers) replayOne(response http.ResponseWriter, request *http.Request) {
	if !handlers.available(response) {
		return
	}
	body, err := io.ReadAll(request.Body)
	if err != nil || strings.TrimSpace(string(body)) != "" {
		writeAdminLifecycleError(response, service.ErrInvalidInput)
		return
	}
	handlers.replay(response, request, []string{chi.URLParam(request, "deliveryID")})
}

func (handlers adminLifecycleHandlers) replayBatch(response http.ResponseWriter, request *http.Request) {
	if !handlers.available(response) {
		return
	}
	var input adminReplayRequest
	if err := decodeJSON(request, &input); err != nil {
		writeAdminLifecycleError(response, service.ErrInvalidInput)
		return
	}
	handlers.replay(response, request, input.DeliveryIDs)
}

func (handlers adminLifecycleHandlers) replay(response http.ResponseWriter, request *http.Request, deliveryIDs []string) {
	if !onlyQueryKeys(request) {
		writeAdminLifecycleError(response, service.ErrInvalidInput)
		return
	}
	keys := request.Header.Values("Idempotency-Key")
	if len(keys) != 1 || strings.Contains(keys[0], ",") {
		writeAdminLifecycleError(response, service.ErrInvalidInput)
		return
	}
	result, replayed, err := handlers.lifecycle.Replay(request.Context(), deliveryIDs, keys[0], authenticatedAdminActor(request))
	if err != nil {
		writeAdminLifecycleError(response, err)
		return
	}
	if replayed {
		response.Header().Set("Idempotent-Replayed", "true")
	}
	response.Header().Set("Cache-Control", "no-store")
	writeJSON(response, http.StatusOK, result)
}

func writeAdminLifecycleError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, service.ErrInvalidInput):
		writeError(response, http.StatusBadRequest, "invalid_request", "The request is invalid.")
	case errors.Is(err, service.ErrNotFound):
		writeError(response, http.StatusNotFound, "not_found", "The requested resource was not found.")
	case errors.Is(err, service.ErrConflict):
		writeError(response, http.StatusConflict, "conflict", "The requested operation conflicts with the current resource state.")
	case errors.Is(err, context.DeadlineExceeded):
		writeError(response, http.StatusGatewayTimeout, "timeout", "The Admin operation timed out.")
	default:
		writeError(response, http.StatusInternalServerError, "internal_error", "An internal error occurred.")
	}
}
