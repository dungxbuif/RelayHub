package httpapi

import (
	"errors"
	"net/http"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/go-chi/chi/v5"
)

type realtimePushHandlers struct{ push *service.RealtimePushService }

func (h realtimePushHandlers) register(w http.ResponseWriter, r *http.Request) {
	if h.push == nil {
		writeError(w, 503, "push_unavailable", "Push is not configured.")
		return
	}
	var input service.RegisterPushDeviceInput
	if decodeJSON(r, &input) != nil {
		writePushError(w, service.ErrInvalidInput)
		return
	}
	device, err := h.push.Register(r.Context(), authenticatedApp(r).App.ID, input)
	if err != nil {
		writePushError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, device)
}
func (h realtimePushHandlers) delete(w http.ResponseWriter, r *http.Request) {
	if h.push == nil {
		writeError(w, 503, "push_unavailable", "Push is not configured.")
		return
	}
	if err := h.push.Delete(r.Context(), authenticatedApp(r).App.ID, chi.URLParam(r, "deviceID")); err != nil {
		writePushError(w, err)
		return
	}
	w.WriteHeader(204)
}
func (h realtimePushHandlers) bind(bind bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.push == nil {
			writeError(w, 503, "push_unavailable", "Push is not configured.")
			return
		}
		err := h.push.Bind(r.Context(), authenticatedApp(r).App.ID, chi.URLParam(r, "channel"), chi.URLParam(r, "deviceID"), bind)
		if err != nil {
			writePushError(w, err)
			return
		}
		w.WriteHeader(204)
	}
}
func (h realtimePushHandlers) publish(w http.ResponseWriter, r *http.Request) {
	if h.push == nil {
		writeError(w, 503, "push_unavailable", "Push is not configured.")
		return
	}
	var input domain.PushNotification
	if decodeJSON(r, &input) != nil {
		writePushError(w, service.ErrInvalidInput)
		return
	}
	outcomes, err := h.push.Publish(r.Context(), authenticatedApp(r).App.ID, chi.URLParam(r, "channel"), input)
	if err != nil {
		writePushError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"outcomes": outcomes})
}
func writePushError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, service.ErrInvalidInput):
		writeError(w, 400, "invalid_request", "The push request is invalid.")
	case errors.Is(err, service.ErrNotFound):
		writeError(w, 404, "device_not_found", "The push device was not found.")
	case errors.Is(err, service.ErrConflict):
		writeError(w, 409, "conflict", "The push request conflicts with current state.")
	case errors.Is(err, service.ErrInvalidDependency):
		writeError(w, 503, "push_unavailable", "Push is temporarily unavailable.")
	default:
		writeError(w, 500, "internal_error", "An internal error occurred.")
	}
}
