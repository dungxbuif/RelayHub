package httpapi

import (
	"errors"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/go-chi/chi/v5"
	"net/http"
	"strconv"
	"time"
)

type eventHandlers struct{ events *service.EventService }

func (h eventHandlers) publish(w http.ResponseWriter, r *http.Request) {
	var input service.PublishEvent
	if err := decodeJSON(r, &input); err != nil {
		writeEventError(w, service.ErrInvalidInput)
		return
	}
	e, j, replay, err := h.events.Publish(r.Context(), authenticatedApp(r).App.ID, input, r.Header.Get("Idempotency-Key"))
	if err != nil {
		writeEventError(w, err)
		return
	}
	if replay {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	writeJSON(w, http.StatusAccepted, store.Publication{Event: e, Jobs: j})
}
func (h eventHandlers) queue(w http.ResponseWriter, r *http.Request) {
	limit, wait := 20, 0
	for key, target := range map[string]*int{"limit": &limit, "wait": &wait} {
		if values, ok := r.URL.Query()[key]; ok {
			if len(values) != 1 {
				writeEventError(w, service.ErrInvalidInput)
				return
			}
			value, err := strconv.Atoi(values[0])
			if err != nil {
				writeEventError(w, service.ErrInvalidInput)
				return
			}
			*target = value
		}
	}
	// Bound before duration conversion to avoid overflow accepting a huge wait.
	if wait < 0 || wait > 30 {
		writeEventError(w, service.ErrInvalidInput)
		return
	}
	items, err := h.events.Lease(r.Context(), authenticatedApp(r).App.ID, limit, time.Duration(wait)*time.Second)
	if err != nil {
		writeEventError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, items)
}
func (h eventHandlers) ack(w http.ResponseWriter, r *http.Request) {
	if err := h.events.Ack(r.Context(), authenticatedApp(r).App.ID, chi.URLParam(r, "eventID")); err != nil {
		writeEventError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (h eventHandlers) getEvent(w http.ResponseWriter, r *http.Request) {
	event, err := h.events.GetEvent(r.Context(), authenticatedApp(r).App.ID, chi.URLParam(r, "eventID"))
	if err != nil {
		writeEventError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, event)
}
func (h eventHandlers) getJob(w http.ResponseWriter, r *http.Request) {
	job, err := h.events.GetJob(r.Context(), authenticatedApp(r).App.ID, chi.URLParam(r, "jobID"))
	if err != nil {
		writeEventError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}
func (h eventHandlers) requeue(w http.ResponseWriter, r *http.Request) {
	job, err := h.events.RequeueJob(r.Context(), chi.URLParam(r, "jobID"))
	writeJob(w, job, err)
}
func (h eventHandlers) deadLetter(w http.ResponseWriter, r *http.Request) {
	job, err := h.events.DeadLetterJob(r.Context(), chi.URLParam(r, "jobID"))
	writeJob(w, job, err)
}
func writeJob(w http.ResponseWriter, job domain.Job, err error) {
	if err != nil {
		writeEventError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}
func writeEventError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, service.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "The requested resource was not found.")
	case errors.Is(err, service.ErrConflict):
		writeError(w, http.StatusConflict, "conflict", "The job cannot make this state transition.")
	default:
		writeServiceError(w, err)
	}
}
