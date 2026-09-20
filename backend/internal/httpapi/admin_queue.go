package httpapi

import (
	"net/http"

	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/go-chi/chi/v5"
)

type adminQueueHandlers struct{ queue *service.QueueService }

func (handlers adminQueueHandlers) subscriptions(response http.ResponseWriter, request *http.Request) {
	items, err := handlers.queue.List(request.Context(), chi.URLParam(request, "appID"))
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"items": items})
}
func (handlers adminQueueHandlers) schedules(response http.ResponseWriter, request *http.Request) {
	items, err := handlers.queue.ListSchedules(request.Context(), chi.URLParam(request, "appID"), chi.URLParam(request, "subscriptionID"))
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"items": items})
}
func (handlers adminQueueHandlers) metrics(response http.ResponseWriter, request *http.Request) {
	item, err := handlers.queue.Depth(request.Context(), chi.URLParam(request, "appID"), chi.URLParam(request, "subscriptionID"))
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, item)
}
func (handlers adminQueueHandlers) callbacks(response http.ResponseWriter, request *http.Request) {
	items, err := handlers.queue.CallbackOutcomes(request.Context(), chi.URLParam(request, "appID"), chi.URLParam(request, "subscriptionID"), 50)
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"items": items})
}
func (handlers adminQueueHandlers) drain(response http.ResponseWriter, request *http.Request) {
	var input struct {
		TimeoutSeconds int `json:"timeout_seconds,omitempty"`
	}
	if decodeJSON(request, &input) != nil {
		writeServiceError(response, service.ErrInvalidInput)
		return
	}
	item, err := handlers.queue.BeginDrain(request.Context(), chi.URLParam(request, "appID"), chi.URLParam(request, "subscriptionID"), input.TimeoutSeconds)
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusAccepted, item)
}
