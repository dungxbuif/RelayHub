package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/go-chi/chi/v5"
)

type queueHandlers struct{ queue *service.QueueService }

func (handlers queueHandlers) create(response http.ResponseWriter, request *http.Request) {
	var input service.QueueSubscriptionInput
	if decodeJSON(request, &input) != nil {
		writeServiceError(response, service.ErrInvalidInput)
		return
	}
	item, err := handlers.queue.Create(request.Context(), authenticatedApp(request).App.ID, input)
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, item)
}

func (handlers queueHandlers) list(response http.ResponseWriter, request *http.Request) {
	items, err := handlers.queue.List(request.Context(), authenticatedApp(request).App.ID)
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"items": items})
}

func (handlers queueHandlers) get(response http.ResponseWriter, request *http.Request) {
	item, err := handlers.queue.Get(request.Context(), authenticatedApp(request).App.ID, chi.URLParam(request, "subscriptionID"))
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, item)
}

func (handlers queueHandlers) update(response http.ResponseWriter, request *http.Request) {
	var input struct {
		PolicyVersion int64 `json:"policy_version"`
		service.QueueSubscriptionInput
	}
	if decodeJSON(request, &input) != nil {
		writeServiceError(response, service.ErrInvalidInput)
		return
	}
	item, err := handlers.queue.Update(request.Context(), authenticatedApp(request).App.ID, chi.URLParam(request, "subscriptionID"), input.PolicyVersion, input.QueueSubscriptionInput)
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, item)
}

func (handlers queueHandlers) pause(paused bool) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		item, err := handlers.queue.Pause(request.Context(), authenticatedApp(request).App.ID, chi.URLParam(request, "subscriptionID"), paused)
		if err != nil {
			writeServiceError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, item)
	}
}

func (handlers queueHandlers) delete(response http.ResponseWriter, request *http.Request) {
	if err := handlers.queue.Delete(request.Context(), authenticatedApp(request).App.ID, chi.URLParam(request, "subscriptionID")); err != nil {
		writeServiceError(response, err)
		return
	}
	response.WriteHeader(http.StatusNoContent)
}

func (handlers queueHandlers) pull(response http.ResponseWriter, request *http.Request) {
	var input service.QueuePullInput
	if decodeJSON(request, &input) != nil {
		writeServiceError(response, service.ErrInvalidInput)
		return
	}
	items, err := handlers.queue.Pull(request.Context(), authenticatedApp(request).App.ID, chi.URLParam(request, "subscriptionID"), input)
	if err != nil {
		writeServiceError(response, err)
		return
	}
	for _, item := range items {
		logOperation(request, operationFields{SubscriptionID: item.SubscriptionID, DeliveryID: item.ID, EventID: item.Event.ID, Attempt: item.Attempt, Operation: "queue.pull", Outcome: "leased"})
	}
	writeJSON(response, http.StatusOK, map[string]any{"items": items})
}

func (handlers queueHandlers) settle(response http.ResponseWriter, request *http.Request) {
	var input struct {
		Items []service.QueueSettleItem `json:"items"`
	}
	if decodeJSON(request, &input) != nil {
		writeServiceError(response, service.ErrInvalidInput)
		return
	}
	items, err := handlers.queue.Settle(request.Context(), authenticatedApp(request).App.ID, chi.URLParam(request, "subscriptionID"), input.Items)
	if err != nil {
		writeServiceError(response, err)
		return
	}
	for _, item := range items {
		logOperation(request, operationFields{SubscriptionID: chi.URLParam(request, "subscriptionID"), Operation: "queue.settle", Outcome: item.Status})
	}
	writeJSON(response, http.StatusOK, map[string]any{"items": items})
}

func (handlers queueHandlers) extend(response http.ResponseWriter, request *http.Request) {
	var input struct {
		Items []service.QueueExtendItem `json:"items"`
	}
	if decodeJSON(request, &input) != nil {
		writeServiceError(response, service.ErrInvalidInput)
		return
	}
	items, err := handlers.queue.Extend(request.Context(), authenticatedApp(request).App.ID, chi.URLParam(request, "subscriptionID"), input.Items)
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"items": items})
}

func (handlers queueHandlers) depth(response http.ResponseWriter, request *http.Request) {
	depth, err := handlers.queue.Depth(request.Context(), authenticatedApp(request).App.ID, chi.URLParam(request, "subscriptionID"))
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, depth)
}

func (handlers queueHandlers) beginDrain(response http.ResponseWriter, request *http.Request) {
	var input struct {
		TimeoutSeconds int `json:"timeout_seconds,omitempty"`
	}
	if decodeJSON(request, &input) != nil {
		writeServiceError(response, service.ErrInvalidInput)
		return
	}
	drain, err := handlers.queue.BeginDrain(request.Context(), authenticatedApp(request).App.ID, chi.URLParam(request, "subscriptionID"), input.TimeoutSeconds)
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusAccepted, drain)
}

func (handlers queueHandlers) drainStatus(response http.ResponseWriter, request *http.Request) {
	drain, err := handlers.queue.DrainStatus(request.Context(), authenticatedApp(request).App.ID, chi.URLParam(request, "subscriptionID"))
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, drain)
}

func (handlers queueHandlers) createSchedule(response http.ResponseWriter, request *http.Request) {
	var input service.QueueScheduleInput
	if decodeJSON(request, &input) != nil {
		writeServiceError(response, service.ErrInvalidInput)
		return
	}
	item, err := handlers.queue.CreateSchedule(request.Context(), authenticatedApp(request).App.ID, chi.URLParam(request, "subscriptionID"), input)
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, item)
}

func (handlers queueHandlers) listSchedules(response http.ResponseWriter, request *http.Request) {
	items, err := handlers.queue.ListSchedules(request.Context(), authenticatedApp(request).App.ID, chi.URLParam(request, "subscriptionID"))
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"items": items})
}

func (handlers queueHandlers) getSchedule(response http.ResponseWriter, request *http.Request) {
	item, err := handlers.queue.GetSchedule(request.Context(), authenticatedApp(request).App.ID, chi.URLParam(request, "subscriptionID"), chi.URLParam(request, "scheduleID"))
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, item)
}

func (handlers queueHandlers) updateSchedule(response http.ResponseWriter, request *http.Request) {
	var input struct {
		PolicyVersion int64 `json:"policy_version"`
		service.QueueScheduleInput
	}
	if decodeJSON(request, &input) != nil {
		writeServiceError(response, service.ErrInvalidInput)
		return
	}
	item, err := handlers.queue.UpdateSchedule(request.Context(), authenticatedApp(request).App.ID, chi.URLParam(request, "subscriptionID"), chi.URLParam(request, "scheduleID"), input.PolicyVersion, input.QueueScheduleInput)
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, item)
}

func (handlers queueHandlers) deleteSchedule(response http.ResponseWriter, request *http.Request) {
	if err := handlers.queue.DeleteSchedule(request.Context(), authenticatedApp(request).App.ID, chi.URLParam(request, "subscriptionID"), chi.URLParam(request, "scheduleID")); err != nil {
		writeServiceError(response, err)
		return
	}
	response.WriteHeader(http.StatusNoContent)
}

func (handlers queueHandlers) deadLetters(response http.ResponseWriter, request *http.Request) {
	limit := 50
	if raw := request.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			writeServiceError(response, service.ErrInvalidInput)
			return
		}
		limit = parsed
	}
	items, err := handlers.queue.DeadLetters(request.Context(), authenticatedApp(request).App.ID, chi.URLParam(request, "subscriptionID"), request.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeServiceError(response, err)
		return
	}
	next := ""
	if len(items) == limit {
		next = items[len(items)-1].DeliveryID
	}
	writeJSON(response, http.StatusOK, map[string]any{"items": items, "next_cursor": next})
}

func (handlers queueHandlers) exportDeadLetters(response http.ResponseWriter, request *http.Request) {
	limit := 100
	if raw := request.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			writeServiceError(response, service.ErrInvalidInput)
			return
		}
		limit = parsed
	}
	format := request.URL.Query().Get("format")
	if format == "" {
		format = "json"
	}
	if format != "json" && format != "ndjson" {
		writeServiceError(response, service.ErrInvalidInput)
		return
	}
	items, err := handlers.queue.DeadLetters(request.Context(), authenticatedApp(request).App.ID, chi.URLParam(request, "subscriptionID"), request.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeServiceError(response, err)
		return
	}
	if len(items) == limit {
		response.Header().Set("X-RelayHub-Next-Cursor", items[len(items)-1].DeliveryID)
	}
	response.Header().Set("Content-Disposition", `attachment; filename="relayhub-dlq.`+format+`"`)
	if format == "ndjson" {
		response.Header().Set("Content-Type", "application/x-ndjson")
		encoder := json.NewEncoder(response)
		for _, item := range items {
			if err := encoder.Encode(item); err != nil {
				return
			}
		}
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"items": items})
}

func (handlers queueHandlers) replayDeadLetters(response http.ResponseWriter, request *http.Request) {
	var input struct {
		DeliveryIDs []string `json:"delivery_ids"`
	}
	if decodeJSON(request, &input) != nil {
		writeServiceError(response, service.ErrInvalidInput)
		return
	}
	count, err := handlers.queue.ReplayDeadLetters(request.Context(), authenticatedApp(request).App.ID, chi.URLParam(request, "subscriptionID"), input.DeliveryIDs)
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]int{"replayed": count})
}

func (handlers queueHandlers) deleteDeadLetters(response http.ResponseWriter, request *http.Request) {
	var input struct {
		DeliveryIDs []string `json:"delivery_ids"`
	}
	if decodeJSON(request, &input) != nil {
		writeServiceError(response, service.ErrInvalidInput)
		return
	}
	count, err := handlers.queue.DeleteDeadLetters(request.Context(), authenticatedApp(request).App.ID, chi.URLParam(request, "subscriptionID"), input.DeliveryIDs)
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]int{"deleted": count})
}
