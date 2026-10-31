package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/go-chi/chi/v5"
)

type adminQueueHandlers struct {
	queue *service.QueueService
	apps  *service.AppService
}

func (h adminQueueHandlers) subscriptions(response http.ResponseWriter, request *http.Request) {
	appID, ok := h.activeApp(response, request)
	if !ok || h.queue == nil {
		if ok {
			writeAdminQueueError(response, service.ErrInvalidDependency)
		}
		return
	}
	items, err := h.queue.List(request.Context(), appID)
	if err != nil {
		writeAdminQueueError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"items": items})
}

func (h adminQueueHandlers) createSubscription(response http.ResponseWriter, request *http.Request) {
	appID, ok := h.activeApp(response, request)
	if !ok || h.queue == nil {
		if ok {
			writeAdminQueueError(response, service.ErrInvalidDependency)
		}
		return
	}
	var input service.QueueSubscriptionInput
	if decodeJSON(request, &input) != nil {
		writeAdminQueueError(response, service.ErrInvalidInput)
		return
	}
	item, err := h.queue.Create(request.Context(), appID, input)
	if err != nil {
		writeAdminQueueError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, item)
}

func (h adminQueueHandlers) getSubscription(response http.ResponseWriter, request *http.Request) {
	appID, ok := h.activeApp(response, request)
	if !ok || h.queue == nil {
		if ok {
			writeAdminQueueError(response, service.ErrInvalidDependency)
		}
		return
	}
	item, err := h.queue.Get(request.Context(), appID, chi.URLParam(request, "subscriptionID"))
	if err != nil {
		writeAdminQueueError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, item)
}

func (h adminQueueHandlers) updateSubscription(response http.ResponseWriter, request *http.Request) {
	appID, ok := h.activeApp(response, request)
	if !ok || h.queue == nil {
		if ok {
			writeAdminQueueError(response, service.ErrInvalidDependency)
		}
		return
	}
	var input struct {
		PolicyVersion int64 `json:"policy_version"`
		service.QueueSubscriptionInput
	}
	if decodeJSON(request, &input) != nil {
		writeAdminQueueError(response, service.ErrInvalidInput)
		return
	}
	item, err := h.queue.Update(request.Context(), appID, chi.URLParam(request, "subscriptionID"), input.PolicyVersion, input.QueueSubscriptionInput)
	if err != nil {
		writeAdminQueueError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, item)
}

func (h adminQueueHandlers) pause(paused bool) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		appID, ok := h.activeApp(response, request)
		if !ok || h.queue == nil {
			if ok {
				writeAdminQueueError(response, service.ErrInvalidDependency)
			}
			return
		}
		item, err := h.queue.Pause(request.Context(), appID, chi.URLParam(request, "subscriptionID"), paused)
		if err != nil {
			writeAdminQueueError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, item)
	}
}

func (h adminQueueHandlers) deleteSubscription(response http.ResponseWriter, request *http.Request) {
	appID, ok := h.activeApp(response, request)
	if !ok || h.queue == nil {
		if ok {
			writeAdminQueueError(response, service.ErrInvalidDependency)
		}
		return
	}
	if request.Header.Get("X-RelayHub-Confirm") != "true" {
		writeAdminQueueError(response, service.ErrInvalidInput)
		return
	}
	if err := h.queue.Delete(request.Context(), appID, chi.URLParam(request, "subscriptionID")); err != nil {
		writeAdminQueueError(response, err)
		return
	}
	response.WriteHeader(http.StatusNoContent)
}

func (h adminQueueHandlers) pull(response http.ResponseWriter, request *http.Request) {
	appID, ok := h.activeApp(response, request)
	if !ok || h.queue == nil {
		if ok {
			writeAdminQueueError(response, service.ErrInvalidDependency)
		}
		return
	}
	var input service.QueuePullInput
	if decodeJSON(request, &input) != nil {
		writeAdminQueueError(response, service.ErrInvalidInput)
		return
	}
	items, err := h.queue.Pull(request.Context(), appID, chi.URLParam(request, "subscriptionID"), input)
	if err != nil {
		writeAdminQueueError(response, err)
		return
	}
	for _, item := range items {
		logOperation(request, operationFields{AppID: appID, SubscriptionID: item.SubscriptionID, DeliveryID: item.ID, EventID: item.Event.ID, Attempt: item.Attempt, Operation: "admin.queue.pull", Outcome: "leased"})
	}
	writeJSON(response, http.StatusOK, map[string]any{"items": items})
}

func (h adminQueueHandlers) settle(response http.ResponseWriter, request *http.Request) {
	appID, ok := h.activeApp(response, request)
	if !ok || h.queue == nil {
		if ok {
			writeAdminQueueError(response, service.ErrInvalidDependency)
		}
		return
	}
	var input struct {
		Items []service.QueueSettleItem `json:"items"`
	}
	if decodeJSON(request, &input) != nil {
		writeAdminQueueError(response, service.ErrInvalidInput)
		return
	}
	items, err := h.queue.Settle(request.Context(), appID, chi.URLParam(request, "subscriptionID"), input.Items)
	if err != nil {
		writeAdminQueueError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"items": items})
}

func (h adminQueueHandlers) extend(response http.ResponseWriter, request *http.Request) {
	appID, ok := h.activeApp(response, request)
	if !ok || h.queue == nil {
		if ok {
			writeAdminQueueError(response, service.ErrInvalidDependency)
		}
		return
	}
	var input struct {
		Items []service.QueueExtendItem `json:"items"`
	}
	if decodeJSON(request, &input) != nil {
		writeAdminQueueError(response, service.ErrInvalidInput)
		return
	}
	items, err := h.queue.Extend(request.Context(), appID, chi.URLParam(request, "subscriptionID"), input.Items)
	if err != nil {
		writeAdminQueueError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"items": items})
}

func (h adminQueueHandlers) metrics(response http.ResponseWriter, request *http.Request) {
	appID, ok := h.activeApp(response, request)
	if !ok || h.queue == nil {
		if ok {
			writeAdminQueueError(response, service.ErrInvalidDependency)
		}
		return
	}
	item, err := h.queue.Depth(request.Context(), appID, chi.URLParam(request, "subscriptionID"))
	if err != nil {
		writeAdminQueueError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, item)
}

func (h adminQueueHandlers) drain(response http.ResponseWriter, request *http.Request) {
	appID, ok := h.activeApp(response, request)
	if !ok || h.queue == nil {
		if ok {
			writeAdminQueueError(response, service.ErrInvalidDependency)
		}
		return
	}
	var input struct {
		TimeoutSeconds int `json:"timeout_seconds,omitempty"`
	}
	if decodeJSON(request, &input) != nil {
		writeAdminQueueError(response, service.ErrInvalidInput)
		return
	}
	item, err := h.queue.BeginDrain(request.Context(), appID, chi.URLParam(request, "subscriptionID"), input.TimeoutSeconds)
	if err != nil {
		writeAdminQueueError(response, err)
		return
	}
	writeJSON(response, http.StatusAccepted, item)
}

func (h adminQueueHandlers) drainStatus(response http.ResponseWriter, request *http.Request) {
	appID, ok := h.activeApp(response, request)
	if !ok || h.queue == nil {
		if ok {
			writeAdminQueueError(response, service.ErrInvalidDependency)
		}
		return
	}
	item, err := h.queue.DrainStatus(request.Context(), appID, chi.URLParam(request, "subscriptionID"))
	if err != nil {
		writeAdminQueueError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, item)
}

func (h adminQueueHandlers) schedules(response http.ResponseWriter, request *http.Request) {
	appID, ok := h.activeApp(response, request)
	if !ok || h.queue == nil {
		if ok {
			writeAdminQueueError(response, service.ErrInvalidDependency)
		}
		return
	}
	items, err := h.queue.ListSchedules(request.Context(), appID, chi.URLParam(request, "subscriptionID"))
	if err != nil {
		writeAdminQueueError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"items": items})
}

func (h adminQueueHandlers) createSchedule(response http.ResponseWriter, request *http.Request) {
	appID, ok := h.activeApp(response, request)
	if !ok || h.queue == nil {
		if ok {
			writeAdminQueueError(response, service.ErrInvalidDependency)
		}
		return
	}
	var input service.QueueScheduleInput
	if decodeJSON(request, &input) != nil {
		writeAdminQueueError(response, service.ErrInvalidInput)
		return
	}
	item, err := h.queue.CreateSchedule(request.Context(), appID, chi.URLParam(request, "subscriptionID"), input)
	if err != nil {
		writeAdminQueueError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, item)
}

func (h adminQueueHandlers) getSchedule(response http.ResponseWriter, request *http.Request) {
	appID, ok := h.activeApp(response, request)
	if !ok || h.queue == nil {
		if ok {
			writeAdminQueueError(response, service.ErrInvalidDependency)
		}
		return
	}
	item, err := h.queue.GetSchedule(request.Context(), appID, chi.URLParam(request, "subscriptionID"), chi.URLParam(request, "scheduleID"))
	if err != nil {
		writeAdminQueueError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, item)
}

func (h adminQueueHandlers) updateSchedule(response http.ResponseWriter, request *http.Request) {
	appID, ok := h.activeApp(response, request)
	if !ok || h.queue == nil {
		if ok {
			writeAdminQueueError(response, service.ErrInvalidDependency)
		}
		return
	}
	var input struct {
		PolicyVersion int64 `json:"policy_version"`
		service.QueueScheduleInput
	}
	if decodeJSON(request, &input) != nil {
		writeAdminQueueError(response, service.ErrInvalidInput)
		return
	}
	item, err := h.queue.UpdateSchedule(request.Context(), appID, chi.URLParam(request, "subscriptionID"), chi.URLParam(request, "scheduleID"), input.PolicyVersion, input.QueueScheduleInput)
	if err != nil {
		writeAdminQueueError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, item)
}

func (h adminQueueHandlers) deleteSchedule(response http.ResponseWriter, request *http.Request) {
	appID, ok := h.activeApp(response, request)
	if !ok || h.queue == nil {
		if ok {
			writeAdminQueueError(response, service.ErrInvalidDependency)
		}
		return
	}
	if request.Header.Get("X-RelayHub-Confirm") != "true" {
		writeAdminQueueError(response, service.ErrInvalidInput)
		return
	}
	if err := h.queue.DeleteSchedule(request.Context(), appID, chi.URLParam(request, "subscriptionID"), chi.URLParam(request, "scheduleID")); err != nil {
		writeAdminQueueError(response, err)
		return
	}
	response.WriteHeader(http.StatusNoContent)
}

func (h adminQueueHandlers) callbacks(response http.ResponseWriter, request *http.Request) {
	appID, ok := h.activeApp(response, request)
	if !ok || h.queue == nil {
		if ok {
			writeAdminQueueError(response, service.ErrInvalidDependency)
		}
		return
	}
	limit, err := adminQueueLimit(request, 50)
	if err != nil {
		writeAdminQueueError(response, err)
		return
	}
	items, err := h.queue.CallbackOutcomes(request.Context(), appID, chi.URLParam(request, "subscriptionID"), limit)
	if err != nil {
		writeAdminQueueError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"items": items})
}

func (h adminQueueHandlers) deadLetters(response http.ResponseWriter, request *http.Request) {
	appID, ok := h.activeApp(response, request)
	if !ok || h.queue == nil {
		if ok {
			writeAdminQueueError(response, service.ErrInvalidDependency)
		}
		return
	}
	limit, err := adminQueueLimit(request, 50)
	if err != nil {
		writeAdminQueueError(response, err)
		return
	}
	items, err := h.queue.DeadLetters(request.Context(), appID, chi.URLParam(request, "subscriptionID"), request.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeAdminQueueError(response, err)
		return
	}
	next := ""
	if len(items) == limit {
		next = items[len(items)-1].DeliveryID
	}
	writeJSON(response, http.StatusOK, map[string]any{"items": items, "next_cursor": next})
}

func (h adminQueueHandlers) exportDeadLetters(response http.ResponseWriter, request *http.Request) {
	appID, ok := h.activeApp(response, request)
	if !ok || h.queue == nil {
		if ok {
			writeAdminQueueError(response, service.ErrInvalidDependency)
		}
		return
	}
	format := request.URL.Query().Get("format")
	if format == "" {
		format = "json"
	}
	if format != "json" && format != "ndjson" {
		writeAdminQueueError(response, service.ErrInvalidInput)
		return
	}
	limit, err := adminQueueLimit(request, 100)
	if err != nil {
		writeAdminQueueError(response, err)
		return
	}
	items, err := h.queue.DeadLetters(request.Context(), appID, chi.URLParam(request, "subscriptionID"), request.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeAdminQueueError(response, err)
		return
	}
	if len(items) == limit {
		response.Header().Set("X-RelayHub-Next-Cursor", items[len(items)-1].DeliveryID)
	}
	response.Header().Set("Content-Disposition", `attachment; filename="relayhub-dlq.`+format+`"`)
	if format == "ndjson" {
		response.Header().Set("Content-Type", "application/x-ndjson")
		for _, item := range items {
			if err := json.NewEncoder(response).Encode(item); err != nil {
				return
			}
		}
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"items": items})
}

func (h adminQueueHandlers) replayDeadLetters(response http.ResponseWriter, request *http.Request) {
	appID, ok := h.activeApp(response, request)
	if !ok || h.queue == nil {
		if ok {
			writeAdminQueueError(response, service.ErrInvalidDependency)
		}
		return
	}
	var input struct {
		DeliveryIDs []string `json:"delivery_ids"`
		Confirm     bool     `json:"confirm"`
	}
	if decodeJSON(request, &input) != nil || !input.Confirm {
		writeAdminQueueError(response, service.ErrInvalidInput)
		return
	}
	count, err := h.queue.ReplayDeadLetters(request.Context(), appID, chi.URLParam(request, "subscriptionID"), input.DeliveryIDs)
	if err != nil {
		writeAdminQueueError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]int{"replayed": count})
}

func (h adminQueueHandlers) deleteDeadLetters(response http.ResponseWriter, request *http.Request) {
	appID, ok := h.activeApp(response, request)
	if !ok || h.queue == nil {
		if ok {
			writeAdminQueueError(response, service.ErrInvalidDependency)
		}
		return
	}
	var input struct {
		DeliveryIDs []string `json:"delivery_ids"`
		Confirm     bool     `json:"confirm"`
	}
	if decodeJSON(request, &input) != nil || !input.Confirm {
		writeAdminQueueError(response, service.ErrInvalidInput)
		return
	}
	count, err := h.queue.DeleteDeadLetters(request.Context(), appID, chi.URLParam(request, "subscriptionID"), input.DeliveryIDs)
	if err != nil {
		writeAdminQueueError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]int{"deleted": count})
}

func (h adminQueueHandlers) activeApp(response http.ResponseWriter, request *http.Request) (string, bool) {
	if h.apps == nil {
		writeAdminQueueError(response, service.ErrInvalidDependency)
		return "", false
	}
	app, err := h.apps.Get(request.Context(), chi.URLParam(request, "appID"))
	if err != nil || !app.Enabled {
		writeError(response, http.StatusNotFound, "not_found", "The requested resource was not found.")
		return "", false
	}
	return app.ID, true
}

func adminQueueLimit(request *http.Request, fallback int) (int, error) {
	limit := fallback
	if raw := request.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return 0, service.ErrInvalidInput
		}
		limit = parsed
	}
	if limit < 1 || limit > 100 {
		return 0, service.ErrInvalidInput
	}
	return limit, nil
}

func writeAdminQueueError(response http.ResponseWriter, err error) {
	if errors.Is(err, service.ErrInvalidDependency) {
		writeError(response, http.StatusServiceUnavailable, "not_ready", "A required dependency is unavailable.")
		return
	}
	writeServiceError(response, err)
}
