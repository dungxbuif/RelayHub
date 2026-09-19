package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/dungxbuif/RelayHub/internal/adminread"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/go-chi/chi/v5"
)

type adminReadHandlers struct{ reads *service.AdminReadService }

func (handlers adminReadHandlers) available(response http.ResponseWriter) bool {
	if handlers.reads == nil {
		writeError(response, http.StatusServiceUnavailable, "unavailable", "Admin reads are unavailable.")
		return false
	}
	return true
}

func (handlers adminReadHandlers) dashboard(response http.ResponseWriter, request *http.Request) {
	if !handlers.available(response) {
		return
	}
	window, step, ok := parseMetricRange(request)
	if !ok {
		writeAdminReadError(response, service.ErrInvalidInput)
		return
	}
	result, err := handlers.reads.Dashboard(request.Context(), window, step)
	if err != nil {
		writeAdminReadError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, result)
}

func (handlers adminReadHandlers) metrics(response http.ResponseWriter, request *http.Request) {
	if !handlers.available(response) {
		return
	}
	window, step, ok := parseMetricRange(request)
	if !ok {
		writeAdminReadError(response, service.ErrInvalidInput)
		return
	}
	result, err := handlers.reads.Metrics(request.Context(), window, step)
	if err != nil {
		writeAdminReadError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, result)
}

func parseMetricRange(request *http.Request) (time.Duration, time.Duration, bool) {
	if !onlyQueryKeys(request, "window", "step") {
		return 0, 0, false
	}
	windows := map[string]time.Duration{"5m": 5 * time.Minute, "15m": 15 * time.Minute, "1h": time.Hour, "6h": 6 * time.Hour, "24h": 24 * time.Hour}
	steps := map[string]time.Duration{"1m": time.Minute, "5m": 5 * time.Minute, "15m": 15 * time.Minute, "1h": time.Hour}
	windowRaw, ok := uniqueQuery(request, "window")
	if !ok {
		return 0, 0, false
	}
	stepRaw, ok := uniqueQuery(request, "step")
	if !ok {
		return 0, 0, false
	}
	if windowRaw == "" {
		windowRaw = "15m"
	}
	if stepRaw == "" {
		stepRaw = "1m"
	}
	window, windowOK := windows[windowRaw]
	step, stepOK := steps[stepRaw]
	return window, step, windowOK && stepOK && validMetricPair(window, step)
}

func validMetricPair(window, step time.Duration) bool {
	pairs := map[time.Duration]map[time.Duration]bool{
		5 * time.Minute: {time.Minute: true}, 15 * time.Minute: {time.Minute: true},
		time.Hour: {time.Minute: true, 5 * time.Minute: true}, 6 * time.Hour: {5 * time.Minute: true, 15 * time.Minute: true},
		24 * time.Hour: {15 * time.Minute: true, time.Hour: true},
	}
	return pairs[window][step]
}

func (handlers adminReadHandlers) events(response http.ResponseWriter, request *http.Request) {
	if !handlers.available(response) {
		return
	}
	filters := adminread.EventFilters{}
	options, ok := parseListOptions(request, &filters, nil, nil)
	if !ok {
		writeAdminReadError(response, service.ErrInvalidInput)
		return
	}
	result, err := handlers.reads.ListEvents(request.Context(), adminread.EventListQuery{Options: options, Filters: filters})
	writeAdminReadResult(response, result, err)
}

func (handlers adminReadHandlers) event(response http.ResponseWriter, request *http.Request) {
	if !handlers.available(response) {
		return
	}
	result, err := handlers.reads.GetEvent(request.Context(), chi.URLParam(request, "eventID"))
	writeAdminReadResult(response, result, err)
}

func (handlers adminReadHandlers) deadLetters(response http.ResponseWriter, request *http.Request) {
	if !handlers.available(response) {
		return
	}
	filters := adminread.DeadLetterFilters{}
	options, ok := parseListOptions(request, nil, &filters, nil)
	if !ok {
		writeAdminReadError(response, service.ErrInvalidInput)
		return
	}
	result, err := handlers.reads.ListDeadLetters(request.Context(), adminread.DeadLetterListQuery{Options: options, Filters: filters})
	writeAdminReadResult(response, result, err)
}

func (handlers adminReadHandlers) deadLetter(response http.ResponseWriter, request *http.Request) {
	if !handlers.available(response) {
		return
	}
	result, err := handlers.reads.GetDeadLetter(request.Context(), chi.URLParam(request, "deliveryID"))
	writeAdminReadResult(response, result, err)
}

func (handlers adminReadHandlers) audit(response http.ResponseWriter, request *http.Request) {
	if !handlers.available(response) {
		return
	}
	filters := adminread.AuditFilters{}
	options, ok := parseListOptions(request, nil, nil, &filters)
	if !ok {
		writeAdminReadError(response, service.ErrInvalidInput)
		return
	}
	result, err := handlers.reads.ListAudit(request.Context(), adminread.AuditListQuery{Options: options, Filters: filters})
	writeAdminReadResult(response, result, err)
}

func parseListOptions(request *http.Request, events *adminread.EventFilters, deadLetters *adminread.DeadLetterFilters, audit *adminread.AuditFilters) (adminread.ListOptions, bool) {
	allowed := []string{"limit", "cursor", "from", "to"}
	switch {
	case events != nil:
		allowed = append(allowed, "type", "source_app_id")
	case deadLetters != nil:
		allowed = append(allowed, "source_app_id", "target_app_id", "sink", "reason")
	case audit != nil:
		allowed = append(allowed, "actor_type", "action", "resource_type", "resource_id", "outcome")
	default:
		return adminread.ListOptions{}, false
	}
	if !onlyQueryKeys(request, allowed...) {
		return adminread.ListOptions{}, false
	}
	values := map[string]string{}
	for _, key := range allowed {
		value, ok := uniqueQuery(request, key)
		if !ok {
			return adminread.ListOptions{}, false
		}
		values[key] = value
	}
	limit := 0
	var err error
	if values["limit"] != "" {
		limit, err = strconv.Atoi(values["limit"])
		if err != nil {
			return adminread.ListOptions{}, false
		}
	}
	from, ok := parseOptionalTime(values["from"])
	if !ok {
		return adminread.ListOptions{}, false
	}
	to, ok := parseOptionalTime(values["to"])
	if !ok {
		return adminread.ListOptions{}, false
	}
	var fingerprint string
	switch {
	case events != nil:
		*events = adminread.EventFilters{Type: values["type"], SourceAppID: values["source_app_id"], From: from, To: to}
		fingerprint = events.Fingerprint()
	case deadLetters != nil:
		*deadLetters = adminread.DeadLetterFilters{SourceAppID: values["source_app_id"], TargetAppID: values["target_app_id"], Sink: values["sink"], Reason: values["reason"], From: from, To: to}
		fingerprint = deadLetters.Fingerprint()
	case audit != nil:
		*audit = adminread.AuditFilters{ActorType: values["actor_type"], Action: values["action"], ResourceType: values["resource_type"], ResourceID: values["resource_id"], Outcome: values["outcome"], From: from, To: to}
		fingerprint = audit.Fingerprint()
	}
	options := adminread.ListOptions{Limit: limit}
	if values["cursor"] != "" {
		cursor, err := adminread.DecodeCursor(values["cursor"], fingerprint)
		if err != nil {
			return adminread.ListOptions{}, false
		}
		options.Cursor = &cursor
	}
	return options, options.ValidateCursor(fingerprint) == nil
}

func parseOptionalTime(raw string) (*time.Time, bool) {
	if raw == "" {
		return nil, true
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return nil, false
	}
	parsed = parsed.UTC()
	return &parsed, true
}

func onlyQueryKeys(request *http.Request, allowed ...string) bool {
	set := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		set[key] = struct{}{}
	}
	for key := range request.URL.Query() {
		if _, ok := set[key]; !ok {
			return false
		}
	}
	return true
}

func uniqueQuery(request *http.Request, key string) (string, bool) {
	values, exists := request.URL.Query()[key]
	if !exists {
		return "", true
	}
	if len(values) != 1 {
		return "", false
	}
	return values[0], true
}

func writeAdminReadResult(response http.ResponseWriter, result any, err error) {
	if err != nil {
		writeAdminReadError(response, service.AdminReadError(err))
		return
	}
	writeJSON(response, http.StatusOK, result)
}

func writeAdminReadError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, service.ErrInvalidInput):
		writeError(response, http.StatusBadRequest, "invalid_request", "The request is invalid.")
	case errors.Is(err, service.ErrNotFound):
		writeError(response, http.StatusNotFound, "not_found", "The requested resource was not found.")
	case errors.Is(err, context.DeadlineExceeded):
		writeError(response, http.StatusGatewayTimeout, "timeout", "The Admin read timed out.")
	default:
		writeError(response, http.StatusInternalServerError, "internal_error", "An internal error occurred.")
	}
}
