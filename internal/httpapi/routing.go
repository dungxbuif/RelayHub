package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/go-chi/chi/v5"
)

type routingHandlers struct {
	routing *service.RoutingService
}

type routingRuleRequest struct {
	SourceAppID     *string `json:"source_app_id"`
	EventType       string  `json:"event_type"`
	TargetAppID     string  `json:"target_app_id"`
	RealtimeChannel *string `json:"realtime_channel"`
	Enabled         *bool   `json:"enabled"`
}

func (handlers routingHandlers) create(response http.ResponseWriter, request *http.Request) {
	var input routingRuleRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(response, http.StatusBadRequest, "invalid_request", "The request is invalid.")
		return
	}
	rule, err := handlers.routing.Create(request.Context(), service.CreateRoutingRule{SourceAppID: input.SourceAppID, EventType: input.EventType, TargetAppID: input.TargetAppID, RealtimeChannel: input.RealtimeChannel, Enabled: input.Enabled})
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, rule)
}

func (handlers routingHandlers) list(response http.ResponseWriter, request *http.Request) {
	rules, err := handlers.routing.List(request.Context())
	if err != nil {
		writeServiceError(response, err)
		return
	}
	if rules == nil {
		rules = []domain.RoutingRule{}
	}
	writeJSON(response, http.StatusOK, rules)
}

func (handlers routingHandlers) update(response http.ResponseWriter, request *http.Request) {
	input, err := decodeUpdateRoutingRule(request)
	if err != nil {
		writeError(response, http.StatusBadRequest, "invalid_request", "The request is invalid.")
		return
	}
	rule, err := handlers.routing.Update(request.Context(), chi.URLParam(request, "ruleID"), input)
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, rule)
}

func (handlers routingHandlers) delete(response http.ResponseWriter, request *http.Request) {
	if err := handlers.routing.Delete(request.Context(), chi.URLParam(request, "ruleID")); err != nil {
		writeServiceError(response, err)
		return
	}
	response.WriteHeader(http.StatusNoContent)
}

func decodeUpdateRoutingRule(request *http.Request) (service.UpdateRoutingRule, error) {
	var raw map[string]json.RawMessage
	if err := decodeJSON(request, &raw); err != nil || len(raw) == 0 {
		return service.UpdateRoutingRule{}, service.ErrInvalidInput
	}
	var result service.UpdateRoutingRule
	for field, value := range raw {
		switch field {
		case "source_app_id":
			result.SourceAppID.Set = true
			if string(value) != "null" {
				var source string
				if err := json.Unmarshal(value, &source); err != nil {
					return service.UpdateRoutingRule{}, service.ErrInvalidInput
				}
				result.SourceAppID.Value = &source
			}
		case "event_type":
			var eventType string
			if err := json.Unmarshal(value, &eventType); err != nil {
				return service.UpdateRoutingRule{}, service.ErrInvalidInput
			}
			result.EventType = &eventType
		case "target_app_id":
			var target string
			if err := json.Unmarshal(value, &target); err != nil {
				return service.UpdateRoutingRule{}, service.ErrInvalidInput
			}
			result.TargetAppID = &target
		case "realtime_channel":
			result.RealtimeChannel.Set = true
			if string(value) != "null" {
				var channel string
				if err := json.Unmarshal(value, &channel); err != nil {
					return service.UpdateRoutingRule{}, service.ErrInvalidInput
				}
				result.RealtimeChannel.Value = &channel
			}
		case "enabled":
			var enabled bool
			if err := json.Unmarshal(value, &enabled); err != nil {
				return service.UpdateRoutingRule{}, service.ErrInvalidInput
			}
			result.Enabled = &enabled
		default:
			return service.UpdateRoutingRule{}, service.ErrInvalidInput
		}
	}
	return result, nil
}
