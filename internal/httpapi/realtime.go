package httpapi

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/go-chi/chi/v5"
)

type realtimePublisher interface {
	PublishChannel(context.Context, domain.ChannelMessage)
}

type realtimeHandlers struct {
	publisher realtimePublisher
}

type realtimePublishRequest struct {
	Data json.RawMessage `json:"data"`
}

func (handlers realtimeHandlers) publish(response http.ResponseWriter, request *http.Request) {
	channel := chi.URLParam(request, "channel")
	var input realtimePublishRequest
	if err := decodeJSON(request, &input); err != nil || !domain.ValidRealtimeChannel(channel) || !domain.JSONObject(input.Data) {
		writeError(response, http.StatusBadRequest, "invalid_request", "The request is invalid.")
		return
	}
	handlers.publisher.PublishChannel(request.Context(), domain.ChannelMessage{Channel: channel, PublisherAppID: authenticatedApp(request).App.ID, Data: append(json.RawMessage(nil), input.Data...)})
	response.WriteHeader(http.StatusAccepted)
}
