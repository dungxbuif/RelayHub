package httpapi

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/dungxbuif/RelayHub/internal/auth"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/go-chi/chi/v5"
)

type adminControlHandlers struct {
	apps      *service.AppService
	issuer    *auth.TokenIssuer
	publisher realtimePublisher
}

type studioTokenRequest struct {
	AppID    string `json:"app_id"`
	Protocol string `json:"protocol"`
}

type studioPublishRequest struct {
	AppID   string          `json:"app_id"`
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

func (handlers adminControlHandlers) updateApp(response http.ResponseWriter, request *http.Request) {
	if handlers.apps == nil {
		writeError(response, http.StatusServiceUnavailable, "unavailable", "Admin application management is unavailable.")
		return
	}
	input, err := decodeUpdateApp(request)
	if err != nil {
		writeServiceError(response, service.ErrInvalidInput)
		return
	}
	app, err := handlers.apps.Update(request.Context(), chi.URLParam(request, "appID"), input)
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, app)
}

func (handlers adminControlHandlers) studioToken(response http.ResponseWriter, request *http.Request) {
	var input studioTokenRequest
	if handlers.apps == nil || handlers.issuer == nil || decodeJSON(request, &input) != nil || (input.Protocol != "realtime" && input.Protocol != "stream") {
		writeServiceError(response, service.ErrInvalidInput)
		return
	}
	app, err := handlers.apps.Get(request.Context(), input.AppID)
	if err != nil || !app.Enabled {
		writeServiceError(response, service.ErrInvalidInput)
		return
	}
	scope := "ws:connect"
	if input.Protocol == "stream" {
		scope = "stream:connect"
	}
	token, err := handlers.issuer.Issue(app.ID, []string{scope}, 5*time.Minute)
	if err != nil {
		writeServiceError(response, service.ErrInvalidInput)
		return
	}
	claims, err := handlers.issuer.Verify(token, scope)
	if err != nil {
		writeError(response, http.StatusInternalServerError, "internal_error", "An internal error occurred.")
		return
	}
	response.Header().Set("Cache-Control", "no-store")
	writeJSON(response, http.StatusCreated, socketTokenResponse{Token: token, ExpiresAt: claims.ExpiresAt})
}

func (handlers adminControlHandlers) studioPublish(response http.ResponseWriter, request *http.Request) {
	var input studioPublishRequest
	if handlers.apps == nil || handlers.publisher == nil || decodeJSON(request, &input) != nil || !domain.ValidRealtimeChannel(input.Channel) || !domain.JSONObject(input.Data) {
		writeServiceError(response, service.ErrInvalidInput)
		return
	}
	app, err := handlers.apps.Get(request.Context(), input.AppID)
	if err != nil || !app.Enabled {
		writeServiceError(response, service.ErrInvalidInput)
		return
	}
	handlers.publisher.PublishChannel(request.Context(), domain.ChannelMessage{Channel: input.Channel, PublisherAppID: app.ID, Data: append(json.RawMessage(nil), input.Data...)})
	response.WriteHeader(http.StatusAccepted)
}
