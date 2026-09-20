package httpapi

import (
	"net/http"
	"strconv"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/go-chi/chi/v5"
)

func realtimeHistoryHandler(dependencies Dependencies) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		token, ok := bearerToken(request.Header.Get("Authorization"))
		if !ok {
			writeError(response, http.StatusUnauthorized, "unauthorized", "Authentication failed.")
			return
		}
		claims, err := dependencies.TokenIssuer.Verify(token, "ws:connect")
		if err != nil {
			writeError(response, http.StatusUnauthorized, "unauthorized", "Authentication failed.")
			return
		}
		channel := chi.URLParam(request, "channel")
		if !historyAllowed(claims.Channels, channel) {
			writeError(response, http.StatusForbidden, "forbidden", "The token does not allow history for this channel.")
			return
		}
		limit, err := strconv.Atoi(request.URL.Query().Get("limit"))
		if err != nil {
			writeError(response, http.StatusBadRequest, "invalid_request", "The request is invalid.")
			return
		}
		result, protocolErr := dependencies.Realtime.HistoryForApp(claims.AppID, channel, request.URL.Query().Get("cursor"), limit)
		if protocolErr != nil {
			status := http.StatusServiceUnavailable
			if protocolErr.Code == "invalid_history" {
				status = http.StatusBadRequest
			}
			writeError(response, status, protocolErr.Code, protocolErr.Message)
			return
		}
		writeJSON(response, http.StatusOK, result)
	}
}

func historyAllowed(capabilities map[string][]string, channel string) bool {
	for grant, actions := range capabilities {
		if !domain.RealtimeChannelGrantMatches(grant, channel) {
			continue
		}
		for _, action := range actions {
			if action == "history" {
				return true
			}
		}
	}
	return false
}
