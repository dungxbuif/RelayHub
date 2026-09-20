package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/dungxbuif/RelayHub/internal/auth"
	"github.com/dungxbuif/RelayHub/internal/streamprotocol"
	"github.com/gorilla/websocket"
)

type StreamServer interface {
	Serve(context.Context, string, *websocket.Conn)
}

func streamHandler(dependencies Dependencies) http.HandlerFunc {
	allowed := map[string]bool{}
	for _, origin := range dependencies.AllowedOrigins {
		allowed[origin] = true
	}
	checkOrigin := func(request *http.Request) bool {
		origins := request.Header.Values("Origin")
		return len(origins) == 0 || len(origins) == 1 && (origins[0] == "" || allowed[origins[0]])
	}
	upgrader := websocket.Upgrader{ReadBufferSize: 4096, WriteBufferSize: 4096, Subprotocols: []string{streamprotocol.Subprotocol}, CheckOrigin: checkOrigin, Error: func(response http.ResponseWriter, _ *http.Request, status int, _ error) {
		writeError(response, status, "invalid_upgrade", "A standard WebSocket upgrade is required.")
	}}
	return func(response http.ResponseWriter, request *http.Request) {
		claims, err := dependencies.TokenIssuer.Verify(request.URL.Query().Get("token"), "stream:connect")
		if err != nil {
			if errors.Is(err, auth.ErrMissingScope) {
				writeError(response, http.StatusForbidden, "forbidden", "The token requires stream:connect.")
			} else {
				writeError(response, http.StatusUnauthorized, "unauthorized", "Authentication failed.")
			}
			return
		}
		if state, ok := request.Context().Value(requestLogKey{}).(*requestLogState); ok {
			state.appID = claims.AppID
		}
		if !checkOrigin(request) {
			writeError(response, http.StatusForbidden, "forbidden", "Origin is not allowed.")
			return
		}
		if !hasSubprotocol(request.Header.Values("Sec-WebSocket-Protocol"), streamprotocol.Subprotocol) {
			writeError(response, http.StatusBadRequest, "unsupported_version", "The relayhub.stream.v1 subprotocol is required.")
			return
		}
		if dependencies.Stream == nil {
			writeError(response, http.StatusServiceUnavailable, "stream_unavailable", "The durable stream service is unavailable.")
			return
		}
		connection, upgradeErr := upgrader.Upgrade(response, request, nil)
		if upgradeErr != nil {
			return
		}
		dependencies.Stream.Serve(request.Context(), claims.AppID, connection)
	}
}

func hasSubprotocol(values []string, expected string) bool {
	for _, value := range values {
		for _, candidate := range strings.Split(value, ",") {
			if strings.TrimSpace(candidate) == expected {
				return true
			}
		}
	}
	return false
}
