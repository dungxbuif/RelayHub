package httpapi

import (
	"errors"
	"net/http"

	"github.com/dungxbuif/RelayHub/internal/auth"
	"github.com/dungxbuif/RelayHub/internal/realtime"
	"github.com/gorilla/websocket"
)

func websocketHandler(d Dependencies) http.HandlerFunc {
	allowed := map[string]bool{}
	for _, origin := range d.AllowedOrigins {
		if origin != "*" {
			allowed[origin] = true
		}
	}
	checkOrigin := func(r *http.Request) bool {
		origins := r.Header.Values("Origin")
		return len(origins) == 0 || (len(origins) == 1 && (origins[0] == "" || allowed[origins[0]]))
	}
	upgrader := websocket.Upgrader{ReadBufferSize: 4096, WriteBufferSize: 4096, CheckOrigin: checkOrigin, Error: func(w http.ResponseWriter, r *http.Request, status int, reason error) {
		writeError(w, status, "invalid_upgrade", "A standard WebSocket upgrade is required.")
	}}
	return func(w http.ResponseWriter, r *http.Request) {
		claims, err := d.TokenIssuer.Verify(r.URL.Query().Get("token"), "ws:connect")
		if err != nil {
			if errors.Is(err, auth.ErrMissingScope) {
				writeError(w, http.StatusForbidden, "forbidden", "The token requires ws:connect.")
			} else {
				writeError(w, http.StatusUnauthorized, "unauthorized", "Authentication failed.")
			}
			return
		}
		if state, ok := r.Context().Value(requestLogKey{}).(*requestLogState); ok {
			state.appID = claims.AppID
		}
		if !checkOrigin(r) {
			writeError(w, http.StatusForbidden, "forbidden", "Origin is not allowed.")
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		session := d.Realtime.Register(claims.AppID)
		defer session.Close()
		session.Send(realtime.ServerFrame{Type: "ready", AppID: claims.AppID, ConnectionID: session.ID()})
		session.Serve(conn)
	}
}
