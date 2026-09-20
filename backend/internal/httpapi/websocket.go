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
		v2 := false
		for _, candidate := range websocket.Subprotocols(r) {
			if candidate == realtime.ProtocolV2 {
				v2 = true
				break
			}
		}
		if v2 && claims.ClientID == "" {
			writeError(w, http.StatusForbidden, "forbidden", "Realtime v2 requires client identity and channel capabilities.")
			return
		}
		requestUpgrader := upgrader
		if v2 {
			requestUpgrader.Subprotocols = []string{realtime.ProtocolV2}
		}
		var session *realtime.Session
		if v2 {
			session = d.Realtime.RegisterV2(claims.AppID, claims.ClientID, claims.Channels)
		} else {
			session = d.Realtime.Register(claims.AppID)
		}
		defer session.Close()
		select {
		case <-session.Done():
			writeError(w, http.StatusServiceUnavailable, "not_ready", "The realtime connection registry is unavailable.")
			return
		default:
		}
		conn, err := requestUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		ready := realtime.ServerFrame{Type: "ready", AppID: claims.AppID, ConnectionID: session.ID()}
		if v2 {
			ready.Protocol = realtime.ProtocolV2
			ready.ClientID = claims.ClientID
			ready.Capabilities = []string{"subscribe", "unsubscribe", "publish", "publish.batch", "history", "rewind", "namespace-grants", "presence", "occupancy", "message.actions", "encryption.aes-256-gcm", "audience.all", "audience.others", "audience.connection", "audience.client"}
		}
		session.Send(ready)
		session.Serve(conn)
	}
}
