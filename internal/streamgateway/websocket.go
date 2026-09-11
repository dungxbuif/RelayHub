package streamgateway

import (
	"context"
	"sync"
	"time"

	"github.com/dungxbuif/RelayHub/internal/streamprotocol"
	"github.com/gorilla/websocket"
)

// Serve runs one authenticated connection. Authentication, Origin and
// subprotocol negotiation are completed by the HTTP boundary before this call.
func (gateway *Gateway) Serve(ctx context.Context, appID string, connection *websocket.Conn) {
	session, err := gateway.open(appID)
	if err != nil {
		_ = connection.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(streamprotocol.CloseDependencyUnavailable, "stream unavailable"), time.Now().Add(time.Second))
		_ = connection.Close()
		return
	}
	var writers sync.WaitGroup
	writers.Add(1)
	go func() {
		defer writers.Done()
		heartbeat := time.NewTicker(25 * time.Second)
		defer heartbeat.Stop()
		for {
			select {
			case raw := <-session.Frames():
				_ = connection.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if connection.WriteMessage(websocket.TextMessage, raw) != nil {
					_ = connection.Close()
					return
				}
			case <-heartbeat.C:
				_ = connection.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if connection.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second)) != nil {
					_ = connection.Close()
					return
				}
			case <-session.Done():
				_ = connection.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, "RelayHub is draining"), time.Now().Add(time.Second))
				_ = connection.Close()
				return
			case <-ctx.Done():
				_ = connection.Close()
				return
			}
		}
	}()
	connection.SetReadLimit(streamprotocol.MaxMessageBytes)
	const pongWait = 60 * time.Second
	_ = connection.SetReadDeadline(time.Now().Add(pongWait))
	connection.SetPongHandler(func(string) error {
		return connection.SetReadDeadline(time.Now().Add(pongWait))
	})
	for {
		messageType, raw, readErr := connection.ReadMessage()
		if readErr != nil {
			break
		}
		if messageType != websocket.TextMessage {
			_ = connection.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseUnsupportedData, "JSON text frames required"), time.Now().Add(time.Second))
			break
		}
		if protocolErr := session.Handle(ctx, raw); protocolErr != nil {
			if !session.enqueue(map[string]any{"type": "error", "code": protocolErr.Code, "message": protocolErr.Message, "retryable": protocolErr.Retryable}) {
				break
			}
			if protocolErr.Code == "frame_too_large" {
				break
			}
		}
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), gateway.options.DrainTimeout)
	_ = session.Close(closeCtx)
	cancel()
	writers.Wait()
}
