package relayhub

import (
	"context"
	"encoding/json"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const RealtimeV2Protocol = "relayhub.realtime.v2"

var realtimeChannel = regexp.MustCompile(`^[a-z0-9][a-z0-9_.:-]{0,95}$`)
var realtimeClient = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,128}$`)
var realtimeCursor = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

type RealtimeTokenRequest struct {
	ClientID   string              `json:"client_id"`
	Channels   map[string][]string `json:"channels"`
	TTLSeconds int                 `json:"ttl_seconds"`
}

type RealtimeAudience struct {
	Type         string `json:"type"`
	ConnectionID string `json:"connection_id,omitempty"`
	ClientID     string `json:"client_id,omitempty"`
}

type RealtimeHistoryRequest struct {
	Limit  int    `json:"limit"`
	Cursor string `json:"cursor,omitempty"`
}

type RealtimePublishItem struct {
	ID       string            `json:"id"`
	Channel  string            `json:"channel"`
	Audience *RealtimeAudience `json:"audience,omitempty"`
	Data     any               `json:"data"`
}

type RealtimePublishOutcome struct {
	ID        string `json:"id"`
	Accepted  bool   `json:"accepted"`
	MessageID string `json:"message_id,omitempty"`
	Code      string `json:"code,omitempty"`
}

type RealtimeFrame struct {
	Type                  string                   `json:"type"`
	Protocol              string                   `json:"protocol,omitempty"`
	AppID                 string                   `json:"app_id,omitempty"`
	ClientID              string                   `json:"client_id,omitempty"`
	ConnectionID          string                   `json:"connection_id,omitempty"`
	Channel               string                   `json:"channel,omitempty"`
	Channels              []string                 `json:"channels,omitempty"`
	PublisherClientID     string                   `json:"publisher_client_id,omitempty"`
	PublisherConnectionID string                   `json:"publisher_connection_id,omitempty"`
	MessageID             string                   `json:"message_id,omitempty"`
	PublishedAt           string                   `json:"published_at,omitempty"`
	Audience              *RealtimeAudience        `json:"audience,omitempty"`
	Occupancy             int                      `json:"occupancy,omitempty"`
	Data                  json.RawMessage          `json:"data,omitempty"`
	Code                  string                   `json:"code,omitempty"`
	Message               string                   `json:"message,omitempty"`
	Cursor                string                   `json:"cursor,omitempty"`
	NextCursor            string                   `json:"next_cursor,omitempty"`
	ContinuityCursor      string                   `json:"continuity_cursor,omitempty"`
	Items                 []RealtimeFrame          `json:"items,omitempty"`
	Outcomes              []RealtimePublishOutcome `json:"outcomes,omitempty"`
}

type RealtimeConn struct {
	connection *websocket.Conn
	writeMu    sync.Mutex
}

func (c *Client) DialRealtime(ctx context.Context, request RealtimeTokenRequest) (*RealtimeConn, RealtimeFrame, error) {
	if request.TTLSeconds == 0 {
		request.TTLSeconds = 600
	}
	if !validRealtimeTokenRequest(request) {
		return nil, RealtimeFrame{}, ErrInvalidInput
	}
	var token string
	var err error
	if c.config.RealtimeTokenProvider != nil {
		token, err = c.config.RealtimeTokenProvider(ctx, request)
	} else {
		var response struct {
			Token string `json:"token"`
		}
		_, err = c.request(ctx, "POST", "/api/v1/socket/token", struct {
			Protocol string              `json:"protocol"`
			ClientID string              `json:"client_id"`
			Channels map[string][]string `json:"channels"`
			TTL      int                 `json:"ttl_seconds"`
		}{RealtimeV2Protocol, request.ClientID, request.Channels, request.TTLSeconds}, "", &response)
		token = response.Token
	}
	if err != nil {
		return nil, RealtimeFrame{}, err
	}
	if token == "" {
		return nil, RealtimeFrame{}, ErrProtocol
	}
	endpoint := *c.base
	if endpoint.Scheme == "https" {
		endpoint.Scheme = "wss"
	} else {
		endpoint.Scheme = "ws"
	}
	endpoint.Path = "/ws"
	endpoint.RawQuery = url.Values{"token": []string{token}}.Encode()
	dialer := c.dialer
	dialer.Subprotocols = []string{RealtimeV2Protocol}
	connection, _, err := dialer.DialContext(ctx, endpoint.String(), nil)
	if err != nil {
		return nil, RealtimeFrame{}, safeTransport(ctx, err)
	}
	if connection.Subprotocol() != RealtimeV2Protocol {
		_ = connection.Close()
		return nil, RealtimeFrame{}, ErrProtocol
	}
	_ = connection.SetReadDeadline(time.Now().Add(c.config.HandshakeTimeout))
	var ready RealtimeFrame
	if err := connection.ReadJSON(&ready); err != nil || ready.Type != "ready" || ready.Protocol != RealtimeV2Protocol || ready.ClientID != request.ClientID {
		_ = connection.Close()
		return nil, RealtimeFrame{}, ErrProtocol
	}
	_ = connection.SetReadDeadline(time.Time{})
	return &RealtimeConn{connection: connection}, ready, nil
}

func (connection *RealtimeConn) Subscribe(channels ...string) error {
	return connection.channels("subscribe", channels)
}
func (connection *RealtimeConn) SubscribeWithRewind(request RealtimeHistoryRequest, channels ...string) error {
	if !validRealtimeHistoryRequest(request) || !validRealtimeRewindChannels(channels) {
		return ErrInvalidInput
	}
	return connection.write(map[string]any{"type": "subscribe", "channels": channels, "rewind": request})
}
func validRealtimeRewindChannels(channels []string) bool {
	return len(channels) <= 10 && validRealtimeChannels(channels)
}
func (connection *RealtimeConn) Unsubscribe(channels ...string) error {
	return connection.channels("unsubscribe", channels)
}
func (connection *RealtimeConn) Publish(channel string, data any, audience RealtimeAudience) error {
	if !realtimeChannel.MatchString(channel) || !validAudience(audience) {
		return ErrInvalidInput
	}
	return connection.write(map[string]any{"type": "channel.publish", "channel": channel, "audience": audience, "data": data})
}
func (connection *RealtimeConn) UpdatePresence(channel string, data any) error {
	if !realtimeChannel.MatchString(channel) {
		return ErrInvalidInput
	}
	return connection.write(map[string]any{"type": "presence.update", "channel": channel, "data": data})
}
func (connection *RealtimeConn) History(channel string, request RealtimeHistoryRequest) error {
	if !realtimeChannel.MatchString(channel) || !validRealtimeHistoryRequest(request) {
		return ErrInvalidInput
	}
	return connection.write(map[string]any{"type": "history.get", "channel": channel, "limit": request.Limit, "cursor": request.Cursor})
}
func (connection *RealtimeConn) PublishBatch(items []RealtimePublishItem) error {
	if !validRealtimePublishBatch(items) {
		return ErrInvalidInput
	}
	return connection.write(map[string]any{"type": "channel.publish.batch", "items": items})
}
func (connection *RealtimeConn) Read() (RealtimeFrame, error) {
	var frame RealtimeFrame
	err := connection.connection.ReadJSON(&frame)
	return frame, err
}
func (connection *RealtimeConn) Close() error { return connection.connection.Close() }
func (connection *RealtimeConn) channels(kind string, channels []string) error {
	if !validRealtimeChannels(channels) {
		return ErrInvalidInput
	}
	return connection.write(map[string]any{"type": kind, "channels": channels})
}
func validRealtimeChannels(channels []string) bool {
	if len(channels) == 0 || len(channels) > 100 {
		return false
	}
	seen := map[string]bool{}
	for _, channel := range channels {
		if !realtimeChannel.MatchString(channel) || seen[channel] {
			return false
		}
		seen[channel] = true
	}
	return true
}
func (connection *RealtimeConn) write(frame any) error {
	connection.writeMu.Lock()
	defer connection.writeMu.Unlock()
	_ = connection.connection.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return connection.connection.WriteJSON(frame)
}
func validRealtimeTokenRequest(request RealtimeTokenRequest) bool {
	if !realtimeClient.MatchString(request.ClientID) || len(request.Channels) == 0 || len(request.Channels) > 100 || request.TTLSeconds < 1 || request.TTLSeconds > 900 {
		return false
	}
	allowed := map[string]bool{"subscribe": true, "publish": true, "presence": true, "history": true, "annotate": true, "file.publish": true, "push.manage": true}
	for channel, actions := range request.Channels {
		if !validRealtimeChannelGrant(channel) || len(actions) == 0 {
			return false
		}
		seen := map[string]bool{}
		for _, action := range actions {
			if !allowed[action] || seen[action] {
				return false
			}
			seen[action] = true
		}
	}
	return true
}
func validRealtimeChannelGrant(grant string) bool {
	complete := func(value string) bool {
		for _, segment := range strings.Split(value, ":") {
			if segment == "" {
				return false
			}
		}
		return true
	}
	if realtimeChannel.MatchString(grant) && complete(grant) {
		return true
	}
	if strings.Count(grant, "*") != 1 || !strings.HasSuffix(grant, ":*") {
		return false
	}
	prefix := strings.TrimSuffix(grant, ":*")
	return prefix != "" && realtimeChannel.MatchString(prefix) && complete(prefix)
}
func validRealtimeHistoryRequest(request RealtimeHistoryRequest) bool {
	return request.Limit >= 1 && request.Limit <= 100 && (request.Cursor == "" || realtimeCursor.MatchString(request.Cursor))
}
func validRealtimePublishBatch(items []RealtimePublishItem) bool {
	if len(items) < 1 || len(items) > 50 {
		return false
	}
	seen := map[string]bool{}
	for _, item := range items {
		if !realtimeClient.MatchString(item.ID) || seen[item.ID] || !realtimeChannel.MatchString(item.Channel) || !jsonObject(item.Data) || item.Audience != nil && !validAudience(*item.Audience) {
			return false
		}
		seen[item.ID] = true
	}
	return true
}
func jsonObject(value any) bool {
	payload, err := json.Marshal(value)
	return err == nil && len(payload) > 1 && payload[0] == '{' && payload[len(payload)-1] == '}'
}
func validAudience(audience RealtimeAudience) bool {
	switch audience.Type {
	case "all", "others":
		return audience.ConnectionID == "" && audience.ClientID == ""
	case "connection":
		return audience.ConnectionID != "" && audience.ClientID == ""
	case "client":
		return realtimeClient.MatchString(audience.ClientID) && audience.ConnectionID == ""
	default:
		return false
	}
}
