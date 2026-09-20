package relayhub

import (
	"context"
	"encoding/json"
	"net/http"
)

type PushDevice struct {
	ID       string `json:"id"`
	AppID    string `json:"app_id"`
	Provider string `json:"provider"`
}
type PushNotification struct {
	Title string          `json:"title"`
	Body  string          `json:"body"`
	Data  json.RawMessage `json:"data,omitempty"`
}
type PushOutcome struct {
	ID                string `json:"id"`
	DeviceID          string `json:"device_id"`
	Channel           string `json:"channel"`
	Provider          string `json:"provider"`
	Status            string `json:"status"`
	ProviderMessageID string `json:"provider_message_id,omitempty"`
	Reason            string `json:"reason,omitempty"`
}

func (c *Client) RegisterPushDevice(ctx context.Context, provider, token string) (PushDevice, error) {
	var result PushDevice
	_, err := c.request(ctx, http.MethodPost, "/api/v2/realtime/push/devices", map[string]string{"provider": provider, "token": token}, "", &result)
	return result, err
}
func (c *Client) DeletePushDevice(ctx context.Context, id string) error {
	_, err := c.request(ctx, http.MethodDelete, "/api/v2/realtime/push/devices/"+urlPathEscape(id), nil, "", nil)
	return err
}
func (c *Client) BindPushDevice(ctx context.Context, channel, id string, bind bool) error {
	method := http.MethodPut
	if !bind {
		method = http.MethodDelete
	}
	_, err := c.request(ctx, method, "/api/v2/realtime/push/channels/"+urlPathEscape(channel)+"/devices/"+urlPathEscape(id), nil, "", nil)
	return err
}
func (c *Client) PublishPush(ctx context.Context, channel string, notification PushNotification) ([]PushOutcome, error) {
	var result struct {
		Outcomes []PushOutcome `json:"outcomes"`
	}
	_, err := c.request(ctx, http.MethodPost, "/api/v2/realtime/push/channels/"+urlPathEscape(channel)+"/notifications", notification, "", &result)
	return result.Outcomes, err
}
