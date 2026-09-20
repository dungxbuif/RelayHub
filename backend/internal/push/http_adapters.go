package push

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
)

type HTTPAdapter struct {
	provider, endpoint, authorization, topic, project string
	client                                            *http.Client
}

func NewAPNSAdapter(endpoint, authorization, topic string, client *http.Client) (*HTTPAdapter, error) {
	return newAdapter("apns", endpoint, authorization, topic, "", client)
}
func NewFCMAdapter(endpoint, authorization, project string, client *http.Client) (*HTTPAdapter, error) {
	return newAdapter("fcm", endpoint, authorization, "", project, client)
}
func newAdapter(provider, endpoint, authorization, topic, project string, client *http.Client) (*HTTPAdapter, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || authorization == "" || (provider == "apns" && topic == "") || (provider == "fcm" && project == "") {
		return nil, errors.New("invalid push adapter configuration")
	}
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &HTTPAdapter{provider: provider, endpoint: strings.TrimRight(endpoint, "/"), authorization: authorization, topic: topic, project: project, client: client}, nil
}
func (adapter *HTTPAdapter) Send(ctx context.Context, device domain.PushDevice, notification domain.PushNotification) (string, error) {
	if adapter == nil || device.Provider != adapter.provider {
		return "", errors.New("push provider mismatch")
	}
	var endpoint string
	var payload any
	if adapter.provider == "apns" {
		endpoint = adapter.endpoint + "/3/device/" + url.PathEscape(string(device.Token))
		payload = map[string]any{"aps": map[string]any{"alert": map[string]string{"title": notification.Title, "body": notification.Body}}, "data": json.RawMessage(notification.Data)}
	} else {
		endpoint = adapter.endpoint + "/v1/projects/" + url.PathEscape(adapter.project) + "/messages:send"
		payload = map[string]any{"message": map[string]any{"token": string(device.Token), "notification": map[string]string{"title": notification.Title, "body": notification.Body}, "data": json.RawMessage(notification.Data)}}
	}
	raw, _ := json.Marshal(payload)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return "", errors.New("create push request")
	}
	request.Header.Set("Authorization", adapter.authorization)
	request.Header.Set("Content-Type", "application/json")
	if adapter.provider == "apns" {
		request.Header.Set("apns-topic", adapter.topic)
	}
	response, err := adapter.client.Do(request)
	if err != nil {
		return "", errors.New("push transport")
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", errors.New("push provider response")
	}
	if adapter.provider == "apns" {
		return response.Header.Get("apns-id"), nil
	}
	var decoded struct {
		Name string `json:"name"`
	}
	_ = json.Unmarshal(body, &decoded)
	return decoded.Name, nil
}
