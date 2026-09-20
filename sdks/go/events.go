package relayhub

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"time"
)

type EventInput struct {
	Type         string          `json:"type"`
	TargetAppIDs []string        `json:"target_app_ids,omitempty"`
	Data         json.RawMessage `json:"data"`
	Queue        *QueuePublish   `json:"queue,omitempty"`
}
type QueuePublish struct {
	AvailableAt      *time.Time      `json:"available_at,omitempty"`
	DelaySeconds     *int            `json:"delay_seconds,omitempty"`
	OrderingKey      string          `json:"ordering_key,omitempty"`
	Priority         int             `json:"priority,omitempty"`
	DeduplicationKey string          `json:"deduplication_key,omitempty"`
	Metadata         json.RawMessage `json:"metadata,omitempty"`
}
type Event struct {
	ID           string          `json:"id"`
	Type         string          `json:"type"`
	SourceAppID  string          `json:"source_app_id"`
	TargetAppIDs []string        `json:"target_app_ids"`
	Data         json.RawMessage `json:"data"`
	CreatedAt    time.Time       `json:"created_at"`
	Queue        *QueueEvent     `json:"queue,omitempty"`
}
type QueueEvent struct {
	AvailableAt time.Time       `json:"available_at,omitempty"`
	OrderingKey string          `json:"ordering_key,omitempty"`
	Priority    int             `json:"priority,omitempty"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
}
type Job struct {
	ID          string `json:"id"`
	EventID     string `json:"event_id"`
	TargetAppID string `json:"target_app_id"`
	Status      string `json:"status"`
	Attempts    int    `json:"attempts"`
}
type Publication struct {
	Event    Event `json:"event"`
	Jobs     []Job `json:"jobs"`
	Replayed bool  `json:"-"`
}

func (c *Client) Publish(ctx context.Context, event EventInput, options ...CallOption) (Publication, error) {
	if ctx.Err() != nil {
		return Publication{}, ctx.Err()
	}
	key, err := callKey(options)
	if err != nil {
		return Publication{}, err
	}
	if strings.TrimSpace(event.Type) == "" || !object(event.Data) || len(event.TargetAppIDs) > 100 {
		return Publication{}, ErrInvalidInput
	}
	seen := map[string]bool{}
	for _, id := range event.TargetAppIDs {
		if strings.TrimSpace(id) == "" || seen[id] {
			return Publication{}, ErrInvalidInput
		}
		seen[id] = true
	}
	var result Publication
	result.Replayed, err = c.request(ctx, "POST", "/api/v1/events", event, key, &result)
	return result, err
}

func (c *Client) PublishRealtime(ctx context.Context, channel string, data json.RawMessage) error {
	if strings.TrimSpace(channel) == "" || strings.Contains(channel, "/") || !object(data) {
		return ErrInvalidInput
	}
	_, err := c.request(ctx, "POST", "/api/v1/realtime/channels/"+urlPathEscape(channel)+"/publish", struct {
		Data json.RawMessage `json:"data"`
	}{data}, "", nil)
	return err
}

func urlPathEscape(value string) string {
	return strings.ReplaceAll(url.QueryEscape(value), "+", "%20")
}
