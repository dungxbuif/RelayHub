package relayhub

import (
	"context"
	"net/http"
	"time"
)

type RealtimeFileInput struct {
	Channel   string `json:"channel"`
	Name      string `json:"name"`
	MIMEType  string `json:"mime_type"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}
type RealtimeFile struct {
	ID          string     `json:"id"`
	AppID       string     `json:"app_id"`
	Channel     string     `json:"channel"`
	Name        string     `json:"name"`
	MIMEType    string     `json:"mime_type"`
	SizeBytes   int64      `json:"size_bytes"`
	SHA256      string     `json:"sha256"`
	Status      string     `json:"status"`
	CreatedAt   time.Time  `json:"created_at"`
	ExpiresAt   time.Time  `json:"expires_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}
type RealtimeFileUpload struct {
	File            RealtimeFile      `json:"file"`
	UploadURL       string            `json:"upload_url"`
	RequiredHeaders map[string]string `json:"required_headers"`
}
type RealtimeFileDownload struct {
	File        RealtimeFile `json:"file"`
	DownloadURL string       `json:"download_url"`
}

func (c *Client) CreateRealtimeFile(ctx context.Context, input RealtimeFileInput) (RealtimeFileUpload, error) {
	var result RealtimeFileUpload
	if !realtimeChannel.MatchString(input.Channel) || input.Name == "" || input.SizeBytes < 1 || input.SHA256 == "" {
		return result, ErrInvalidInput
	}
	_, err := c.request(ctx, http.MethodPost, "/api/v2/realtime/files", input, "", &result)
	return result, err
}
func (c *Client) CompleteRealtimeFile(ctx context.Context, id string) (RealtimeFile, error) {
	var result RealtimeFile
	_, err := c.request(ctx, http.MethodPost, "/api/v2/realtime/files/"+urlPathEscape(id)+"/complete", nil, "", &result)
	return result, err
}
func (c *Client) GetRealtimeFileDownload(ctx context.Context, id string) (RealtimeFileDownload, error) {
	var result RealtimeFileDownload
	_, err := c.request(ctx, http.MethodGet, "/api/v2/realtime/files/"+urlPathEscape(id)+"/download", nil, "", &result)
	return result, err
}
