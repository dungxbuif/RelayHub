package domain

import "time"

type RealtimeFile struct {
	ID          string     `json:"id"`
	AppID       string     `json:"app_id"`
	Channel     string     `json:"channel"`
	Name        string     `json:"name"`
	MIMEType    string     `json:"mime_type"`
	SizeBytes   int64      `json:"size_bytes"`
	SHA256      string     `json:"sha256"`
	ObjectKey   string     `json:"-"`
	Status      string     `json:"status"`
	CreatedAt   time.Time  `json:"created_at"`
	ExpiresAt   time.Time  `json:"expires_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}
