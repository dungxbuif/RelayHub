package domain

import (
	"encoding/json"
	"time"
)

type PushDevice struct {
	ID        string    `json:"id"`
	AppID     string    `json:"app_id"`
	Provider  string    `json:"provider"`
	Token     []byte    `json:"-"`
	TokenHash string    `json:"-"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
type PushNotification struct {
	Title string          `json:"title"`
	Body  string          `json:"body"`
	Data  json.RawMessage `json:"data,omitempty"`
}
type PushOutcome struct {
	ID                string    `json:"id"`
	AppID             string    `json:"app_id"`
	DeviceID          string    `json:"device_id"`
	Channel           string    `json:"channel"`
	Provider          string    `json:"provider"`
	Status            string    `json:"status"`
	ProviderMessageID string    `json:"provider_message_id,omitempty"`
	Reason            string    `json:"reason,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
}
