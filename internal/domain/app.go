package domain

import "time"

type DeliveryMode string

const (
	DeliveryQueue     DeliveryMode = "queue"
	DeliveryWebSocket DeliveryMode = "websocket"
	DeliveryCallback  DeliveryMode = "callback"
	DeliveryAll       DeliveryMode = "all"
)

func (mode DeliveryMode) Valid() bool {
	switch mode {
	case DeliveryQueue, DeliveryWebSocket, DeliveryCallback, DeliveryAll:
		return true
	default:
		return false
	}
}

type App struct {
	ID           string       `json:"id"`
	Name         string       `json:"name"`
	CallbackURL  *string      `json:"callback_url,omitempty"`
	DeliveryMode DeliveryMode `json:"delivery_mode"`
	Enabled      bool         `json:"enabled"`
	CreatedAt    time.Time    `json:"created_at"`
	UpdatedAt    time.Time    `json:"updated_at"`
}
