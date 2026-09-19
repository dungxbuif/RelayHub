package domain

import (
	"encoding/json"
	"regexp"
	"strings"
	"time"
)

var realtimeChannelPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.:-]{0,95}$`)

type RoutingRule struct {
	ID              string     `json:"id"`
	SourceAppID     *string    `json:"source_app_id,omitempty"`
	EventType       string     `json:"event_type"`
	TargetAppID     string     `json:"target_app_id"`
	RealtimeChannel *string    `json:"realtime_channel,omitempty"`
	Enabled         bool       `json:"enabled"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
	DeletedAt       *time.Time `json:"-"`
}

type ChannelMessage struct {
	Channel        string          `json:"channel"`
	PublisherAppID string          `json:"publisher_app_id"`
	Data           json.RawMessage `json:"data"`
}

func ValidRealtimeChannel(channel string) bool {
	return channel == strings.TrimSpace(channel) && realtimeChannelPattern.MatchString(channel)
}
