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

func PrivateRealtimeChannel(channel string) bool {
	return ValidRealtimeChannel(channel) && strings.HasPrefix(channel, "private:") && len(strings.TrimPrefix(channel, "private:")) > 0
}

// ValidRealtimeChannelGrant accepts an exact channel or a namespace grant with
// one terminal wildcard segment. A wildcard never spans ':' separators.
func ValidRealtimeChannelGrant(grant string) bool {
	if ValidRealtimeChannel(grant) && completeRealtimeNamespace(grant) {
		return true
	}
	if !strings.HasSuffix(grant, ":*") || strings.Count(grant, "*") != 1 {
		return false
	}
	prefix := strings.TrimSuffix(grant, ":*")
	return prefix != "" && ValidRealtimeChannel(prefix) && completeRealtimeNamespace(prefix)
}

func RealtimeChannelGrantMatches(grant, channel string) bool {
	if !ValidRealtimeChannel(channel) || !ValidRealtimeChannelGrant(grant) {
		return false
	}
	if grant == channel {
		return true
	}
	prefix, wildcard := strings.CutSuffix(grant, ":*")
	if !wildcard {
		return false
	}
	remainder, found := strings.CutPrefix(channel, prefix+":")
	return found && remainder != "" && !strings.Contains(remainder, ":")
}

func completeRealtimeNamespace(value string) bool {
	for _, segment := range strings.Split(value, ":") {
		if segment == "" {
			return false
		}
	}
	return true
}
