package redisstate

import (
	"errors"
	"regexp"
	"strconv"
	"unicode"
)

var (
	ErrInvalidKeyPart = errors.New("invalid Redis key part")
	keyPrefixPattern  = regexp.MustCompile(`^[a-z0-9:_-]{1,32}$`)
)

type Keyspace struct {
	Prefix string
}

func (k Keyspace) AdminSession(id string) (string, error) {
	if !k.valid() || !validKeyPart(id) {
		return "", ErrInvalidKeyPart
	}
	return k.Prefix + ":{session:" + id + "}:admin", nil
}

func (k Keyspace) RateLimit(appID, dimension, window string) (string, error) {
	if !k.valid() || !validKeyPart(appID) || !validKeyPart(dimension) || !validKeyPart(window) {
		return "", ErrInvalidKeyPart
	}
	return k.Prefix + ":{app:" + appID + "}:rate:" + dimension + ":" + window, nil
}

func (k Keyspace) ConnectionOwner(connectionID string) (string, error) {
	if !k.valid() || !validKeyPart(connectionID) {
		return "", ErrInvalidKeyPart
	}
	return k.Prefix + ":{connection:" + connectionID + "}:owner", nil
}

func (k Keyspace) InstanceMember(instanceID string) (string, error) {
	if !k.valid() || !validKeyPart(instanceID) {
		return "", ErrInvalidKeyPart
	}
	return k.Prefix + ":{instances}:member:" + instanceID, nil
}

func (k Keyspace) InstanceIndex() string {
	if !k.valid() {
		return ""
	}
	return k.Prefix + ":{instances}:live"
}

func (k Keyspace) DashboardBucket(minute int64) (string, error) {
	if !k.valid() || minute <= 0 {
		return "", ErrInvalidKeyPart
	}
	return k.Prefix + ":{metrics}:dashboard:" + strconv.FormatInt(minute, 10), nil
}

func (k Keyspace) DashboardInstance(instanceID string) (string, error) {
	if !k.valid() || !validKeyPart(instanceID) {
		return "", ErrInvalidKeyPart
	}
	return k.Prefix + ":{metrics}:instance:" + instanceID, nil
}

func (k Keyspace) DashboardInstanceIndex() string {
	if !k.valid() {
		return ""
	}
	return k.Prefix + ":{metrics}:instances"
}

func (k Keyspace) RealtimeConnection(appID, connectionID string) (string, error) {
	if !k.valid() || !validKeyPart(appID) || !validKeyPart(connectionID) {
		return "", ErrInvalidKeyPart
	}
	return k.Prefix + ":{app:" + appID + "}:realtime:connection:" + connectionID, nil
}

func (k Keyspace) RealtimeConnectionIndex(appID string) (string, error) {
	if !k.valid() || !validKeyPart(appID) {
		return "", ErrInvalidKeyPart
	}
	return k.Prefix + ":{app:" + appID + "}:realtime:connections", nil
}

func (k Keyspace) RealtimePresence(appID, channel, connectionID string) (string, error) {
	if !k.valid() || !validKeyPart(appID) || !validKeyPart(channel) || !validKeyPart(connectionID) {
		return "", ErrInvalidKeyPart
	}
	return k.Prefix + ":{presence:" + appID + ":" + channel + "}:member:" + connectionID, nil
}

func (k Keyspace) RealtimePresenceIndex(appID, channel string) (string, error) {
	if !k.valid() || !validKeyPart(appID) || !validKeyPart(channel) {
		return "", ErrInvalidKeyPart
	}
	return k.Prefix + ":{presence:" + appID + ":" + channel + "}:members", nil
}

func (k Keyspace) RealtimeHistory(appID, channel string) (string, error) {
	if !k.valid() || !validKeyPart(appID) || !validKeyPart(channel) {
		return "", ErrInvalidKeyPart
	}
	return k.Prefix + ":{history:" + appID + ":" + channel + "}:messages", nil
}

func (k Keyspace) RealtimeMessageActions(appID, channel, messageID string) (string, error) {
	if !k.valid() || !validKeyPart(appID) || !validKeyPart(channel) || !validKeyPart(messageID) {
		return "", ErrInvalidKeyPart
	}
	return k.Prefix + ":{actions:" + appID + ":" + channel + ":" + messageID + "}:state", nil
}

func (k Keyspace) valid() bool {
	return keyPrefixPattern.MatchString(k.Prefix)
}

func validKeyPart(part string) bool {
	if part == "" || len(part) > 128 {
		return false
	}
	for _, r := range part {
		if r == '{' || r == '}' || unicode.IsSpace(r) || unicode.IsControl(r) {
			return false
		}
	}
	return true
}
