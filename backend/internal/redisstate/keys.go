package redisstate

import (
	"errors"
	"regexp"
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
