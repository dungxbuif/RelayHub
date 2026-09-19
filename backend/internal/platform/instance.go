package platform

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
)

var ErrInvalidInstance = errors.New("invalid runtime instance")

type Instance struct {
	ID         string
	Role       string
	Generation uint64
	StartedAt  time.Time
}

func NewInstance(role, configuredID string, now time.Time) (Instance, error) {
	if (role != "api" && role != "worker") || now.IsZero() {
		return Instance{}, ErrInvalidInstance
	}
	id := strings.TrimSpace(configuredID)
	if id == "" {
		id = role + "_" + strings.ToLower(uuid.NewString())
	}
	if !validInstanceID(id) {
		return Instance{}, ErrInvalidInstance
	}
	var random [8]byte
	for {
		if _, err := rand.Read(random[:]); err != nil {
			return Instance{}, errors.New("generate runtime instance generation")
		}
		generation := binary.BigEndian.Uint64(random[:])
		if generation != 0 {
			return Instance{ID: id, Role: role, Generation: generation, StartedAt: now}, nil
		}
	}
}

func validInstanceID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for _, r := range id {
		if r == '{' || r == '}' || unicode.IsSpace(r) || unicode.IsControl(r) {
			return false
		}
	}
	return true
}
