package domain

import (
	"encoding/json"
	"time"
)

type Event struct {
	ID           string          `json:"id"`
	Type         string          `json:"type"`
	SourceAppID  string          `json:"source_app_id"`
	TargetAppIDs []string        `json:"target_app_ids"`
	Data         json.RawMessage `json:"data"`
	CreatedAt    time.Time       `json:"created_at"`
}

func (event Event) CanRead(actor string) bool {
	if actor == event.SourceAppID {
		return true
	}
	for _, target := range event.TargetAppIDs {
		if target == actor {
			return true
		}
	}
	return false
}
