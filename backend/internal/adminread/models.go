package adminread

import (
	"encoding/json"
	"time"
)

type ListOptions struct {
	Limit  int
	Cursor *Cursor
}

func (options ListOptions) Validate() error {
	if options.Limit < 0 || options.Limit > MaxPageSize {
		return ErrInvalidArgument
	}
	return nil
}

func (options ListOptions) NormalizedLimit() int {
	if options.Limit == 0 {
		return DefaultPageSize
	}
	return options.Limit
}

type Page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
}

type EventFilters struct {
	Type, SourceAppID string
	From, To          *time.Time
}

type EventListQuery struct {
	Options ListOptions
	Filters EventFilters
}

type EventSummary struct {
	ID            string    `json:"id"`
	Type          string    `json:"type"`
	SourceAppID   string    `json:"source_app_id"`
	TargetCount   int       `json:"target_count"`
	DeliveryCount int       `json:"delivery_count"`
	CreatedAt     time.Time `json:"created_at"`
}

type EventDetail struct {
	EventSummary
	TargetAppIDs []string        `json:"target_app_ids"`
	Data         json.RawMessage `json:"data"`
}

type DeadLetterFilters struct {
	SourceAppID, TargetAppID, Sink, Reason string
	From, To                               *time.Time
}

type DeadLetterListQuery struct {
	Options ListOptions
	Filters DeadLetterFilters
}

type DeadLetterSummary struct {
	DeliveryID  string    `json:"delivery_id"`
	JobID       string    `json:"job_id"`
	EventID     string    `json:"event_id"`
	SourceAppID string    `json:"source_app_id"`
	TargetAppID string    `json:"target_app_id"`
	Sink        string    `json:"sink"`
	Reason      string    `json:"reason"`
	Attempts    int       `json:"attempts"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type DeadLetterDetail struct {
	DeadLetterSummary
	EventType string `json:"event_type"`
}

type AuditFilters struct {
	ActorType, Action, ResourceType, ResourceID, Outcome string
	From, To                                             *time.Time
}

type AuditListQuery struct {
	Options ListOptions
	Filters AuditFilters
}

type AuditSummary struct {
	ID           int64           `json:"id"`
	OccurredAt   time.Time       `json:"occurred_at"`
	ActorType    string          `json:"actor_type"`
	ActorID      string          `json:"actor_id,omitempty"`
	Action       string          `json:"action"`
	ResourceType string          `json:"resource_type"`
	ResourceID   string          `json:"resource_id,omitempty"`
	Outcome      string          `json:"outcome"`
	Metadata     json.RawMessage `json:"metadata,omitempty"`
}

type DurableCounts struct {
	Pending, Retrying, DeadLetter int64      `json:"pending"`
	OldestPendingAt               *time.Time `json:"oldest_pending_at,omitempty"`
}
