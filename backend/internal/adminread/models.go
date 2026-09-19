package adminread

import (
	"encoding/json"
	"strconv"
	"strings"
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

func (options ListOptions) ValidateCursor(fingerprint string) error {
	if err := options.Validate(); err != nil {
		return err
	}
	if options.Cursor != nil && (options.Cursor.Version != CursorVersion || options.Cursor.FilterFingerprint != fingerprint || options.Cursor.Timestamp.IsZero() || options.Cursor.ID == "") {
		return ErrInvalidCursor
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

func (filters EventFilters) Fingerprint() string {
	return FilterFingerprint("event", filters.Type, filters.SourceAppID, normalizedTime(filters.From), normalizedTime(filters.To))
}

func (filters EventFilters) Validate() error {
	return validateFilterRange(filters.From, filters.To, filters.Type, filters.SourceAppID)
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

func (filters DeadLetterFilters) Fingerprint() string {
	return FilterFingerprint("dlq", filters.SourceAppID, filters.TargetAppID, filters.Sink, filters.Reason, normalizedTime(filters.From), normalizedTime(filters.To))
}

func (filters DeadLetterFilters) Validate() error {
	if filters.Sink != "" && filters.Sink != "stream" && filters.Sink != "callback" {
		return ErrInvalidArgument
	}
	return validateFilterRange(filters.From, filters.To, filters.SourceAppID, filters.TargetAppID, filters.Reason)
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

func (filters AuditFilters) Fingerprint() string {
	return FilterFingerprint("audit", filters.ActorType, filters.Action, filters.ResourceType, filters.ResourceID, filters.Outcome, normalizedTime(filters.From), normalizedTime(filters.To))
}

func (filters AuditFilters) Validate() error {
	return validateFilterRange(filters.From, filters.To, filters.ActorType, filters.Action, filters.ResourceType, filters.ResourceID, filters.Outcome)
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

type MetricPoint struct {
	At     time.Time        `json:"at"`
	Values map[string]int64 `json:"values"`
}

type InstanceSummary struct {
	InstanceID    string    `json:"instance_id"`
	Connections   int64     `json:"connections"`
	NATSConnected bool      `json:"nats_connected"`
	NATSChangedAt time.Time `json:"nats_changed_at"`
	HeartbeatAt   time.Time `json:"heartbeat_at"`
}

type MetricsSnapshot struct {
	GeneratedAt        time.Time         `json:"generated_at"`
	WindowSeconds      int64             `json:"window_seconds"`
	StepSeconds        int64             `json:"step_seconds"`
	Series             []MetricPoint     `json:"series"`
	Instances          []InstanceSummary `json:"instances"`
	ActiveConnections  int64             `json:"active_connections"`
	DegradedComponents []string          `json:"degraded_components"`
}

type DashboardSnapshot struct {
	MetricsSnapshot
	Durable DurableCounts `json:"durable"`
}

func normalizedTime(value *time.Time) string {
	if value == nil {
		return ""
	}
	return strconv.FormatInt(value.UTC().UnixMicro(), 10)
}

func validateFilterRange(from, to *time.Time, values ...string) error {
	if from != nil && to != nil && from.After(*to) {
		return ErrInvalidArgument
	}
	for _, value := range values {
		if len(value) > 256 || strings.TrimSpace(value) != value {
			return ErrInvalidArgument
		}
	}
	return nil
}
