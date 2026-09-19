package adminread

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

const MaxReplayBatch = 100

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
	Pending              int64      `json:"pending"`
	Retrying             int64      `json:"retrying"`
	DeadLetter           int64      `json:"dead_letter"`
	OldestPendingAt      *time.Time `json:"oldest_pending_at,omitempty"`
	DeliveryLatencyP50MS *float64   `json:"delivery_latency_p50_ms,omitempty"`
	DeliveryLatencyP95MS *float64   `json:"delivery_latency_p95_ms,omitempty"`
	DeliveryLatencyP99MS *float64   `json:"delivery_latency_p99_ms,omitempty"`
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

type DeliveryAttemptSummary struct {
	DeliveryID string    `json:"delivery_id"`
	Generation int64     `json:"generation"`
	Attempt    int       `json:"attempt"`
	Outcome    string    `json:"outcome"`
	Reason     string    `json:"reason,omitempty"`
	StartedAt  time.Time `json:"started_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type TimelineItem struct {
	ID         string    `json:"id"`
	Type       string    `json:"type"`
	OccurredAt time.Time `json:"occurred_at"`
	EventID    string    `json:"event_id"`
	DeliveryID string    `json:"delivery_id,omitempty"`
	JobID      string    `json:"job_id,omitempty"`
	Generation int64     `json:"generation,omitempty"`
	Attempt    int       `json:"attempt,omitempty"`
	Outcome    string    `json:"outcome,omitempty"`
	Reason     string    `json:"reason,omitempty"`
	ActorType  string    `json:"actor_type,omitempty"`
	ActorID    string    `json:"actor_id,omitempty"`
}

type EventTimeline struct {
	Event      EventDetail              `json:"event"`
	Deliveries []DeadLetterSummary      `json:"deliveries"`
	Attempts   []DeliveryAttemptSummary `json:"attempts"`
	Items      []TimelineItem           `json:"items"`
}

type ReplayCommand struct {
	DeliveryIDs        []string
	IdempotencyKeyHash string
	RequestFingerprint string
	ActorID            string
	Now                time.Time
}

func (command ReplayCommand) Validate() error {
	if len(command.DeliveryIDs) < 1 || len(command.DeliveryIDs) > MaxReplayBatch || !isHexDigest(command.IdempotencyKeyHash) || !isHexDigest(command.RequestFingerprint) || command.Now.IsZero() || !validLifecycleValue(command.ActorID) {
		return ErrInvalidArgument
	}
	seen := make(map[string]struct{}, len(command.DeliveryIDs))
	for _, id := range command.DeliveryIDs {
		if !validLifecycleValue(id) {
			return ErrInvalidArgument
		}
		if _, exists := seen[id]; exists {
			return ErrInvalidArgument
		}
		seen[id] = struct{}{}
	}
	return nil
}

type ReplayItem struct {
	DeliveryID     string `json:"delivery_id"`
	EventID        string `json:"event_id"`
	JobID          string `json:"job_id"`
	FromGeneration int64  `json:"from_generation"`
	Generation     int64  `json:"generation"`
	Status         string `json:"status"`
}

type ReplayResult struct {
	Items []ReplayItem `json:"items"`
}

func isHexDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
			return false
		}
	}
	return true
}

func validLifecycleValue(value string) bool {
	return value != "" && len(value) <= 256 && strings.TrimSpace(value) == value
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
