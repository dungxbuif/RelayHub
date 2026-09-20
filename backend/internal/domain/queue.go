package domain

import (
	"encoding/json"
	"time"
)

type QueueOrderingMode string

const (
	QueueOrderingNone QueueOrderingMode = "none"
	QueueOrderingKey  QueueOrderingMode = "key"
)

type QueueSubscription struct {
	ID                       string            `json:"id"`
	AppID                    string            `json:"app_id"`
	Name                     string            `json:"name"`
	Enabled                  bool              `json:"enabled"`
	PausedAt                 *time.Time        `json:"paused_at,omitempty"`
	DrainingAt               *time.Time        `json:"draining_at,omitempty"`
	DrainDeadlineAt          *time.Time        `json:"drain_deadline_at,omitempty"`
	DrainedAt                *time.Time        `json:"drained_at,omitempty"`
	EventTypes               []string          `json:"event_types,omitempty"`
	MaxAttempts              int               `json:"max_attempts"`
	DefaultVisibilitySeconds int               `json:"default_visibility_seconds"`
	MaxVisibilitySeconds     int               `json:"max_visibility_seconds"`
	MaxTotalLeaseSeconds     int               `json:"max_total_lease_seconds"`
	RetentionSeconds         int               `json:"retention_seconds"`
	MaxInFlight              int               `json:"max_in_flight"`
	MaxBatchSize             int               `json:"max_batch_size"`
	RetryDelaySeconds        int               `json:"retry_delay_seconds"`
	OrderingMode             QueueOrderingMode `json:"ordering_mode"`
	DeduplicationSeconds     int               `json:"deduplication_seconds"`
	MaxDispatchRate          *int              `json:"max_dispatch_rate,omitempty"`
	SuccessCallbackURL       *string           `json:"success_callback_url,omitempty"`
	FailureCallbackURL       *string           `json:"failure_callback_url,omitempty"`
	ResultCallbackMetadata   json.RawMessage   `json:"result_callback_metadata,omitempty"`
	PolicyVersion            int64             `json:"policy_version"`
	CreatedAt                time.Time         `json:"created_at"`
	UpdatedAt                time.Time         `json:"updated_at"`
}

type QueueDrain struct {
	SubscriptionID string     `json:"subscription_id"`
	Status         string     `json:"status"`
	InFlight       int64      `json:"in_flight"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	DeadlineAt     *time.Time `json:"deadline_at,omitempty"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
}

type QueueSchedule struct {
	ID              string          `json:"id"`
	AppID           string          `json:"app_id"`
	SubscriptionID  string          `json:"subscription_id"`
	Name            string          `json:"name"`
	Enabled         bool            `json:"enabled"`
	CronExpression  string          `json:"cron_expression"`
	Timezone        string          `json:"timezone"`
	EventType       string          `json:"event_type"`
	Data            json.RawMessage `json:"data"`
	OrderingKey     string          `json:"ordering_key,omitempty"`
	Priority        int             `json:"priority"`
	Metadata        json.RawMessage `json:"metadata,omitempty"`
	NextRunAt       time.Time       `json:"next_run_at"`
	LastRunAt       *time.Time      `json:"last_run_at,omitempty"`
	PolicyVersion   int64           `json:"policy_version"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	ClaimToken      string          `json:"-"`
	ClaimGeneration int64           `json:"-"`
}

type QueueDelivery struct {
	ID             string          `json:"id"`
	SubscriptionID string          `json:"subscription_id"`
	Event          Event           `json:"event"`
	Receipt        string          `json:"receipt"`
	Attempt        int             `json:"attempt"`
	Generation     int64           `json:"generation"`
	LeaseExpiresAt time.Time       `json:"lease_expires_at"`
	OrderingKey    string          `json:"ordering_key,omitempty"`
	Priority       int             `json:"priority"`
	Metadata       json.RawMessage `json:"metadata,omitempty"`
}

type QueueDepth struct {
	Available       int64      `json:"available"`
	InFlight        int64      `json:"in_flight"`
	Acknowledged    int64      `json:"acknowledged"`
	DeadLetter      int64      `json:"dead_letter"`
	OldestAvailable *time.Time `json:"oldest_available_at,omitempty"`
}

type QueueDeadLetter struct {
	DeliveryID     string    `json:"delivery_id"`
	SubscriptionID string    `json:"subscription_id"`
	EventID        string    `json:"event_id"`
	Attempts       int       `json:"attempts"`
	Generation     int64     `json:"generation"`
	Reason         string    `json:"reason,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type QueueResultCallback struct {
	ID, AppID, SubscriptionID, DeliveryID, EventID string
	Generation                                     int64
	Outcome                                        string
	URL                                            string
	Body                                           []byte
	Secret                                         []byte
	Attempt                                        int
	ClaimToken                                     string
	ClaimGeneration                                int64
	ClaimExpiresAt                                 time.Time
}

type QueueCallbackOutcome struct {
	ID, SubscriptionID, DeliveryID, EventID string
	Generation                              int64
	Outcome, Status, Reason                 string
	Attempts                                int
	CreatedAt, UpdatedAt                    time.Time
}
