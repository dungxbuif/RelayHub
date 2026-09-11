package domain

import "time"

type JobStatus string

const (
	JobPending    JobStatus = "pending"
	JobLeased     JobStatus = "leased"
	JobDelivered  JobStatus = "delivered"
	JobAcked      JobStatus = "acked"
	JobDeadLetter JobStatus = "dead_letter"
)

type Job struct {
	Callback           bool       `json:"callback,omitempty"`
	CallbackAttempts   int        `json:"callback_attempts,omitempty"`
	CallbackGeneration int        `json:"callback_generation,omitempty"`
	RetryAt            *time.Time `json:"retry_at,omitempty"`
	LastReason         string     `json:"last_reason,omitempty"`
	ID                 string     `json:"id"`
	EventID            string     `json:"event_id"`
	SourceAppID        string     `json:"source_app_id"`
	TargetAppID        string     `json:"target_app_id"`
	Status             JobStatus  `json:"status"`
	Attempts           int        `json:"attempts"`
	LeaseUntil         *time.Time `json:"lease_until,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

func (status JobStatus) CanTransition(next JobStatus) bool {
	switch status {
	case JobPending:
		return next == JobLeased || next == JobAcked || next == JobDeadLetter
	case JobLeased:
		return next == JobPending || next == JobDelivered || next == JobAcked || next == JobDeadLetter
	case JobDelivered:
		return next == JobAcked || next == JobDeadLetter
	case JobAcked:
		return next == JobAcked
	case JobDeadLetter:
		return next == JobPending || next == JobDeadLetter
	default:
		return false
	}
}
