package store

import (
	"context"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
)

// CallbackDispatchDisposition tells a JetStream callback worker whether it may
// make an HTTP request. PostgreSQL is authoritative; the broker only wakes work.
type CallbackDispatchDisposition string

const (
	CallbackDispatchReady      CallbackDispatchDisposition = "ready"
	CallbackDispatchBusy       CallbackDispatchDisposition = "busy"
	CallbackDispatchComplete   CallbackDispatchDisposition = "complete"
	CallbackDispatchDeadLetter CallbackDispatchDisposition = "dead_letter"
)

// CallbackDispatch is a freshly validated, token-fenced callback attempt.
// Secret and endpoint are read from the current application version immediately
// before dispatch and must never be serialized to the broker.
type CallbackDispatch struct {
	DeliveryID, PublicJobID, TargetAppID, Token string
	Attempt, CredentialVersion                  int
	Generation                                  int64
	LeaseExpiresAt, RetryAt                     time.Time
	App                                         domain.App
	Event                                       domain.Event
	Body, Secret                                []byte
	Reason                                      string
	DLQPublished                                bool
}

type CallbackAttemptTransition struct {
	Status       domain.JobStatus
	Now, RetryAt time.Time
	Reason       string
}

type CallbackAttemptStore interface {
	BeginCallbackAttempt(context.Context, string, int64, string, time.Time, time.Duration) (CallbackDispatch, CallbackDispatchDisposition, error)
	FinishCallbackAttempt(context.Context, string, int64, string, int, CallbackAttemptTransition) error
	MarkCallbackDLQPublished(context.Context, string, int64, time.Time) error
}
