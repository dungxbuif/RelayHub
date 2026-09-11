package store

import (
	"context"
	"errors"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
)

var (
	ErrNotFound      = errors.New("store record not found")
	ErrConflict      = errors.New("store record conflict")
	ErrInvalidTarget = errors.New("invalid target application")
)

type HealthChecker interface {
	Ping(context.Context) error
}

type AppCredential struct {
	AppID      string
	APIKeyHash string
	HMACSecret []byte
}

type ApplicationStore interface {
	CreateApplication(context.Context, domain.App, AppCredential) error
	ListApplications(context.Context) ([]domain.App, error)
	GetApplication(context.Context, string) (domain.App, error)
	UpdateApplication(context.Context, domain.App) (domain.App, error)
	DisableApplication(context.Context, string, time.Time) (domain.App, error)
	FindCredentialByAPIKeyHash(context.Context, string) (AppCredential, error)
	RotateApplicationCredential(context.Context, string, AppCredential, time.Time) error
}

type Store interface {
	HealthChecker
	ApplicationStore
}

// EventRetention applies event TTL at publish and job TTL on terminal transitions.
type EventRetention struct{ Event, Job, Idempotency time.Duration }
type Publication struct {
	Event domain.Event `json:"event"`
	Jobs  []domain.Job `json:"jobs"`
}
type LeasedEvent struct {
	Event domain.Event `json:"event"`
	Job   domain.Job   `json:"job"`
}
type ApplicationReader interface {
	GetApplication(context.Context, string) (domain.App, error)
}
type EventStore interface {
	FindPublication(context.Context, string, string) (Publication, error)
	PublishEvent(context.Context, Publication, string, EventRetention) (Publication, bool, error)
	GetEvent(context.Context, string) (domain.Event, error)
	GetJob(context.Context, string) (domain.Job, error)
	LeaseJobs(context.Context, string, int, time.Time, time.Duration) ([]LeasedEvent, error)
	AckEvent(context.Context, string, string, time.Time, time.Duration) error
	TransitionJob(context.Context, string, domain.JobStatus, time.Time, time.Duration) (domain.Job, error)
}

// EventJobReader resolves a target-owned job after a durable acknowledgement.
type EventJobReader interface {
	GetEventJob(context.Context, string, string) (domain.Job, error)
}

// CallbackFinishMargin leaves time for durable persistence before lease expiry.
const CallbackFinishMargin = time.Second

// CallbackClaim's token fences an active attempt; generation fences old stream entries.
type CallbackClaim struct {
	ExpiresAt               time.Time
	MessageID, JobID, Token string
	Generation              int
}
type CallbackData struct {
	Job          domain.Job
	App          domain.App
	Event        domain.Event
	Body, Secret []byte
}
type CallbackTransition struct {
	Status       domain.JobStatus
	Now, RetryAt time.Time
	Reason       string
	Disable      bool
}
type CallbackStore interface {
	ClaimCallback(context.Context, string, time.Duration, time.Duration) (CallbackClaim, error)
	LoadCallback(context.Context, CallbackClaim) (CallbackData, error)
	StartCallback(context.Context, CallbackClaim, time.Duration) (domain.Job, error)
	FinishCallback(context.Context, CallbackClaim, CallbackTransition) (domain.Job, error)
	AckCallback(context.Context, CallbackClaim) error
	PromoteCallbacks(context.Context, time.Time, int) error
}
