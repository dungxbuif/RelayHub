package store

import (
	"context"
	"errors"
	"time"

	"github.com/dungxbuif/RelayHub/internal/adminread"
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
	Version    int64
	RevokedAt  *time.Time
}

type ApplicationStore interface {
	CreateApplication(context.Context, domain.App, AppCredential) error
	ListApplications(context.Context) ([]domain.App, error)
	GetApplication(context.Context, string) (domain.App, error)
	UpdateApplication(context.Context, domain.App) (domain.App, error)
	// CompareAndSwapApplication rejects stale editable fields with ErrConflict.
	CompareAndSwapApplication(context.Context, domain.App, domain.App) (domain.App, error)
	DisableApplication(context.Context, string, time.Time) (domain.App, error)
	FindCredentialByAPIKeyHash(context.Context, string) (AppCredential, error)
	RotateApplicationCredential(context.Context, string, AppCredential, time.Time) error
}

type Store interface {
	HealthChecker
	ApplicationStore
}

// AdminReadStore exposes bounded cross-application operational views. It is
// deliberately separate from mutation interfaces so read-only consumers cannot
// accidentally gain control-plane write authority.
type AdminReadStore interface {
	ListAdminEvents(context.Context, adminread.EventListQuery) (adminread.Page[adminread.EventSummary], error)
	GetAdminEvent(context.Context, string) (adminread.EventDetail, error)
	ListAdminDeadLetters(context.Context, adminread.DeadLetterListQuery) (adminread.Page[adminread.DeadLetterSummary], error)
	GetAdminDeadLetter(context.Context, string) (adminread.DeadLetterDetail, error)
	ListAdminAudit(context.Context, adminread.AuditListQuery) (adminread.Page[adminread.AuditSummary], error)
	AdminDurableCounts(context.Context) (adminread.DurableCounts, error)
}

// AdminLifecycleStore owns generation-fenced replay and persisted timeline
// reconstruction. It is separate from read-only Admin consumers.
type AdminLifecycleStore interface {
	GetAdminEventTimeline(context.Context, string) (adminread.EventTimeline, error)
	ReplayAdminDeadLetters(context.Context, adminread.ReplayCommand) (adminread.ReplayResult, bool, error)
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

type RoutingRuleStore interface {
	CreateRoutingRule(context.Context, domain.RoutingRule) error
	ListRoutingRules(context.Context) ([]domain.RoutingRule, error)
	GetRoutingRule(context.Context, string) (domain.RoutingRule, error)
	UpdateRoutingRule(context.Context, domain.RoutingRule) (domain.RoutingRule, error)
	DeleteRoutingRule(context.Context, string, time.Time) error
	ResolveRoutingRules(context.Context, string, string) ([]domain.RoutingRule, error)
}

type EventPublisher interface {
	FindPublication(context.Context, string, string) (Publication, error)
	PublishEvent(context.Context, Publication, string, EventRetention) (Publication, bool, error)
}

type EventReader interface {
	GetEvent(context.Context, string) (domain.Event, error)
	GetJob(context.Context, string) (domain.Job, error)
}

type DeliveryManager interface {
	LeaseJobs(context.Context, string, int, time.Time, time.Duration) ([]LeasedEvent, error)
	AckEvent(context.Context, string, string, time.Time, time.Duration) error
	TransitionJob(context.Context, string, domain.JobStatus, time.Time, time.Duration) (domain.Job, error)
}

type EventStore interface {
	EventPublisher
	EventReader
	DeliveryManager
}

type QueuePullRequest struct {
	AppID, SubscriptionID string
	Limit                 int
	Visibility            time.Duration
	Now                   time.Time
	Receipts              []string
}

type QueueSettlementDisposition string

const (
	QueueAcknowledge QueueSettlementDisposition = "ack"
	QueueRetry       QueueSettlementDisposition = "retry"
	QueueDeadLetter  QueueSettlementDisposition = "dead_letter"
)

type QueueSettlement struct {
	Receipt     string
	Disposition QueueSettlementDisposition
	Delay       time.Duration
	Reason      string
}

type QueueSettlementResult struct {
	Receipt string `json:"receipt"`
	Status  string `json:"status"`
}

type QueueLeaseExtension struct {
	Receipt   string
	Extension time.Duration
}

type QueueDeadLetterQuery struct {
	Limit  int
	Cursor string
}

type QueueRepository interface {
	CreateQueueSubscription(context.Context, domain.QueueSubscription) error
	ListQueueSubscriptions(context.Context, string) ([]domain.QueueSubscription, error)
	GetQueueSubscription(context.Context, string, string) (domain.QueueSubscription, error)
	UpdateQueueSubscription(context.Context, domain.QueueSubscription, int64) (domain.QueueSubscription, error)
	DeleteQueueSubscription(context.Context, string, string) error
	PullQueueDeliveries(context.Context, QueuePullRequest) ([]domain.QueueDelivery, error)
	SettleQueueDeliveries(context.Context, string, string, []QueueSettlement, time.Time) ([]QueueSettlementResult, error)
	ExtendQueueLeases(context.Context, string, string, []QueueLeaseExtension, time.Time) ([]QueueSettlementResult, error)
	QueueDepth(context.Context, string, string, time.Time) (domain.QueueDepth, error)
	ListQueueDeadLetters(context.Context, string, string, QueueDeadLetterQuery) ([]domain.QueueDeadLetter, error)
	ReplayQueueDeadLetters(context.Context, string, string, []string, time.Time) (int, error)
	DeleteQueueDeadLetters(context.Context, string, string, []string) (int, error)
}

type RealtimeFileRepository interface {
	CreateRealtimeFile(context.Context, domain.RealtimeFile) error
	GetRealtimeFile(context.Context, string, string) (domain.RealtimeFile, error)
	CompleteRealtimeFile(context.Context, string, string, time.Time) (domain.RealtimeFile, error)
}

type DeliveryAssignmentDisposition string

const (
	DeliveryAssigned        DeliveryAssignmentDisposition = "assigned"
	DeliveryAlreadyAssigned DeliveryAssignmentDisposition = "already_assigned"
	DeliveryAlreadyComplete DeliveryAssignmentDisposition = "already_complete"
)

type DeliveryAssignment struct {
	DeliveryID, TargetAppID, ConnectionID, Token string
	Attempt                                      int
	Generation                                   int64
	ExpiresAt                                    time.Time
}

type DeliveryAssignmentStore interface {
	AssignStreamDelivery(context.Context, string, string, string, string, int64, time.Time, time.Duration, time.Duration) (DeliveryAssignment, DeliveryAssignmentDisposition, error)
	AcknowledgeStreamDelivery(context.Context, string, string, string, string, int64, time.Time) error
	ReleaseStreamDelivery(context.Context, string, string, string, string, int64, time.Time) error
	ProgressStreamDelivery(context.Context, string, string, string, string, int64, time.Time, time.Duration) error
}

type OutboxMessage struct {
	ID, EventID, DeliveryID string
	Subject, MessageID      string
	Payload                 []byte
	ClaimToken              string
	Attempts                int64
	Reclaimed               bool
	CreatedAt               time.Time
}

// OutboxPublishStart is persisted before the broker call. Exhausted means the
// row was atomically moved to its terminal state without making another call.
type OutboxPublishStart struct {
	Attempt   int64
	Exhausted bool
}

type OutboxStats struct {
	Pending, Claimed, Failed int64
	OldestPendingAt          *time.Time
}

type OutboxStore interface {
	ClaimOutbox(context.Context, time.Time, time.Time, string, int) ([]OutboxMessage, error)
	BeginOutboxPublish(context.Context, string, string, time.Time, int64) (OutboxPublishStart, error)
	MarkOutboxDispatched(context.Context, string, string, time.Time) error
	RetryOutbox(context.Context, string, string, time.Time, string) error
	FailOutbox(context.Context, string, string, time.Time, string) error
	OutboxStats(context.Context) (OutboxStats, error)
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

// ErrInvalidResult intentionally does not disclose whether an invocation exists,
// belongs to another owner/connection, has expired, or was already completed.
var ErrInvalidResult = errors.New("invalid invocation result")

type InvocationWatch interface {
	Updates() <-chan struct{}
	Close()
}

// FunctionStore atomically fences each dispatch/result and evaluates persisted
// deadlines on reads and transitions. Invocation/idempotency state lasts 24h.
type FunctionCatalog interface {
	CreateFunction(context.Context, domain.Function) error
	GetFunction(context.Context, string) (domain.Function, error)
	ListFunctions(context.Context, string) ([]domain.Function, error)
	DeleteFunction(context.Context, string, string) error
}

type FunctionInvocationStore interface {
	FindInvocation(context.Context, string, string) (domain.Invocation, error)
	CreateInvocation(context.Context, domain.Invocation, string) (domain.Invocation, bool, error)
	GetInvocation(context.Context, string) (domain.Invocation, error)
	ClaimInvocation(context.Context, string, string, string) error
	AcknowledgeInvocation(context.Context, string, string, string) error
	ReleaseInvocation(context.Context, string, string, string) error
	CompleteInvocation(context.Context, string, string, domain.RPCResult) error
	WatchInvocation(context.Context, string) (InvocationWatch, error)
}

type FunctionStore interface {
	FunctionCatalog
	FunctionInvocationStore
}
