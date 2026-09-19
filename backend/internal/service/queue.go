package service

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/observability"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/google/uuid"
)

var queueName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.-]{0,63}$`)

type QueueSubscriptionInput struct {
	Name                     string                   `json:"name"`
	Enabled                  *bool                    `json:"enabled,omitempty"`
	EventTypes               []string                 `json:"event_types,omitempty"`
	MaxAttempts              int                      `json:"max_attempts,omitempty"`
	DefaultVisibilitySeconds int                      `json:"default_visibility_seconds,omitempty"`
	MaxVisibilitySeconds     int                      `json:"max_visibility_seconds,omitempty"`
	MaxTotalLeaseSeconds     int                      `json:"max_total_lease_seconds,omitempty"`
	RetentionSeconds         int                      `json:"retention_seconds,omitempty"`
	MaxInFlight              int                      `json:"max_in_flight,omitempty"`
	MaxBatchSize             int                      `json:"max_batch_size,omitempty"`
	RetryDelaySeconds        *int                     `json:"retry_delay_seconds,omitempty"`
	OrderingMode             domain.QueueOrderingMode `json:"ordering_mode,omitempty"`
	DeduplicationSeconds     int                      `json:"deduplication_seconds,omitempty"`
	MaxDispatchRate          *int                     `json:"max_dispatch_rate,omitempty"`
}

type QueuePullInput struct {
	MaxMessages       int `json:"max_messages"`
	WaitSeconds       int `json:"wait_seconds,omitempty"`
	VisibilitySeconds int `json:"visibility_seconds,omitempty"`
}

type QueueSettleItem struct {
	Receipt      string                           `json:"receipt"`
	Disposition  store.QueueSettlementDisposition `json:"disposition"`
	DelaySeconds int                              `json:"delay_seconds,omitempty"`
	Reason       string                           `json:"reason,omitempty"`
}

type QueueExtendItem struct {
	Receipt          string `json:"receipt"`
	ExtensionSeconds int    `json:"extension_seconds"`
}

type QueueOptions struct {
	Now    func() time.Time
	Random io.Reader
	NewID  func() string
}

type QueueService struct {
	repository store.QueueRepository
	options    QueueOptions
}

func NewQueueService(repository store.QueueRepository, options QueueOptions) *QueueService {
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Random == nil {
		options.Random = rand.Reader
	}
	if options.NewID == nil {
		options.NewID = func() string { return "sub_" + uuid.NewString() }
	}
	return &QueueService{repository: repository, options: options}
}

func (service *QueueService) Create(ctx context.Context, appID string, input QueueSubscriptionInput) (domain.QueueSubscription, error) {
	if service == nil || nilDependency(service.repository) || appID == "" {
		return domain.QueueSubscription{}, ErrInvalidDependency
	}
	now := service.options.Now().UTC()
	item := subscriptionFromInput(input)
	item.ID, item.AppID, item.PolicyVersion, item.CreatedAt, item.UpdatedAt = service.options.NewID(), appID, 1, now, now
	if !validQueueSubscription(item) {
		return domain.QueueSubscription{}, ErrInvalidInput
	}
	if err := service.repository.CreateQueueSubscription(ctx, item); err != nil {
		return domain.QueueSubscription{}, mapStoreError(err)
	}
	observability.QueueOutcome("subscription_created", 1)
	return item, nil
}

func (service *QueueService) List(ctx context.Context, appID string) ([]domain.QueueSubscription, error) {
	if service == nil || nilDependency(service.repository) || appID == "" {
		return nil, ErrInvalidDependency
	}
	items, err := service.repository.ListQueueSubscriptions(ctx, appID)
	return items, mapStoreError(err)
}

func (service *QueueService) Get(ctx context.Context, appID, id string) (domain.QueueSubscription, error) {
	if service == nil || nilDependency(service.repository) {
		return domain.QueueSubscription{}, ErrInvalidDependency
	}
	if appID == "" || id == "" {
		return domain.QueueSubscription{}, ErrInvalidInput
	}
	item, err := service.repository.GetQueueSubscription(ctx, appID, id)
	return item, mapStoreError(err)
}

func (service *QueueService) Update(ctx context.Context, appID, id string, expectedVersion int64, input QueueSubscriptionInput) (domain.QueueSubscription, error) {
	if service == nil || nilDependency(service.repository) {
		return domain.QueueSubscription{}, ErrInvalidDependency
	}
	if appID == "" || id == "" || expectedVersion < 1 {
		return domain.QueueSubscription{}, ErrInvalidInput
	}
	current, err := service.repository.GetQueueSubscription(ctx, appID, id)
	if err != nil {
		return domain.QueueSubscription{}, mapStoreError(err)
	}
	updated := subscriptionFromInput(input)
	updated.ID, updated.AppID, updated.CreatedAt, updated.UpdatedAt = current.ID, current.AppID, current.CreatedAt, service.options.Now().UTC()
	updated.PausedAt = current.PausedAt
	if !updated.Enabled {
		updated.PausedAt = nil
	}
	if !validQueueSubscription(updated) {
		return domain.QueueSubscription{}, ErrInvalidInput
	}
	updated, err = service.repository.UpdateQueueSubscription(ctx, updated, expectedVersion)
	return updated, mapStoreError(err)
}

func (service *QueueService) Pause(ctx context.Context, appID, id string, paused bool) (domain.QueueSubscription, error) {
	current, err := service.Get(ctx, appID, id)
	if err != nil {
		return domain.QueueSubscription{}, err
	}
	if !current.Enabled && paused {
		return domain.QueueSubscription{}, ErrConflict
	}
	current.UpdatedAt = service.options.Now().UTC()
	if paused {
		value := current.UpdatedAt
		current.PausedAt = &value
	} else {
		current.PausedAt = nil
	}
	updated, err := service.repository.UpdateQueueSubscription(ctx, current, current.PolicyVersion)
	return updated, mapStoreError(err)
}

func (service *QueueService) Delete(ctx context.Context, appID, id string) error {
	if service == nil || nilDependency(service.repository) {
		return ErrInvalidDependency
	}
	if appID == "" || id == "" {
		return ErrInvalidInput
	}
	return mapStoreError(service.repository.DeleteQueueSubscription(ctx, appID, id))
}

func (service *QueueService) Pull(ctx context.Context, appID, subscriptionID string, input QueuePullInput) ([]domain.QueueDelivery, error) {
	if service == nil || nilDependency(service.repository) {
		return nil, ErrInvalidDependency
	}
	if appID == "" || subscriptionID == "" || input.MaxMessages < 1 || input.MaxMessages > 100 || input.WaitSeconds < 0 || input.WaitSeconds > 30 || input.VisibilitySeconds < 0 || input.VisibilitySeconds > 3600 {
		return nil, ErrInvalidInput
	}
	pollContext := ctx
	cancel := func() {}
	if input.WaitSeconds > 0 {
		pollContext, cancel = context.WithTimeout(ctx, time.Duration(input.WaitSeconds)*time.Second)
	}
	defer cancel()
	for {
		receipts, err := service.receipts(input.MaxMessages)
		if err != nil {
			return nil, err
		}
		items, err := service.repository.PullQueueDeliveries(pollContext, store.QueuePullRequest{AppID: appID, SubscriptionID: subscriptionID, Limit: input.MaxMessages, Visibility: time.Duration(input.VisibilitySeconds) * time.Second, Now: service.options.Now().UTC(), Receipts: receipts})
		if err != nil {
			if input.WaitSeconds > 0 && pollContext.Err() != nil && ctx.Err() == nil {
				return []domain.QueueDelivery{}, nil
			}
			return nil, mapStoreError(err)
		}
		if len(items) > 0 || input.WaitSeconds == 0 {
			if len(items) == 0 {
				observability.QueueOutcome("pull_empty", 1)
			} else {
				observability.QueueOutcome("leased", len(items))
			}
			return items, nil
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-pollContext.Done():
			timer.Stop()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			observability.QueueOutcome("pull_empty", 1)
			return []domain.QueueDelivery{}, nil
		case <-timer.C:
		}
	}
}

func (service *QueueService) Settle(ctx context.Context, appID, subscriptionID string, items []QueueSettleItem) ([]store.QueueSettlementResult, error) {
	if service == nil || nilDependency(service.repository) {
		return nil, ErrInvalidDependency
	}
	if appID == "" || subscriptionID == "" || len(items) < 1 || len(items) > 100 {
		return nil, ErrInvalidInput
	}
	settlements := make([]store.QueueSettlement, 0, len(items))
	seen := map[string]struct{}{}
	for _, item := range items {
		if !validReceipt(item.Receipt) || item.DelaySeconds < 0 || item.DelaySeconds > 86400 || len(item.Reason) > 1024 || (item.Disposition != store.QueueAcknowledge && item.Disposition != store.QueueRetry && item.Disposition != store.QueueDeadLetter) {
			return nil, ErrInvalidInput
		}
		if _, duplicate := seen[item.Receipt]; duplicate {
			return nil, ErrInvalidInput
		}
		seen[item.Receipt] = struct{}{}
		settlements = append(settlements, store.QueueSettlement{Receipt: item.Receipt, Disposition: item.Disposition, Delay: time.Duration(item.DelaySeconds) * time.Second, Reason: strings.TrimSpace(item.Reason)})
	}
	results, err := service.repository.SettleQueueDeliveries(ctx, appID, subscriptionID, settlements, service.options.Now().UTC())
	if err == nil {
		for _, result := range results {
			outcome := map[string]string{"acked": "acked", "available": "retried", "dead_letter": "dead_lettered", "invalid_receipt": "invalid_receipt"}[result.Status]
			observability.QueueOutcome(outcome, 1)
		}
	}
	return results, mapStoreError(err)
}

func (service *QueueService) Extend(ctx context.Context, appID, subscriptionID string, items []QueueExtendItem) ([]store.QueueSettlementResult, error) {
	if service == nil || nilDependency(service.repository) {
		return nil, ErrInvalidDependency
	}
	if appID == "" || subscriptionID == "" || len(items) < 1 || len(items) > 100 {
		return nil, ErrInvalidInput
	}
	extensions := make([]store.QueueLeaseExtension, 0, len(items))
	seen := map[string]struct{}{}
	for _, item := range items {
		if !validReceipt(item.Receipt) || item.ExtensionSeconds < 1 || item.ExtensionSeconds > 3600 {
			return nil, ErrInvalidInput
		}
		if _, duplicate := seen[item.Receipt]; duplicate {
			return nil, ErrInvalidInput
		}
		seen[item.Receipt] = struct{}{}
		extensions = append(extensions, store.QueueLeaseExtension{Receipt: item.Receipt, Extension: time.Duration(item.ExtensionSeconds) * time.Second})
	}
	results, err := service.repository.ExtendQueueLeases(ctx, appID, subscriptionID, extensions, service.options.Now().UTC())
	if err == nil {
		for _, result := range results {
			if result.Status == "extended" {
				observability.QueueOutcome("lease_extended", 1)
			} else {
				observability.QueueOutcome("invalid_receipt", 1)
			}
		}
	}
	return results, mapStoreError(err)
}

func (service *QueueService) Depth(ctx context.Context, appID, subscriptionID string) (domain.QueueDepth, error) {
	if service == nil || nilDependency(service.repository) {
		return domain.QueueDepth{}, ErrInvalidDependency
	}
	if appID == "" || subscriptionID == "" {
		return domain.QueueDepth{}, ErrInvalidInput
	}
	depth, err := service.repository.QueueDepth(ctx, appID, subscriptionID, service.options.Now().UTC())
	return depth, mapStoreError(err)
}

func (service *QueueService) DeadLetters(ctx context.Context, appID, subscriptionID, cursor string, limit int) ([]domain.QueueDeadLetter, error) {
	if service == nil || nilDependency(service.repository) {
		return nil, ErrInvalidDependency
	}
	if appID == "" || subscriptionID == "" || limit < 1 || limit > 100 || len(cursor) > 128 {
		return nil, ErrInvalidInput
	}
	items, err := service.repository.ListQueueDeadLetters(ctx, appID, subscriptionID, store.QueueDeadLetterQuery{Limit: limit, Cursor: cursor})
	return items, mapStoreError(err)
}

func (service *QueueService) ReplayDeadLetters(ctx context.Context, appID, subscriptionID string, ids []string) (int, error) {
	if service == nil || nilDependency(service.repository) {
		return 0, ErrInvalidDependency
	}
	if !validDeliveryIDs(appID, subscriptionID, ids) {
		return 0, ErrInvalidInput
	}
	count, err := service.repository.ReplayQueueDeadLetters(ctx, appID, subscriptionID, ids, service.options.Now().UTC())
	if err == nil {
		observability.QueueOutcome("replayed", count)
	}
	return count, mapStoreError(err)
}

func (service *QueueService) DeleteDeadLetters(ctx context.Context, appID, subscriptionID string, ids []string) (int, error) {
	if service == nil || nilDependency(service.repository) {
		return 0, ErrInvalidDependency
	}
	if !validDeliveryIDs(appID, subscriptionID, ids) {
		return 0, ErrInvalidInput
	}
	count, err := service.repository.DeleteQueueDeadLetters(ctx, appID, subscriptionID, ids)
	if err == nil {
		observability.QueueOutcome("deleted", count)
	}
	return count, mapStoreError(err)
}

func subscriptionFromInput(input QueueSubscriptionInput) domain.QueueSubscription {
	enabled := true
	if input.Enabled != nil {
		enabled = *input.Enabled
	}
	retryDelay := 5
	if input.RetryDelaySeconds != nil {
		retryDelay = *input.RetryDelaySeconds
	}
	item := domain.QueueSubscription{Name: strings.TrimSpace(input.Name), Enabled: enabled, EventTypes: append([]string(nil), input.EventTypes...), MaxAttempts: input.MaxAttempts, DefaultVisibilitySeconds: input.DefaultVisibilitySeconds, MaxVisibilitySeconds: input.MaxVisibilitySeconds, MaxTotalLeaseSeconds: input.MaxTotalLeaseSeconds, RetentionSeconds: input.RetentionSeconds, MaxInFlight: input.MaxInFlight, MaxBatchSize: input.MaxBatchSize, RetryDelaySeconds: retryDelay, OrderingMode: input.OrderingMode, DeduplicationSeconds: input.DeduplicationSeconds, MaxDispatchRate: input.MaxDispatchRate}
	if item.MaxAttempts == 0 {
		item.MaxAttempts = 10
	}
	if item.DefaultVisibilitySeconds == 0 {
		item.DefaultVisibilitySeconds = 60
	}
	if item.MaxVisibilitySeconds == 0 {
		item.MaxVisibilitySeconds = 300
	}
	if item.MaxTotalLeaseSeconds == 0 {
		item.MaxTotalLeaseSeconds = 3600
	}
	if item.RetentionSeconds == 0 {
		item.RetentionSeconds = 604800
	}
	if item.MaxInFlight == 0 {
		item.MaxInFlight = 100
	}
	if item.MaxBatchSize == 0 {
		item.MaxBatchSize = 20
	}
	if item.OrderingMode == "" {
		item.OrderingMode = domain.QueueOrderingNone
	}
	for index := range item.EventTypes {
		item.EventTypes[index] = strings.TrimSpace(item.EventTypes[index])
	}
	sort.Strings(item.EventTypes)
	return item
}

func validQueueSubscription(item domain.QueueSubscription) bool {
	if !queueName.MatchString(item.Name) || item.MaxAttempts < 1 || item.MaxAttempts > 100 || item.DefaultVisibilitySeconds < 1 || item.DefaultVisibilitySeconds > 900 || item.MaxVisibilitySeconds < item.DefaultVisibilitySeconds || item.MaxVisibilitySeconds > 3600 || item.MaxTotalLeaseSeconds < item.MaxVisibilitySeconds || item.MaxTotalLeaseSeconds > 86400 || item.RetentionSeconds < 60 || item.RetentionSeconds > 2592000 || item.MaxInFlight < 1 || item.MaxInFlight > 10000 || item.MaxBatchSize < 1 || item.MaxBatchSize > 100 || item.RetryDelaySeconds < 0 || item.RetryDelaySeconds > 86400 || (item.OrderingMode != domain.QueueOrderingNone && item.OrderingMode != domain.QueueOrderingKey) || item.DeduplicationSeconds < 0 || item.DeduplicationSeconds > 86400 || len(item.EventTypes) > 100 {
		return false
	}
	if item.MaxDispatchRate != nil && (*item.MaxDispatchRate < 1 || *item.MaxDispatchRate > 100000) {
		return false
	}
	for index, eventType := range item.EventTypes {
		if eventType == "" || len(eventType) > 128 || (index > 0 && eventType == item.EventTypes[index-1]) {
			return false
		}
	}
	return true
}

func (service *QueueService) receipts(count int) ([]string, error) {
	result := make([]string, count)
	for index := range result {
		raw := make([]byte, 32)
		if _, err := io.ReadFull(service.options.Random, raw); err != nil {
			return nil, err
		}
		result[index] = base64.RawURLEncoding.EncodeToString(raw)
	}
	return result, nil
}

func validReceipt(receipt string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(receipt)
	return err == nil && len(decoded) == 32
}

func validDeliveryIDs(appID, subscriptionID string, ids []string) bool {
	if appID == "" || subscriptionID == "" || len(ids) < 1 || len(ids) > 100 {
		return false
	}
	seen := map[string]struct{}{}
	for _, id := range ids {
		if !strings.HasPrefix(id, "qdl_") || len(id) > 128 {
			return false
		}
		if _, duplicate := seen[id]; duplicate {
			return false
		}
		seen[id] = struct{}{}
	}
	return true
}
