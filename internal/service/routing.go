package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
)

type CreateRoutingRule struct {
	SourceAppID     *string `json:"source_app_id"`
	EventType       string  `json:"event_type"`
	TargetAppID     string  `json:"target_app_id"`
	RealtimeChannel *string `json:"realtime_channel"`
	Enabled         *bool   `json:"enabled"`
}

type UpdateRoutingRule struct {
	SourceAppID     OptionalString `json:"-"`
	EventType       *string        `json:"event_type"`
	TargetAppID     *string        `json:"target_app_id"`
	RealtimeChannel OptionalString `json:"-"`
	Enabled         *bool          `json:"enabled"`
}

type RoutingOptions struct {
	Now   func() time.Time
	NewID func(string) (string, error)
}

type RoutingService struct {
	store   store.RoutingRuleStore
	apps    store.ApplicationReader
	options RoutingOptions
}

func NewRoutingService(repository store.RoutingRuleStore, apps store.ApplicationReader, options RoutingOptions) *RoutingService {
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.NewID == nil {
		options.NewID = func(prefix string) (string, error) {
			var raw [16]byte
			if _, err := rand.Read(raw[:]); err != nil {
				return "", err
			}
			return prefix + hex.EncodeToString(raw[:]), nil
		}
	}
	return &RoutingService{store: repository, apps: apps, options: options}
}

func (service *RoutingService) Create(ctx context.Context, input CreateRoutingRule) (domain.RoutingRule, error) {
	if service == nil || service.store == nil || service.apps == nil {
		return domain.RoutingRule{}, ErrInvalidDependency
	}
	rule, err := service.newRule(ctx, input)
	if err != nil {
		return domain.RoutingRule{}, err
	}
	if err := service.store.CreateRoutingRule(ctx, rule); err != nil {
		return domain.RoutingRule{}, mapStoreError(err)
	}
	return rule, nil
}

func (service *RoutingService) List(ctx context.Context) ([]domain.RoutingRule, error) {
	if service == nil || service.store == nil {
		return nil, ErrInvalidDependency
	}
	rules, err := service.store.ListRoutingRules(ctx)
	return rules, mapStoreError(err)
}

func (service *RoutingService) Update(ctx context.Context, id string, input UpdateRoutingRule) (domain.RoutingRule, error) {
	if service == nil || service.store == nil || service.apps == nil {
		return domain.RoutingRule{}, ErrInvalidDependency
	}
	if strings.TrimSpace(id) == "" {
		return domain.RoutingRule{}, ErrInvalidInput
	}
	current, err := service.store.GetRoutingRule(ctx, id)
	if err != nil {
		return domain.RoutingRule{}, mapStoreError(err)
	}
	if input.SourceAppID.Set {
		current.SourceAppID = normalizeStringPointer(input.SourceAppID.Value)
	}
	if input.EventType != nil {
		current.EventType = strings.TrimSpace(*input.EventType)
	}
	if input.TargetAppID != nil {
		current.TargetAppID = strings.TrimSpace(*input.TargetAppID)
	}
	if input.RealtimeChannel.Set {
		current.RealtimeChannel = normalizeStringPointer(input.RealtimeChannel.Value)
	}
	if input.Enabled != nil {
		current.Enabled = *input.Enabled
	}
	current.UpdatedAt = service.options.Now().UTC()
	if err := service.validateRule(ctx, current); err != nil {
		return domain.RoutingRule{}, err
	}
	updated, err := service.store.UpdateRoutingRule(ctx, current)
	return updated, mapStoreError(err)
}

func (service *RoutingService) Delete(ctx context.Context, id string) error {
	if service == nil || service.store == nil {
		return ErrInvalidDependency
	}
	if strings.TrimSpace(id) == "" {
		return ErrInvalidInput
	}
	return mapStoreError(service.store.DeleteRoutingRule(ctx, id, service.options.Now().UTC()))
}

func (service *RoutingService) Resolve(ctx context.Context, sourceAppID, eventType string) ([]domain.RoutingRule, error) {
	if service == nil || service.store == nil {
		return nil, ErrInvalidDependency
	}
	rules, err := service.store.ResolveRoutingRules(ctx, sourceAppID, eventType)
	if err != nil {
		return nil, mapStoreError(err)
	}
	return rules, nil
}

func (service *RoutingService) newRule(ctx context.Context, input CreateRoutingRule) (domain.RoutingRule, error) {
	id, err := service.options.NewID("route_")
	if err != nil {
		return domain.RoutingRule{}, err
	}
	enabled := true
	if input.Enabled != nil {
		enabled = *input.Enabled
	}
	now := service.options.Now().UTC()
	rule := domain.RoutingRule{ID: id, SourceAppID: normalizeStringPointer(input.SourceAppID), EventType: strings.TrimSpace(input.EventType), TargetAppID: strings.TrimSpace(input.TargetAppID), RealtimeChannel: normalizeStringPointer(input.RealtimeChannel), Enabled: enabled, CreatedAt: now, UpdatedAt: now}
	if err := service.validateRule(ctx, rule); err != nil {
		return domain.RoutingRule{}, err
	}
	return rule, nil
}

func (service *RoutingService) validateRule(ctx context.Context, rule domain.RoutingRule) error {
	if strings.TrimSpace(rule.ID) == "" || strings.TrimSpace(rule.EventType) == "" || strings.TrimSpace(rule.TargetAppID) == "" {
		return ErrInvalidInput
	}
	if rule.SourceAppID != nil {
		source, err := service.apps.GetApplication(ctx, *rule.SourceAppID)
		if errors.Is(err, store.ErrNotFound) || (err == nil && !source.Enabled) {
			return ErrInvalidInput
		}
		if err != nil {
			return mapStoreError(err)
		}
	}
	target, err := service.apps.GetApplication(ctx, rule.TargetAppID)
	if errors.Is(err, store.ErrNotFound) || (err == nil && !target.Enabled) {
		return ErrInvalidInput
	}
	if err != nil {
		return mapStoreError(err)
	}
	if rule.RealtimeChannel != nil && !domain.ValidRealtimeChannel(*rule.RealtimeChannel) {
		return ErrInvalidInput
	}
	return nil
}

func normalizeStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}
