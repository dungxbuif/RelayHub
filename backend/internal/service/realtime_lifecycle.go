package service

import (
	"context"
	"encoding/json"
	"regexp"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
)

var lifecycleType = regexp.MustCompile(`^(presence\.(join|update|leave|timeout)|client\.publish|delivery\.failure)$`)

type LifecycleEventPublisher interface {
	Publish(context.Context, string, PublishEvent, string) (domain.Event, []domain.Job, bool, error)
}
type RealtimeLifecycleEmitter struct {
	apps   store.ApplicationReader
	events LifecycleEventPublisher
}

func NewRealtimeLifecycleEmitter(apps store.ApplicationReader, events LifecycleEventPublisher) *RealtimeLifecycleEmitter {
	return &RealtimeLifecycleEmitter{apps: apps, events: events}
}

func (emitter *RealtimeLifecycleEmitter) Emit(ctx context.Context, appID, eventType, idempotencyKey string, metadata map[string]any) error {
	if emitter == nil || nilDependency(emitter.apps) || nilDependency(emitter.events) {
		return ErrInvalidDependency
	}
	if appID == "" || !lifecycleType.MatchString(eventType) || idempotencyKey == "" {
		return ErrInvalidInput
	}
	app, err := emitter.apps.GetApplication(ctx, appID)
	if err != nil {
		return mapStoreError(err)
	}
	if !app.Enabled || app.CallbackURL == nil || (app.DeliveryMode != domain.DeliveryCallback && app.DeliveryMode != domain.DeliveryAll) {
		return nil
	}
	safe := map[string]any{"channel": metadata["channel"], "message_id": metadata["message_id"], "client_id": metadata["client_id"], "connection_id": metadata["connection_id"], "occupancy": metadata["occupancy"], "occurred_at": time.Now().UTC().Format(time.RFC3339Nano)}
	data, err := json.Marshal(safe)
	if err != nil || len(data) > 4096 {
		return ErrInvalidInput
	}
	_, _, _, err = emitter.events.Publish(ctx, appID, PublishEvent{Type: "relayhub.realtime." + eventType, TargetAppIDs: []string{appID}, Data: data}, "realtime:"+idempotencyKey+":"+eventType)
	return err
}
