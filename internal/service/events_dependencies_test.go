package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
)

func TestEventServiceComposesIndependentStoreCapabilities(t *testing.T) {
	memory := &eventMemory{events: map[string]domain.Event{}, jobs: map[string]domain.Job{}, idem: map[string]store.Publication{}}
	apps := newMemoryAppStore()
	service, err := NewEventServiceWithStores(eventPublisherOnly{memory}, eventReaderOnly{memory}, deliveryManagerOnly{memory}, apps, EventOptions{})
	if err != nil || service == nil {
		t.Fatalf("NewEventServiceWithStores() service=%v error=%v", service, err)
	}
}

func TestEventServiceDependenciesFailClosedIncludingTypedNil(t *testing.T) {
	memory := &eventMemory{events: map[string]domain.Event{}, jobs: map[string]domain.Job{}, idem: map[string]store.Publication{}}
	apps := newMemoryAppStore()
	var typedNil *eventMemory
	for name, build := range map[string]func() (*EventService, error){
		"publisher": func() (*EventService, error) {
			return NewEventServiceWithStores(typedNil, eventReaderOnly{memory}, deliveryManagerOnly{memory}, apps, EventOptions{})
		},
		"apps": func() (*EventService, error) {
			return NewEventServiceWithStores(eventPublisherOnly{memory}, eventReaderOnly{memory}, deliveryManagerOnly{memory}, (*memoryAppStore)(nil), EventOptions{})
		},
	} {
		t.Run(name, func(t *testing.T) {
			if service, err := build(); service != nil || !errors.Is(err, ErrInvalidDependency) {
				t.Fatalf("service=%v error=%v", service, err)
			}
		})
	}
	legacy := NewEventService(nil, nil, EventOptions{})
	if _, _, _, err := legacy.Publish(context.Background(), "source", validEvent(), "key"); !errors.Is(err, ErrInvalidDependency) {
		t.Fatalf("legacy nil Publish() error=%v", err)
	}
	partial, err := NewEventServiceWithStores(eventPublisherOnly{memory}, typedNil, typedNil, apps, EventOptions{})
	if err != nil || partial == nil {
		t.Fatalf("partial service=%v error=%v", partial, err)
	}
	if _, err := partial.GetEvent(context.Background(), "a", "event"); !errors.Is(err, ErrInvalidDependency) {
		t.Fatalf("missing reader error=%v", err)
	}
	if _, err := partial.Lease(context.Background(), "a", 1, 0); !errors.Is(err, ErrInvalidDependency) {
		t.Fatalf("missing delivery manager error=%v", err)
	}
}

type eventPublisherOnly struct{ memory *eventMemory }

func (only eventPublisherOnly) FindPublication(ctx context.Context, source, key string) (store.Publication, error) {
	return only.memory.FindPublication(ctx, source, key)
}
func (only eventPublisherOnly) PublishEvent(ctx context.Context, publication store.Publication, key string, retention store.EventRetention) (store.Publication, bool, error) {
	return only.memory.PublishEvent(ctx, publication, key, retention)
}

type eventReaderOnly struct{ memory *eventMemory }

func (only eventReaderOnly) GetEvent(ctx context.Context, id string) (domain.Event, error) {
	return only.memory.GetEvent(ctx, id)
}
func (only eventReaderOnly) GetJob(ctx context.Context, id string) (domain.Job, error) {
	return only.memory.GetJob(ctx, id)
}

type deliveryManagerOnly struct{ memory *eventMemory }

func (only deliveryManagerOnly) LeaseJobs(ctx context.Context, target string, limit int, now time.Time, lease time.Duration) ([]store.LeasedEvent, error) {
	return only.memory.LeaseJobs(ctx, target, limit, now, lease)
}
func (only deliveryManagerOnly) AckEvent(ctx context.Context, target, event string, now time.Time, retention time.Duration) error {
	return only.memory.AckEvent(ctx, target, event, now, retention)
}
func (only deliveryManagerOnly) TransitionJob(ctx context.Context, id string, status domain.JobStatus, now time.Time, retention time.Duration) (domain.Job, error) {
	return only.memory.TransitionJob(ctx, id, status, now, retention)
}
