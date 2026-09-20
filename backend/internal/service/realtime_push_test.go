package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
)

type pushRepositoryFake struct {
	devices  map[string]domain.PushDevice
	bindings map[string]bool
	outcomes []domain.PushOutcome
}

func (fake *pushRepositoryFake) CreatePushDevice(_ context.Context, d domain.PushDevice) (domain.PushDevice, error) {
	if fake.devices == nil {
		fake.devices = map[string]domain.PushDevice{}
	}
	fake.devices[d.AppID+"/"+d.ID] = d
	return d, nil
}
func (fake *pushRepositoryFake) DeletePushDevice(_ context.Context, app, id string) error {
	if _, ok := fake.devices[app+"/"+id]; !ok {
		return store.ErrNotFound
	}
	delete(fake.devices, app+"/"+id)
	return nil
}
func (fake *pushRepositoryFake) BindPushDevice(_ context.Context, app, channel, id string, _ time.Time) error {
	if _, ok := fake.devices[app+"/"+id]; !ok {
		return store.ErrNotFound
	}
	if fake.bindings == nil {
		fake.bindings = map[string]bool{}
	}
	fake.bindings[app+"/"+channel+"/"+id] = true
	return nil
}
func (fake *pushRepositoryFake) UnbindPushDevice(_ context.Context, app, channel, id string) error {
	delete(fake.bindings, app+"/"+channel+"/"+id)
	return nil
}
func (fake *pushRepositoryFake) ListPushDevicesForChannel(_ context.Context, app, channel string, _ int) ([]domain.PushDevice, error) {
	result := []domain.PushDevice{}
	for key, device := range fake.devices {
		if fake.bindings[app+"/"+channel+"/"+device.ID] && key == app+"/"+device.ID {
			result = append(result, device)
		}
	}
	return result, nil
}
func (fake *pushRepositoryFake) CreatePushOutcome(_ context.Context, outcome domain.PushOutcome) error {
	fake.outcomes = append(fake.outcomes, outcome)
	return nil
}

type pushAdapterFake struct{}

func (pushAdapterFake) Send(context.Context, domain.PushDevice, domain.PushNotification) (string, error) {
	return "provider_1", nil
}

func TestRealtimePushIsAppScopedBoundedAndPersistsOutcomes(t *testing.T) {
	repository := &pushRepositoryFake{}
	sequence := 0
	service := NewRealtimePushService(repository, RealtimePushOptions{Now: func() time.Time { return time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC) }, NewID: func(prefix string) string { sequence++; return prefix + string(rune('0'+sequence)) }, Adapters: map[string]PushAdapter{"fcm": pushAdapterFake{}}})
	device, err := service.Register(context.Background(), "app_a", RegisterPushDeviceInput{Provider: "fcm", Token: "long-provider-device-token"})
	if err != nil || len(device.Token) != 0 {
		t.Fatalf("device=%#v error=%v", device, err)
	}
	if err := service.Bind(context.Background(), "app_a", "room", device.ID, true); err != nil {
		t.Fatal(err)
	}
	outcomes, err := service.Publish(context.Background(), "app_a", "room", domain.PushNotification{Title: "Update", Body: "Ready", Data: json.RawMessage(`{"screen":"orders"}`)})
	if err != nil || len(outcomes) != 1 || outcomes[0].Status != "delivered" || len(repository.outcomes) != 1 {
		t.Fatalf("outcomes=%#v error=%v", outcomes, err)
	}
	if _, err := service.Publish(context.Background(), "app_a", "room", domain.PushNotification{Data: json.RawMessage(`{"access_token":"secret"}`)}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("secret template=%v", err)
	}
}

func TestRealtimePushRejectsNestedSecretFields(t *testing.T) {
	repository := &pushRepositoryFake{}
	service := NewRealtimePushService(repository, RealtimePushOptions{})
	_, err := service.Publish(context.Background(), "app_1", "private:room", domain.PushNotification{Data: json.RawMessage(`{"nested":{"access_token":"must-not-leak"}}`)})
	if !errors.Is(err, ErrInvalidInput) || len(repository.outcomes) != 0 {
		t.Fatalf("error=%v outcomes=%d", err, len(repository.outcomes))
	}
}
