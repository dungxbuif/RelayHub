package service

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/dungxbuif/RelayHub/internal/domain"
)

type lifecycleApps struct{ app domain.App }

func (apps lifecycleApps) GetApplication(context.Context, string) (domain.App, error) {
	return apps.app, nil
}

type lifecycleEvents struct {
	input PublishEvent
	key   string
}

func (events *lifecycleEvents) Publish(_ context.Context, _ string, input PublishEvent, key string) (domain.Event, []domain.Job, bool, error) {
	events.input = input
	events.key = key
	return domain.Event{}, nil, false, nil
}

func TestRealtimeLifecycleUsesDurableCallbackEventWithSafeMetadata(t *testing.T) {
	callback := "https://app.example/callback"
	events := &lifecycleEvents{}
	emitter := NewRealtimeLifecycleEmitter(lifecycleApps{app: domain.App{ID: "app_a", Enabled: true, CallbackURL: &callback, DeliveryMode: domain.DeliveryCallback}}, events)
	err := emitter.Emit(context.Background(), "app_a", "client.publish", "msg_1", map[string]any{"channel": "room", "message_id": "msg_1", "client_id": "client_1", "secret": "must-not-pass", "payload": json.RawMessage(`{"private":true}`)})
	if err != nil || events.input.Type != "relayhub.realtime.client.publish" || events.key != "realtime:msg_1:client.publish" {
		t.Fatalf("input=%#v key=%q error=%v", events.input, events.key, err)
	}
	if len(events.input.Data) == 0 || !json.Valid(events.input.Data) {
		t.Fatal("missing safe metadata")
	}
	if bytes.Contains(events.input.Data, []byte("secret")) || bytes.Contains(events.input.Data, []byte("private")) {
		t.Fatalf("unsafe lifecycle payload=%s", events.input.Data)
	}
}
