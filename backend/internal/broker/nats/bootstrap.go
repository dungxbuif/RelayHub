package natsbroker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

var ErrUnsafeStreamConfig = errors.New("unsafe existing JetStream configuration")

type StreamSettings struct {
	MaxAge          time.Duration
	DuplicateWindow time.Duration
	Replicas        int
}

func DefaultStreamSettings() StreamSettings {
	return StreamSettings{MaxAge: 7 * 24 * time.Hour, DuplicateWindow: 24 * time.Hour, Replicas: 1}
}

func ExpectedStreams(settings StreamSettings) []jetstream.StreamConfig {
	base := func(name, subject string, retention jetstream.RetentionPolicy) jetstream.StreamConfig {
		return jetstream.StreamConfig{
			Name: name, Subjects: []string{subject}, Retention: retention,
			MaxConsumers: -1, MaxMsgs: -1, MaxBytes: -1, MaxMsgsPerSubject: -1,
			Discard: jetstream.DiscardOld, MaxAge: settings.MaxAge, MaxMsgSize: 1 << 20,
			Storage: jetstream.FileStorage, Replicas: settings.Replicas,
			Duplicates: settings.DuplicateWindow, NoAck: false, AllowDirect: false,
		}
	}
	return []jetstream.StreamConfig{
		base("RH_DELIVERIES", "rh.v1.delivery.*", jetstream.WorkQueuePolicy),
		base("RH_CALLBACKS", "rh.v1.callback.*", jetstream.WorkQueuePolicy),
		base("RH_DLQ", "rh.v1.dlq.*", jetstream.LimitsPolicy),
	}
}

func (client *Client) Bootstrap(ctx context.Context, settings StreamSettings) error {
	if settings.MaxAge <= 0 || settings.DuplicateWindow <= 0 || (settings.Replicas != 1 && settings.Replicas != 3 && settings.Replicas != 5) {
		return errors.New("invalid JetStream settings")
	}
	if _, err := client.jetstream.AccountInfo(ctx); err != nil {
		return fmt.Errorf("%w: JetStream unavailable", ErrUnavailable)
	}
	for _, expected := range ExpectedStreams(settings) {
		stream, err := client.jetstream.Stream(ctx, expected.Name)
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			if _, createErr := client.jetstream.CreateStream(ctx, expected); createErr != nil {
				return fmt.Errorf("bootstrap stream %s: %w", expected.Name, createErr)
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("inspect stream %s: %w", expected.Name, err)
		}
		info, err := stream.Info(ctx)
		if err != nil {
			return fmt.Errorf("inspect stream %s: %w", expected.Name, err)
		}
		if !managedStreamConfigEqual(info.Config, expected) {
			return fmt.Errorf("%w: stream %s differs from RelayHub settings", ErrUnsafeStreamConfig, expected.Name)
		}
	}
	client.managedMu.Lock()
	client.managed = &settings
	client.managedMu.Unlock()
	return nil
}

func (client *Client) validateManagedStreams(ctx context.Context, settings StreamSettings) error {
	for _, expected := range ExpectedStreams(settings) {
		stream, err := client.jetstream.Stream(ctx, expected.Name)
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return fmt.Errorf("%w: stream %s is missing", ErrUnsafeStreamConfig, expected.Name)
		}
		if err != nil {
			return fmt.Errorf("inspect stream %s: %w", expected.Name, err)
		}
		info, err := stream.Info(ctx)
		if err != nil {
			return fmt.Errorf("inspect stream %s: %w", expected.Name, err)
		}
		if !managedStreamConfigEqual(info.Config, expected) {
			return fmt.Errorf("%w: stream %s differs from RelayHub settings", ErrUnsafeStreamConfig, expected.Name)
		}
	}
	return nil
}

func managedStreamConfigEqual(actual, expected jetstream.StreamConfig) bool {
	return actual.Name == expected.Name &&
		slices.Equal(actual.Subjects, expected.Subjects) &&
		actual.Retention == expected.Retention &&
		actual.MaxConsumers == expected.MaxConsumers &&
		actual.MaxMsgs == expected.MaxMsgs &&
		actual.MaxBytes == expected.MaxBytes &&
		actual.Discard == expected.Discard &&
		actual.MaxAge == expected.MaxAge &&
		actual.MaxMsgsPerSubject == expected.MaxMsgsPerSubject &&
		actual.MaxMsgSize == expected.MaxMsgSize &&
		actual.Storage == expected.Storage &&
		actual.Replicas == expected.Replicas &&
		actual.NoAck == expected.NoAck &&
		actual.Duplicates == expected.Duplicates &&
		actual.AllowDirect == expected.AllowDirect
}
