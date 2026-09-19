package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/adminread"
	"github.com/dungxbuif/RelayHub/internal/store"
)

type adminLifecycleRepositoryStub struct {
	command  adminread.ReplayCommand
	timeline adminread.EventTimeline
	err      error
	replayed bool
}

func (stub *adminLifecycleRepositoryStub) GetAdminEventTimeline(context.Context, string) (adminread.EventTimeline, error) {
	return stub.timeline, stub.err
}

func (stub *adminLifecycleRepositoryStub) ReplayAdminDeadLetters(_ context.Context, command adminread.ReplayCommand) (adminread.ReplayResult, bool, error) {
	stub.command = command
	return adminread.ReplayResult{Items: []adminread.ReplayItem{{DeliveryID: command.DeliveryIDs[0], Generation: 2}}}, stub.replayed, stub.err
}

func TestAdminLifecycleReplayCanonicalizesAndHashesSecrets(t *testing.T) {
	now := time.Date(2026, 9, 20, 8, 0, 0, 0, time.UTC)
	repository := &adminLifecycleRepositoryStub{}
	service, err := NewAdminLifecycleService(repository, AdminLifecycleOptions{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	result, replayed, err := service.Replay(context.Background(), []string{"dlv_b", "dlv_a"}, "retry-key", "admin_bearer")
	if err != nil || replayed || len(result.Items) != 1 {
		t.Fatalf("Replay() result=%#v replayed=%v error=%v", result, replayed, err)
	}
	command := repository.command
	if strings.Join(command.DeliveryIDs, ",") != "dlv_a,dlv_b" || command.Now != now || command.ActorID != "admin_bearer" {
		t.Fatalf("command = %#v", command)
	}
	if len(command.IdempotencyKeyHash) != 64 || strings.Contains(command.IdempotencyKeyHash, "retry-key") || len(command.RequestFingerprint) != 64 {
		t.Fatalf("unsafe hashes in command = %#v", command)
	}
	firstFingerprint := command.RequestFingerprint
	if _, _, err := service.Replay(context.Background(), []string{"dlv_a", "dlv_b"}, "retry-key-2", "admin_bearer"); err != nil || repository.command.RequestFingerprint != firstFingerprint {
		t.Fatalf("selection fingerprint is not canonical: %#v error=%v", repository.command, err)
	}
}

func TestAdminLifecycleRejectsInvalidReplayAndMapsStoreErrors(t *testing.T) {
	repository := &adminLifecycleRepositoryStub{}
	service, err := NewAdminLifecycleService(repository, AdminLifecycleOptions{})
	if err != nil {
		t.Fatal(err)
	}
	invalid := []struct {
		ids, key, actor []string
	}{
		{nil, []string{"key"}, []string{"actor"}},
		{[]string{"same", "same"}, []string{"key"}, []string{"actor"}},
		{[]string{"dlv"}, nil, []string{"actor"}},
		{[]string{"dlv"}, []string{" key"}, []string{"actor"}},
		{[]string{"dlv"}, []string{strings.Repeat("k", 257)}, []string{"actor"}},
		{[]string{"dlv"}, []string{"key"}, nil},
	}
	for index, candidate := range invalid {
		var key, actor string
		if len(candidate.key) > 0 {
			key = candidate.key[0]
		}
		if len(candidate.actor) > 0 {
			actor = candidate.actor[0]
		}
		if _, _, err := service.Replay(context.Background(), candidate.ids, key, actor); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("invalid case %d error=%v", index, err)
		}
	}
	repository.err = store.ErrConflict
	if _, _, err := service.Replay(context.Background(), []string{"dlv"}, "key", "actor"); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict error=%v", err)
	}
	repository.err = store.ErrNotFound
	if _, err := service.Timeline(context.Background(), "evt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("not found error=%v", err)
	}
}

func TestAdminLifecycleRejectsInvalidDependencies(t *testing.T) {
	if _, err := NewAdminLifecycleService(nil, AdminLifecycleOptions{}); !errors.Is(err, ErrInvalidDependency) {
		t.Fatalf("nil repository error=%v", err)
	}
	if _, err := NewAdminLifecycleService(&adminLifecycleRepositoryStub{}, AdminLifecycleOptions{Timeout: time.Minute}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("invalid timeout error=%v", err)
	}
}
