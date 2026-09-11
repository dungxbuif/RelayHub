//go:build integration

package redisstore

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/google/uuid"
)

func TestIntegrationFixturesIsolateAndCleanOnlyTheirNamespace(t *testing.T) {
	one, two := integrationRedisClient(t), integrationRedisClient(t)
	ctx := context.Background()
	if one.prefix == two.prefix {
		t.Error("independent tests share a Redis namespace")
	}
	foreign := "unrelated-" + uuid.NewString()
	if err := one.client.Set(ctx, foreign, "preserve", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = one.client.Del(ctx, foreign).Err() })
	if err := one.client.Set(ctx, one.key("owned"), "remove", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	flushIntegrationRedis(t, one)
	if value, err := one.client.Get(ctx, foreign).Result(); err != nil || value != "preserve" {
		t.Fatalf("cleanup deleted unrelated state: %v", err)
	}
	if count, err := one.client.Exists(ctx, one.key("owned")).Result(); err != nil || count != 0 {
		t.Fatalf("owned state remains: %d %v", count, err)
	}
}

func TestLoadCallbackPreservesRedisFailure(t *testing.T) {
	c := integrationRedisClient(t)
	ctx := context.Background()
	claim := store.CallbackClaim{JobID: "failure-probe", Token: "owned", ExpiresAt: time.Now().Add(time.Minute)}
	for _, scenario := range []string{"wrong type", "missing", "mismatch", "expired"} {
		t.Run(scenario, func(t *testing.T) {
			if err := c.client.Del(ctx, c.callbackLock(claim.JobID)).Err(); err != nil {
				t.Fatal(err)
			}
			probe := claim
			switch scenario {
			case "wrong type":
				if err := c.client.LPush(ctx, c.callbackLock(claim.JobID), "not-a-string").Err(); err != nil {
					t.Fatal(err)
				}
			case "mismatch":
				if err := c.client.Set(ctx, c.callbackLock(claim.JobID), "other", time.Minute).Err(); err != nil {
					t.Fatal(err)
				}
			case "expired":
				probe.ExpiresAt = time.Now().Add(-time.Second)
			}
			_, err := c.LoadCallback(ctx, probe)
			if scenario == "wrong type" {
				if err == nil || errors.Is(err, store.ErrConflict) || !strings.Contains(err.Error(), "WRONGTYPE") {
					t.Fatalf("Redis error was hidden: %v", err)
				}
			} else if !errors.Is(err, store.ErrConflict) {
				t.Fatalf("expected ordinary conflict, got %v", err)
			}
		})
	}
}

type concurrentPatchStore struct {
	*Client
	reads      atomic.Int32
	blockReads int32
	read       chan struct{}
	resume     chan struct{}
}

func (s *concurrentPatchStore) GetApplication(ctx context.Context, id string) (domain.App, error) {
	app, err := s.Client.GetApplication(ctx, id)
	if s.reads.Add(1) <= s.blockReads {
		s.read <- struct{}{}
		select {
		case <-s.resume:
		case <-ctx.Done():
			return domain.App{}, ctx.Err()
		}
	}
	return app, err
}

func TestApplicationConcurrentPartialUpdatesPreserveChanges(t *testing.T) {
	c := integrationRedisClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	callback := "https://old.example/events"
	app := domain.App{ID: "patch-app", Name: "old", Enabled: true, CallbackURL: &callback, DeliveryMode: domain.DeliveryAll, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := c.CreateApplication(ctx, app, store.AppCredential{AppID: app.ID, APIKeyHash: "old"}); err != nil {
		t.Fatal(err)
	}
	barrier := &concurrentPatchStore{Client: c, blockReads: 2, read: make(chan struct{}, 2), resume: make(chan struct{})}
	svc := service.NewAppService(barrier, service.AppOptions{})
	name, queue := "new", domain.DeliveryQueue
	done := make(chan error, 2)
	for _, patch := range []service.UpdateApp{{Name: &name}, {CallbackURL: service.OptionalString{Set: true}, DeliveryMode: &queue}} {
		go func() { _, err := svc.Update(ctx, app.ID, patch); done <- err }()
	}
	for range 2 {
		select {
		case <-barrier.read:
		case <-ctx.Done():
			t.Fatal("patches did not read the same snapshot")
		}
	}
	// Independent administrative fields must survive both stale snapshots too.
	later := time.Now().Add(time.Hour)
	if _, err := c.DisableApplication(ctx, app.ID, later); err != nil {
		t.Fatal(err)
	}
	if err := c.RotateApplicationCredential(ctx, app.ID, store.AppCredential{AppID: app.ID, APIKeyHash: "rotated", HMACSecret: []byte("new-secret")}, later); err != nil {
		t.Fatal(err)
	}
	close(barrier.resume)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	actual, err := c.GetApplication(ctx, app.ID)
	if err != nil || actual.Name != name || actual.CallbackURL != nil || actual.DeliveryMode != queue || actual.Enabled || !actual.UpdatedAt.Equal(later) {
		t.Fatalf("concurrent patch lost fields: %#v %v", actual, err)
	}
	if _, err := c.FindCredentialByAPIKeyHash(ctx, "old"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("old credential restored: %v", err)
	}
	if _, err := c.FindCredentialByAPIKeyHash(ctx, "rotated"); err != nil {
		t.Fatal(err)
	}
}

func TestApplicationPatchRevalidatesAfterConcurrentCallbackRemoval(t *testing.T) {
	c := integrationRedisClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	callback := "https://old.example/events"
	app := domain.App{ID: "validation-app", Name: "app", Enabled: true, CallbackURL: &callback, DeliveryMode: domain.DeliveryQueue, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := c.CreateApplication(ctx, app, store.AppCredential{AppID: app.ID, APIKeyHash: "validation-key"}); err != nil {
		t.Fatal(err)
	}
	barrier := &concurrentPatchStore{Client: c, blockReads: 1, read: make(chan struct{}, 1), resume: make(chan struct{})}
	svc := service.NewAppService(barrier, service.AppOptions{})
	mode := domain.DeliveryCallback
	done := make(chan error, 1)
	go func() { _, err := svc.Update(ctx, app.ID, service.UpdateApp{DeliveryMode: &mode}); done <- err }()
	select {
	case <-barrier.read:
	case <-ctx.Done():
		t.Fatal("stale read was not observed")
	}
	if _, err := service.NewAppService(c, service.AppOptions{}).Update(ctx, app.ID, service.UpdateApp{CallbackURL: service.OptionalString{Set: true}}); err != nil {
		t.Fatal(err)
	}
	close(barrier.resume)
	if err := <-done; !errors.Is(err, service.ErrInvalidInput) {
		t.Fatalf("merged callback mode was not revalidated: %v", err)
	}
	actual, err := c.GetApplication(ctx, app.ID)
	if err != nil || actual.CallbackURL != nil || actual.DeliveryMode != domain.DeliveryQueue {
		t.Fatalf("invalid or stale configuration persisted: %#v %v", actual, err)
	}
}
