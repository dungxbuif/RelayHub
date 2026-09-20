//go:build integration

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
)

func TestPostgresRealtimeFilesAreAppScopedAndCompleteOnce(t *testing.T) {
	client := integrationPostgresClient(t)
	resetControlTables(t, client)
	ctx := context.Background()
	now := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	for _, id := range []string{"app_a", "app_b"} {
		if err := client.CreateApplication(ctx, domain.App{ID: id, Name: id, DeliveryMode: domain.DeliveryWebSocket, Enabled: true, CreatedAt: now, UpdatedAt: now}, store.AppCredential{AppID: id, APIKeyHash: "hash-" + id, HMACSecret: []byte("secret")}); err != nil {
			t.Fatal(err)
		}
	}
	file := domain.RealtimeFile{ID: "file_1", AppID: "app_a", Channel: "room", Name: "photo.png", MIMEType: "image/png", SizeBytes: 12, SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ObjectKey: "realtime/app_a/file_1", Status: "pending", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if err := client.CreateRealtimeFile(ctx, file); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetRealtimeFile(ctx, "app_b", file.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cross-app get=%v", err)
	}
	ready, err := client.CompleteRealtimeFile(ctx, "app_a", file.ID, now.Add(time.Minute))
	if err != nil || ready.Status != "ready" || ready.CompletedAt == nil {
		t.Fatalf("ready=%#v error=%v", ready, err)
	}
	if _, err := client.CompleteRealtimeFile(ctx, "app_a", file.ID, now.Add(2*time.Minute)); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("second completion=%v", err)
	}
}
