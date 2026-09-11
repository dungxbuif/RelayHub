//go:build integration

package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
)

func TestPostgresCallbackAttempts(t *testing.T) {
	client := integrationPostgresClient(t)
	ctx := context.Background()
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	resetControlTables(t, client)

	source := domain.App{ID: "app_source", Name: "source", DeliveryMode: domain.DeliveryWebSocket, Enabled: true, CreatedAt: now, UpdatedAt: now}
	oldURL := "https://old.example/callback"
	target := domain.App{ID: "app_target", Name: "target", CallbackURL: &oldURL, DeliveryMode: domain.DeliveryCallback, Enabled: true, CreatedAt: now, UpdatedAt: now}
	if err := client.CreateApplication(ctx, source, store.AppCredential{AppID: source.ID, APIKeyHash: "source-hash", HMACSecret: []byte("source-secret")}); err != nil {
		t.Fatal(err)
	}
	if err := client.CreateApplication(ctx, target, store.AppCredential{AppID: target.ID, APIKeyHash: "target-old", HMACSecret: []byte("old-secret")}); err != nil {
		t.Fatal(err)
	}
	event := domain.Event{ID: "evt_callback", Type: "order.created", SourceAppID: source.ID, TargetAppIDs: []string{target.ID}, Data: json.RawMessage(`{"amount":42}`), CreatedAt: now}
	job := domain.Job{ID: "job_callback", EventID: event.ID, SourceAppID: source.ID, TargetAppID: target.ID, CreatedAt: now, UpdatedAt: now}
	if _, _, err := client.PublishEvent(ctx, store.Publication{Event: event, Jobs: []domain.Job{job}}, "callback-key", store.EventRetention{Event: time.Hour, Job: time.Hour, Idempotency: time.Hour}); err != nil {
		t.Fatal(err)
	}
	var deliveryID string
	if err := client.pool.QueryRow(ctx, `SELECT id FROM deliveries WHERE public_job_id=$1 AND sink='callback'`, job.ID).Scan(&deliveryID); err != nil {
		t.Fatal(err)
	}

	newURL := "https://new.example/callback"
	target.CallbackURL = &newURL
	target.UpdatedAt = now.Add(time.Minute)
	if _, err := client.UpdateApplication(ctx, target); err != nil {
		t.Fatal(err)
	}
	if err := client.RotateApplicationCredential(ctx, target.ID, store.AppCredential{AppID: target.ID, APIKeyHash: "target-new", HMACSecret: []byte("new-secret")}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	dispatch, disposition, err := client.BeginCallbackAttempt(ctx, deliveryID, "worker-a", now.Add(2*time.Minute), 30*time.Second)
	if err != nil || disposition != store.CallbackDispatchReady {
		t.Fatalf("BeginCallbackAttempt() disposition=%q error=%v", disposition, err)
	}
	if dispatch.Attempt != 1 || dispatch.CredentialVersion != 2 || dispatch.App.CallbackURL == nil || *dispatch.App.CallbackURL != newURL || !bytes.Equal(dispatch.Secret, []byte("new-secret")) {
		t.Fatalf("dispatch did not revalidate current callback state: %+v", dispatch)
	}
	if string(dispatch.Body) != `{"id":"evt_callback","type":"order.created","source_app_id":"app_source","target_app_ids":["app_target"],"data":{"amount":42},"created_at":"2026-09-12T12:00:00Z"}` {
		t.Fatalf("body=%s", dispatch.Body)
	}

	busy, disposition, err := client.BeginCallbackAttempt(ctx, deliveryID, "worker-b", now.Add(2*time.Minute+time.Second), 30*time.Second)
	if err != nil || disposition != store.CallbackDispatchBusy || !busy.RetryAt.Equal(dispatch.LeaseExpiresAt) {
		t.Fatalf("busy=%+v disposition=%q error=%v", busy, disposition, err)
	}
	if err := client.FinishCallbackAttempt(ctx, deliveryID, "stale-token", dispatch.Attempt, store.CallbackAttemptTransition{Status: domain.JobDelivered, Now: now.Add(3 * time.Minute), Reason: "http_success"}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale finish error=%v", err)
	}
	takeover, disposition, err := client.BeginCallbackAttempt(ctx, deliveryID, "worker-b", dispatch.LeaseExpiresAt, 30*time.Second)
	if err != nil || disposition != store.CallbackDispatchReady || takeover.Attempt != 2 {
		t.Fatalf("takeover=%+v disposition=%q error=%v", takeover, disposition, err)
	}
	retryAt := now.Add(4*time.Minute + 789*time.Nanosecond)
	if err := client.FinishCallbackAttempt(ctx, deliveryID, takeover.Token, takeover.Attempt, store.CallbackAttemptTransition{Status: domain.JobPending, Now: dispatch.LeaseExpiresAt.Add(time.Second), RetryAt: retryAt, Reason: "http_transient"}); err != nil {
		t.Fatal(err)
	}
	waiting, disposition, err := client.BeginCallbackAttempt(ctx, deliveryID, "worker-b", retryAt.Add(-time.Second), 30*time.Second)
	if err != nil || disposition != store.CallbackDispatchBusy || !waiting.RetryAt.Equal(retryAt.Truncate(time.Microsecond)) {
		t.Fatalf("waiting=%+v disposition=%q error=%v", waiting, disposition, err)
	}
	second, disposition, err := client.BeginCallbackAttempt(ctx, deliveryID, "worker-b", retryAt, 30*time.Second)
	if err != nil || disposition != store.CallbackDispatchReady || second.Attempt != 3 {
		t.Fatalf("second=%+v disposition=%q error=%v", second, disposition, err)
	}
	dead := store.CallbackAttemptTransition{Status: domain.JobDeadLetter, Now: retryAt.Add(time.Second), Reason: "http_permanent"}
	if err := client.FinishCallbackAttempt(ctx, deliveryID, second.Token, second.Attempt, dead); err != nil {
		t.Fatal(err)
	}
	if err := client.FinishCallbackAttempt(ctx, deliveryID, second.Token, second.Attempt, dead); err != nil {
		t.Fatalf("idempotent finish error=%v", err)
	}
	terminal, disposition, err := client.BeginCallbackAttempt(ctx, deliveryID, "worker-c", retryAt.Add(2*time.Second), 30*time.Second)
	if err != nil || disposition != store.CallbackDispatchDeadLetter || terminal.DLQPublished {
		t.Fatalf("terminal=%+v disposition=%q error=%v", terminal, disposition, err)
	}
	if err := client.MarkCallbackDLQPublished(ctx, deliveryID, retryAt.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	terminal, disposition, err = client.BeginCallbackAttempt(ctx, deliveryID, "worker-c", retryAt.Add(4*time.Second), 30*time.Second)
	if err != nil || disposition != store.CallbackDispatchDeadLetter || !terminal.DLQPublished {
		t.Fatalf("published terminal=%+v disposition=%q error=%v", terminal, disposition, err)
	}

	event2 := event
	event2.ID = "evt_callback_success"
	event2.CreatedAt = retryAt.Add(5 * time.Second)
	job2 := job
	job2.ID, job2.EventID, job2.CreatedAt, job2.UpdatedAt = "job_callback_success", event2.ID, event2.CreatedAt, event2.CreatedAt
	if _, _, err := client.PublishEvent(ctx, store.Publication{Event: event2, Jobs: []domain.Job{job2}}, "callback-success-key", store.EventRetention{Event: time.Hour, Job: time.Hour, Idempotency: time.Hour}); err != nil {
		t.Fatal(err)
	}
	var delivery2 string
	if err := client.pool.QueryRow(ctx, `SELECT id FROM deliveries WHERE public_job_id=$1 AND sink='callback'`, job2.ID).Scan(&delivery2); err != nil {
		t.Fatal(err)
	}
	success, disposition, err := client.BeginCallbackAttempt(ctx, delivery2, "worker-success", event2.CreatedAt.Add(time.Second), 30*time.Second)
	if err != nil || disposition != store.CallbackDispatchReady {
		t.Fatalf("success begin=%+v disposition=%q error=%v", success, disposition, err)
	}
	if err := client.FinishCallbackAttempt(ctx, delivery2, success.Token, success.Attempt, store.CallbackAttemptTransition{Status: domain.JobDelivered, Now: event2.CreatedAt.Add(2 * time.Second), Reason: "http_success"}); err != nil {
		t.Fatal(err)
	}
	gotJob, err := client.GetJob(ctx, job2.ID)
	if err != nil || gotJob.Status != domain.JobDelivered || gotJob.CallbackAttempts != 1 {
		t.Fatalf("successful callback job=%+v error=%v", gotJob, err)
	}
}
