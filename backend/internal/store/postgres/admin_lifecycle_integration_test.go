//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/adminread"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
)

func TestAdminReplayDeadLettersIsAtomicFencedAndIdempotent(t *testing.T) {
	client := integrationPostgresClient(t)
	ctx := context.Background()
	now := time.Date(2026, 9, 20, 5, 0, 0, 0, time.UTC)
	resetControlTables(t, client)

	source := domain.App{ID: "app_replay_source", Name: "source", DeliveryMode: domain.DeliveryWebSocket, Enabled: true, CreatedAt: now, UpdatedAt: now}
	callbackURL := "https://receiver.example/callback"
	target := domain.App{ID: "app_replay_target", Name: "target", CallbackURL: &callbackURL, DeliveryMode: domain.DeliveryAll, Enabled: true, CreatedAt: now, UpdatedAt: now}
	for _, app := range []domain.App{source, target} {
		if err := client.CreateApplication(ctx, app, store.AppCredential{AppID: app.ID, APIKeyHash: "hash_" + app.ID, HMACSecret: []byte("secret")}); err != nil {
			t.Fatal(err)
		}
	}
	event := domain.Event{ID: "evt_replay", Type: "order.created", SourceAppID: source.ID, TargetAppIDs: []string{target.ID}, Data: json.RawMessage(`{"safe":true}`), CreatedAt: now}
	job := domain.Job{ID: "job_replay", EventID: event.ID, SourceAppID: source.ID, TargetAppID: target.ID, CreatedAt: now, UpdatedAt: now}
	if _, _, err := client.PublishEvent(ctx, store.Publication{Event: event, Jobs: []domain.Job{job}}, "publish-replay", store.EventRetention{Event: 24 * time.Hour, Job: 24 * time.Hour, Idempotency: time.Hour}); err != nil {
		t.Fatal(err)
	}
	rows, err := client.pool.Query(ctx, `UPDATE deliveries SET status='dead_letter',attempts=3,callback_reason='attempts_exhausted',updated_at=$2 WHERE event_id=$1 RETURNING id`, event.ID, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	var deliveryIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		deliveryIDs = append(deliveryIDs, id)
	}
	rows.Close()
	sort.Strings(deliveryIDs)
	if len(deliveryIDs) != 2 {
		t.Fatalf("delivery IDs = %v", deliveryIDs)
	}
	if _, err := client.pool.Exec(ctx, `UPDATE outbox SET dispatched_at=$2,failed_at=$2,attempts=4,last_error='terminal',updated_at=$2 WHERE event_id=$1`, event.ID, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	command := adminread.ReplayCommand{DeliveryIDs: deliveryIDs, IdempotencyKeyHash: strings.Repeat("a", 64), RequestFingerprint: strings.Repeat("b", 64), ActorID: "session_operator", Now: now.Add(2 * time.Minute)}
	result, replayed, err := client.ReplayAdminDeadLetters(ctx, command)
	if err != nil || replayed || len(result.Items) != 2 {
		t.Fatalf("first replay = %#v replayed=%v error=%v", result, replayed, err)
	}
	for _, item := range result.Items {
		if item.FromGeneration != 1 || item.Generation != 2 || item.Status != "pending" {
			t.Fatalf("replay item = %#v", item)
		}
		var generation, attempts int64
		var status, messageID string
		var payload []byte
		var dispatchedAt, failedAt *time.Time
		if err := client.pool.QueryRow(ctx, `SELECT d.generation,d.attempts,d.status,o.message_id,o.payload,o.dispatched_at,o.failed_at FROM deliveries d JOIN outbox o ON o.delivery_id=d.id WHERE d.id=$1`, item.DeliveryID).Scan(&generation, &attempts, &status, &messageID, &payload, &dispatchedAt, &failedAt); err != nil {
			t.Fatal(err)
		}
		if generation != 2 || attempts != 0 || status != "pending" || !strings.HasSuffix(messageID, "-g2") || dispatchedAt != nil || failedAt != nil || !strings.Contains(string(payload), `"generation":2`) {
			t.Fatalf("reset state generation=%d attempts=%d status=%s message=%s payload=%s dispatched=%v failed=%v", generation, attempts, status, messageID, payload, dispatchedAt, failedAt)
		}
	}
	duplicate, replayed, err := client.ReplayAdminDeadLetters(ctx, command)
	if err != nil || !replayed || !replayResultsEqual(result, duplicate) {
		t.Fatalf("duplicate replay = %#v replayed=%v error=%v", duplicate, replayed, err)
	}
	var wait sync.WaitGroup
	concurrentErrors := make(chan error, 8)
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			concurrent, duplicate, err := client.ReplayAdminDeadLetters(ctx, command)
			if err == nil && (!duplicate || !replayResultsEqual(result, concurrent)) {
				err = errors.New("concurrent duplicate result diverged")
			}
			concurrentErrors <- err
		}()
	}
	wait.Wait()
	close(concurrentErrors)
	for err := range concurrentErrors {
		if err != nil {
			t.Fatal(err)
		}
	}
	rebound := command
	rebound.RequestFingerprint = strings.Repeat("c", 64)
	if _, _, err := client.ReplayAdminDeadLetters(ctx, rebound); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("idempotency key rebound error=%v", err)
	}
	var replayRows, auditRows int
	if err := client.pool.QueryRow(ctx, `SELECT count(*) FROM delivery_replays WHERE delivery_id=ANY($1)`, deliveryIDs).Scan(&replayRows); err != nil {
		t.Fatal(err)
	}
	if err := client.pool.QueryRow(ctx, `SELECT count(*) FROM audit_log WHERE action='delivery.replay' AND resource_id=ANY($1)`, deliveryIDs).Scan(&auditRows); err != nil {
		t.Fatal(err)
	}
	if replayRows != 2 || auditRows != 2 {
		t.Fatalf("replay rows=%d audit rows=%d", replayRows, auditRows)
	}
	timeline, err := client.GetAdminEventTimeline(ctx, event.ID)
	if err != nil {
		t.Fatal(err)
	}
	if timeline.Event.ID != event.ID || len(timeline.Deliveries) != 2 || timelineTypeCount(timeline, "operator.replayed") != 2 {
		t.Fatalf("replay timeline = %#v", timeline)
	}
	for index := 1; index < len(timeline.Items); index++ {
		if timeline.Items[index].OccurredAt.Before(timeline.Items[index-1].OccurredAt) {
			t.Fatalf("timeline not ordered at %d: %#v", index, timeline.Items)
		}
	}
	if _, err := client.pool.Exec(ctx, `UPDATE deliveries SET status='dead_letter' WHERE id=$1`, deliveryIDs[0]); err != nil {
		t.Fatal(err)
	}
	mixed := adminread.ReplayCommand{DeliveryIDs: deliveryIDs, IdempotencyKeyHash: strings.Repeat("f", 64), RequestFingerprint: strings.Repeat("1", 64), ActorID: "session_operator", Now: now.Add(3 * time.Minute)}
	if _, _, err := client.ReplayAdminDeadLetters(ctx, mixed); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("mixed-state batch error=%v", err)
	}
	var mixedRequestCount int
	if err := client.pool.QueryRow(ctx, `SELECT count(*) FROM admin_replay_requests WHERE idempotency_key_hash=$1`, mixed.IdempotencyKeyHash).Scan(&mixedRequestCount); err != nil || mixedRequestCount != 0 {
		t.Fatalf("mixed batch persisted request count=%d error=%v", mixedRequestCount, err)
	}
}

func replayResultsEqual(left, right adminread.ReplayResult) bool {
	leftJSON, _ := json.Marshal(left)
	rightJSON, _ := json.Marshal(right)
	return string(leftJSON) == string(rightJSON)
}

func timelineTypeCount(timeline adminread.EventTimeline, lifecycleType string) int {
	count := 0
	for _, item := range timeline.Items {
		if item.Type == lifecycleType {
			count++
		}
	}
	return count
}
