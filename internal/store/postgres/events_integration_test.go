//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
)

func TestTransactionalEventAcceptanceAndOutbox(t *testing.T) {
	client := integrationPostgresClient(t)
	ctx := context.Background()
	resetControlTables(t, client)
	now := time.Date(2026, 9, 12, 10, 30, 0, 0, time.UTC)
	createEventTestApp(t, client, now, domain.App{ID: "producer", Name: "producer", DeliveryMode: domain.DeliveryWebSocket, Enabled: true})
	createEventTestApp(t, client, now, domain.App{ID: "stream", Name: "stream", DeliveryMode: domain.DeliveryWebSocket, Enabled: true})
	callbackURL := "https://callback.internal/events"
	createEventTestApp(t, client, now, domain.App{ID: "callback", Name: "callback", CallbackURL: &callbackURL, DeliveryMode: domain.DeliveryCallback, Enabled: true})
	createEventTestApp(t, client, now, domain.App{ID: "all", Name: "all", CallbackURL: &callbackURL, DeliveryMode: domain.DeliveryAll, Enabled: true})
	createEventTestApp(t, client, now, domain.App{ID: "disabled", Name: "disabled", DeliveryMode: domain.DeliveryWebSocket, Enabled: false})

	t.Run("commit preserves JSON and creates independent sink rows", func(t *testing.T) {
		publication := eventPublication(now, "evt_atomic", []string{"stream", "callback", "all"})
		got, replay, err := client.PublishEvent(ctx, publication, "idem-atomic", store.EventRetention{Event: 24 * time.Hour, Job: 24 * time.Hour, Idempotency: time.Hour})
		if err != nil || replay || got.Event.ID != publication.Event.ID {
			t.Fatalf("PublishEvent() = %#v replay=%v error=%v", got, replay, err)
		}
		var data string
		if err := client.pool.QueryRow(ctx, `SELECT data::text FROM events WHERE id=$1`, publication.Event.ID).Scan(&data); err != nil || data != `{"n":9007199254740993,"empty":{}}` {
			t.Fatalf("stored data=%q error=%v", data, err)
		}
		rows, err := client.pool.Query(ctx, `SELECT target_app_id,sink FROM deliveries WHERE event_id=$1 ORDER BY target_app_id,sink`, publication.Event.ID)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var sinks []string
		for rows.Next() {
			var target, sink string
			if err := rows.Scan(&target, &sink); err != nil {
				t.Fatal(err)
			}
			sinks = append(sinks, target+":"+sink)
		}
		want := []string{"all:callback", "all:stream", "callback:callback", "stream:stream"}
		if len(sinks) != len(want) {
			t.Fatalf("delivery sinks=%v want=%v", sinks, want)
		}
		for index := range want {
			if sinks[index] != want[index] {
				t.Fatalf("delivery sinks=%v want=%v", sinks, want)
			}
		}
		var outboxCount int
		if err := client.pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE event_id=$1`, publication.Event.ID).Scan(&outboxCount); err != nil || outboxCount != 4 {
			t.Fatalf("outbox count=%d error=%v", outboxCount, err)
		}

		changed := eventPublication(now.Add(time.Hour), "evt_changed", []string{"stream"})
		changed.Event.Type = "changed"
		replayed, replay, err := client.PublishEvent(ctx, changed, "idem-atomic", store.EventRetention{Event: 24 * time.Hour, Idempotency: time.Hour})
		if err != nil || !replay || replayed.Event.ID != publication.Event.ID || string(replayed.Event.Data) != string(publication.Event.Data) || len(replayed.Jobs) != len(publication.Jobs) {
			t.Fatalf("replay = %#v replay=%v error=%v", replayed, replay, err)
		}
	})

	t.Run("invalid target rolls back every row", func(t *testing.T) {
		publication := eventPublication(now, "evt_invalid", []string{"stream", "disabled"})
		_, _, err := client.PublishEvent(ctx, publication, "idem-invalid", store.EventRetention{Event: time.Hour, Idempotency: time.Hour})
		if !errors.Is(err, store.ErrInvalidTarget) {
			t.Fatalf("PublishEvent() error=%v", err)
		}
		for _, table := range []string{"events", "event_idempotency", "deliveries", "outbox"} {
			var count int
			if err := client.pool.QueryRow(ctx, `SELECT count(*) FROM `+table+` WHERE `+map[string]string{"events": "id", "event_idempotency": "event_id", "deliveries": "event_id", "outbox": "event_id"}[table]+`=$1`, publication.Event.ID).Scan(&count); err != nil || count != 0 {
				t.Fatalf("%s count=%d error=%v", table, count, err)
			}
		}
	})

	t.Run("concurrent idempotency returns one exact publication", func(t *testing.T) {
		const workers = 12
		results := make(chan store.Publication, workers)
		errorsCh := make(chan error, workers)
		var group sync.WaitGroup
		for index := range workers {
			group.Add(1)
			go func() {
				defer group.Done()
				publication := eventPublication(now, "evt_concurrent_"+string(rune('a'+index)), []string{"stream"})
				got, _, err := client.PublishEvent(ctx, publication, "idem-concurrent", store.EventRetention{Event: time.Hour, Idempotency: time.Hour})
				if err != nil {
					errorsCh <- err
					return
				}
				results <- got
			}()
		}
		group.Wait()
		close(results)
		close(errorsCh)
		if len(errorsCh) != 0 {
			t.Fatalf("concurrent errors=%v", <-errorsCh)
		}
		winner := ""
		for result := range results {
			if winner == "" {
				winner = result.Event.ID
			}
			if result.Event.ID != winner {
				t.Fatalf("idempotency winners %q and %q", winner, result.Event.ID)
			}
		}
		var eventCount int
		if err := client.pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE id LIKE 'evt_concurrent_%'`).Scan(&eventCount); err != nil || eventCount != 1 {
			t.Fatalf("event count=%d error=%v", eventCount, err)
		}
	})

	t.Run("target disable serializes before acceptance", func(t *testing.T) {
		tx, err := client.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `UPDATE applications SET enabled=false WHERE id='stream'`); err != nil {
			t.Fatal(err)
		}
		result := make(chan error, 1)
		go func() {
			publication := eventPublication(now, "evt_disable_race", []string{"stream"})
			_, _, err := client.PublishEvent(ctx, publication, "idem-disable-race", store.EventRetention{Event: time.Hour, Idempotency: time.Hour})
			result <- err
		}()
		select {
		case err := <-result:
			t.Fatalf("publication bypassed target row lock: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err := <-result; !errors.Is(err, store.ErrInvalidTarget) {
			t.Fatalf("raced publication error=%v", err)
		}
		var count int
		if err := client.pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE id='evt_disable_race'`).Scan(&count); err != nil || count != 0 {
			t.Fatalf("raced event count=%d error=%v", count, err)
		}
	})

	t.Run("outbox claims are fenced and stale claims reuse message identity", func(t *testing.T) {
		first, err := client.ClaimOutbox(ctx, now, now.Add(-time.Minute), "claim-one", 100)
		if err != nil || len(first) == 0 {
			t.Fatalf("ClaimOutbox()=%#v error=%v", first, err)
		}
		second, err := client.ClaimOutbox(ctx, now, now.Add(-time.Minute), "claim-two", 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(second) != 0 {
			t.Fatalf("active claim leaked rows: %#v", second)
		}
		if err := client.MarkOutboxDispatched(ctx, first[0].ID, "wrong-token", now); !errors.Is(err, store.ErrConflict) {
			t.Fatalf("wrong-token completion error=%v", err)
		}
		reclaimed, err := client.ClaimOutbox(ctx, now.Add(2*time.Minute), now.Add(time.Minute), "claim-three", 100)
		if err != nil || len(reclaimed) != len(first) {
			t.Fatalf("stale reclaim=%d first=%d error=%v", len(reclaimed), len(first), err)
		}
		identities := map[string]string{}
		for _, message := range first {
			identities[message.ID] = message.MessageID
		}
		for _, message := range reclaimed {
			if identities[message.ID] != message.MessageID {
				t.Fatalf("message identity changed for %s", message.ID)
			}
		}
		if err := client.MarkOutboxDispatched(ctx, reclaimed[0].ID, "claim-three", now.Add(2*time.Minute)); err != nil {
			t.Fatal(err)
		}
		if err := client.MarkOutboxDispatched(ctx, reclaimed[0].ID, "claim-three", now.Add(2*time.Minute)); err != nil {
			t.Fatalf("idempotent completion error=%v", err)
		}
	})
}

func createEventTestApp(t *testing.T, client *Client, now time.Time, app domain.App) {
	t.Helper()
	app.CreatedAt, app.UpdatedAt = now, now
	if err := client.CreateApplication(context.Background(), app, store.AppCredential{AppID: app.ID, APIKeyHash: "hash-" + app.ID, HMACSecret: []byte("secret")}); err != nil {
		t.Fatal(err)
	}
}

func eventPublication(now time.Time, eventID string, targets []string) store.Publication {
	event := domain.Event{ID: eventID, Type: "order.created", SourceAppID: "producer", TargetAppIDs: append([]string(nil), targets...), Data: json.RawMessage(`{"n":9007199254740993,"empty":{}}`), CreatedAt: now}
	jobs := make([]domain.Job, len(targets))
	for index, target := range targets {
		jobs[index] = domain.Job{ID: "job_" + eventID + "_" + target, EventID: eventID, SourceAppID: "producer", TargetAppID: target, Status: domain.JobPending, CreatedAt: now, UpdatedAt: now}
	}
	return store.Publication{Event: event, Jobs: jobs}
}
