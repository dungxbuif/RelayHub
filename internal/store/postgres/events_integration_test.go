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
		for _, message := range first {
			if message.Reclaimed {
				t.Fatalf("fresh claim marked reclaimed: %#v", message)
			}
		}
		var attempts int64
		if err := client.pool.QueryRow(ctx, `SELECT max(attempts) FROM outbox WHERE claim_token='claim-one'`).Scan(&attempts); err != nil || attempts != 0 {
			t.Fatalf("claim penalized untouched rows: attempts=%d error=%v", attempts, err)
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
		for _, message := range reclaimed {
			if !message.Reclaimed {
				t.Fatalf("stale claim missing reclaimed flag: %#v", message)
			}
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
		dispatchedMessage := reclaimed[0]
		for _, candidate := range reclaimed {
			var target string
			if err := client.pool.QueryRow(ctx, `SELECT target_app_id FROM deliveries WHERE id=$1`, candidate.DeliveryID).Scan(&target); err != nil {
				t.Fatal(err)
			}
			if target == "stream" {
				dispatchedMessage = candidate
				break
			}
		}
		started, err := client.BeginOutboxPublish(ctx, dispatchedMessage.ID, "claim-three", now.Add(2*time.Minute), 3)
		if err != nil || started.Attempt != 1 || started.Exhausted {
			t.Fatalf("BeginOutboxPublish()=%#v error=%v", started, err)
		}
		if err := client.MarkOutboxDispatched(ctx, dispatchedMessage.ID, "claim-three", now.Add(2*time.Minute)); err != nil {
			t.Fatal(err)
		}
		if err := client.pool.QueryRow(ctx, `SELECT attempts FROM outbox WHERE id=$1`, dispatchedMessage.ID).Scan(&attempts); err != nil || attempts != 1 {
			t.Fatalf("completed dispatch attempts=%d error=%v", attempts, err)
		}
		if err := client.pool.QueryRow(ctx, `SELECT max(attempts) FROM outbox WHERE id<>$1 AND claim_token='claim-three'`, dispatchedMessage.ID).Scan(&attempts); err != nil || attempts != 0 {
			t.Fatalf("unvisited claimed rows attempts=%d error=%v", attempts, err)
		}
		if err := client.MarkOutboxDispatched(ctx, dispatchedMessage.ID, "claim-three", now.Add(2*time.Minute)); err != nil {
			t.Fatalf("idempotent completion error=%v", err)
		}
		var terminalOutboxID, terminalDeliveryID string
		if err := client.pool.QueryRow(ctx, `SELECT o.id,o.delivery_id FROM outbox o JOIN deliveries d ON d.id=o.delivery_id WHERE d.target_app_id='callback'`).Scan(&terminalOutboxID, &terminalDeliveryID); err != nil {
			t.Fatal(err)
		}
		if _, err := client.BeginOutboxPublish(ctx, terminalOutboxID, "claim-three", now.Add(2*time.Minute), 3); err != nil {
			t.Fatal(err)
		}
		if err := client.FailOutbox(ctx, terminalOutboxID, "claim-three", now.Add(2*time.Minute), "broker_unavailable"); err != nil {
			t.Fatal(err)
		}
		var failed bool
		var terminalAttempts int64
		var deliveryStatus string
		if err := client.pool.QueryRow(ctx, `SELECT failed_at IS NOT NULL,attempts FROM outbox WHERE id=$1`, terminalOutboxID).Scan(&failed, &terminalAttempts); err != nil || !failed || terminalAttempts != 1 {
			t.Fatalf("terminal outbox failed=%v attempts=%d error=%v", failed, terminalAttempts, err)
		}
		if err := client.pool.QueryRow(ctx, `SELECT status FROM deliveries WHERE id=$1`, terminalDeliveryID).Scan(&deliveryStatus); err != nil || deliveryStatus != "dead_letter" {
			t.Fatalf("terminal delivery status=%q error=%v", deliveryStatus, err)
		}
	})

	t.Run("durable assignment suppresses a second physical message beyond broker dedupe", func(t *testing.T) {
		var deliveryID string
		if err := client.pool.QueryRow(ctx, `SELECT id FROM deliveries WHERE target_app_id='all' AND sink='stream' LIMIT 1`).Scan(&deliveryID); err != nil {
			t.Fatal(err)
		}
		first, disposition, err := client.AssignStreamDelivery(ctx, deliveryID, "all", "conn-one", "assign-one", now, time.Minute)
		if err != nil || disposition != store.DeliveryAssigned || first.Attempt != 1 {
			t.Fatalf("first assignment=%#v disposition=%s error=%v", first, disposition, err)
		}
		_, disposition, err = client.AssignStreamDelivery(ctx, deliveryID, "all", "conn-two", "assign-two", now.Add(10*time.Second), time.Minute)
		if err != nil || disposition != store.DeliveryAlreadyAssigned {
			t.Fatalf("duplicate physical assignment disposition=%s error=%v", disposition, err)
		}
		if err := client.AcknowledgeStreamDelivery(ctx, deliveryID, "all", "conn-one", "assign-one", now.Add(20*time.Second)); err != nil {
			t.Fatal(err)
		}
		// This models a second broker message appended after its duplicate window.
		_, disposition, err = client.AssignStreamDelivery(ctx, deliveryID, "all", "conn-two", "assign-three", now.Add(48*time.Hour), time.Minute)
		if err != nil || disposition != store.DeliveryAlreadyComplete {
			t.Fatalf("post-window duplicate disposition=%s error=%v", disposition, err)
		}
	})

	t.Run("nack and progress retain the complete assignment fence", func(t *testing.T) {
		publication := eventPublication(now.Add(3*time.Hour), "evt_stream_controls", []string{"stream"})
		if _, _, err := client.PublishEvent(ctx, publication, "idem-stream-controls", store.EventRetention{Event:24*time.Hour,Job:24*time.Hour,Idempotency:time.Hour}); err != nil { t.Fatal(err) }
		var deliveryID string
		if err:=client.pool.QueryRow(ctx,`SELECT id FROM deliveries WHERE event_id=$1 AND sink='stream'`,publication.Event.ID).Scan(&deliveryID);err!=nil{t.Fatal(err)}
		assigned,disposition,err:=client.AssignStreamDelivery(ctx,deliveryID,"stream","conn-one","token-one",now.Add(3*time.Hour),time.Minute)
		if err!=nil||disposition!=store.DeliveryAssigned{t.Fatalf("assignment=%#v disposition=%s error=%v",assigned,disposition,err)}
		progressAt:=now.Add(3*time.Hour+10*time.Second)
		if err:=client.ProgressStreamDelivery(ctx,deliveryID,"stream","conn-one","token-one",progressAt,2*time.Minute);err!=nil{t.Fatal(err)}
		var expires time.Time
		if err:=client.pool.QueryRow(ctx,`SELECT assignment_expires_at FROM deliveries WHERE id=$1`,deliveryID).Scan(&expires);err!=nil||!expires.Equal(progressAt.Add(2*time.Minute)){t.Fatalf("expiry=%v error=%v",expires,err)}
		if err:=client.ProgressStreamDelivery(ctx,deliveryID,"stream","conn-one","wrong-token",progressAt,time.Minute);!errors.Is(err,store.ErrConflict){t.Fatalf("wrong progress fence=%v",err)}
		if err:=client.ReleaseStreamDelivery(ctx,deliveryID,"other","conn-one","token-one",progressAt);!errors.Is(err,store.ErrNotFound){t.Fatalf("cross-app release=%v",err)}
		if err:=client.ReleaseStreamDelivery(ctx,deliveryID,"stream","conn-old","token-one",progressAt);!errors.Is(err,store.ErrConflict){t.Fatalf("stale-session release=%v",err)}
		if err:=client.ReleaseStreamDelivery(ctx,deliveryID,"stream","conn-one","token-one",progressAt);err!=nil{t.Fatal(err)}
		var status string;var assignedConnection *string
		if err:=client.pool.QueryRow(ctx,`SELECT status,assigned_connection_id FROM deliveries WHERE id=$1`,deliveryID).Scan(&status,&assignedConnection);err!=nil||status!="retrying"||assignedConnection!=nil{t.Fatalf("status=%q assigned=%v error=%v",status,assignedConnection,err)}
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
