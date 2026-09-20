//go:build integration

package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
)

func TestQueueV2PublicationFansOutToIndependentEnabledSubscriptions(t *testing.T) {
	client := integrationPostgresClient(t)
	resetControlTables(t, client)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	createEventTestApp(t, client, now, domain.App{ID: "queue_source", Name: "queue_source", DeliveryMode: domain.DeliveryWebSocket, Enabled: true})
	createEventTestApp(t, client, now, domain.App{ID: "queue_target", Name: "queue_target", DeliveryMode: domain.DeliveryQueue, Enabled: true})
	for _, subscription := range []struct {
		id, name string
		enabled  bool
	}{
		{"sub_orders", "orders", true},
		{"sub_analytics", "analytics", true},
		{"sub_disabled", "disabled", false},
	} {
		if _, err := client.pool.Exec(ctx, `INSERT INTO queue_subscriptions(id,app_id,name,enabled,created_at,updated_at) VALUES($1,'queue_target',$2,$3,$4,$4)`, subscription.id, subscription.name, subscription.enabled, now); err != nil {
			t.Fatal(err)
		}
	}
	publication := eventPublication(now, "evt_queue_v2", []string{"queue_target"})
	publication.Event.SourceAppID = "queue_source"
	publication.Jobs[0].SourceAppID = "queue_source"
	if _, replay, err := client.PublishEvent(ctx, publication, "idem-queue-v2", store.EventRetention{Event: 24 * time.Hour, Job: 24 * time.Hour, Idempotency: time.Hour}); err != nil || replay {
		t.Fatalf("PublishEvent() replay=%v error=%v", replay, err)
	}
	rows, err := client.pool.Query(ctx, `SELECT subscription_id,status,generation FROM queue_deliveries WHERE event_id=$1 ORDER BY subscription_id`, publication.Event.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var subscription, status string
		var generation int64
		if err := rows.Scan(&subscription, &status, &generation); err != nil {
			t.Fatal(err)
		}
		got = append(got, subscription+":"+status)
		if generation != 1 {
			t.Fatalf("generation=%d", generation)
		}
	}
	if len(got) != 2 || got[0] != "sub_analytics:available" || got[1] != "sub_orders:available" {
		t.Fatalf("queue deliveries=%v", got)
	}
	replayedPublication := eventPublication(now.Add(time.Minute), "evt_queue_other", []string{"queue_target"})
	replayedPublication.Event.SourceAppID = "queue_source"
	replayedPublication.Jobs[0].SourceAppID = "queue_source"
	if _, _, err := client.PublishEvent(ctx, replayedPublication, "idem-queue-v2", store.EventRetention{Event: time.Hour, Idempotency: time.Hour}); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := client.pool.QueryRow(ctx, `SELECT count(*) FROM queue_deliveries WHERE event_id=$1`, publication.Event.ID).Scan(&count); err != nil || count != 2 {
		t.Fatalf("idempotent queue count=%d error=%v", count, err)
	}
}

func TestQueueV2ConcurrentPullersReceiveDisjointBatches(t *testing.T) {
	client := integrationPostgresClient(t)
	resetControlTables(t, client)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	createEventTestApp(t, client, now, domain.App{ID: "queue_source", Name: "queue_source", DeliveryMode: domain.DeliveryWebSocket, Enabled: true})
	createEventTestApp(t, client, now, domain.App{ID: "queue_concurrent", Name: "queue_concurrent", DeliveryMode: domain.DeliveryQueue, Enabled: true})
	subscription := queueTestSubscription(now, "sub_concurrent", "queue_concurrent")
	if err := client.CreateQueueSubscription(ctx, subscription); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 10; index++ {
		eventID := fmt.Sprintf("evt_concurrent_%02d", index)
		publication := eventPublication(now.Add(time.Duration(index)*time.Millisecond), eventID, []string{"queue_concurrent"})
		publication.Event.SourceAppID = "queue_source"
		publication.Jobs[0].SourceAppID = "queue_source"
		if _, _, err := client.PublishEvent(ctx, publication, eventID, store.EventRetention{Event: time.Hour, Idempotency: time.Hour}); err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	type result struct {
		items []domain.QueueDelivery
		err   error
	}
	results := make(chan result, 2)
	var wait sync.WaitGroup
	for puller := 0; puller < 2; puller++ {
		wait.Add(1)
		go func(puller int) {
			defer wait.Done()
			<-start
			receipts := make([]string, 5)
			for index := range receipts {
				receipts[index] = fmt.Sprintf("receipt-%d-%d", puller, index)
			}
			items, err := client.PullQueueDeliveries(ctx, store.QueuePullRequest{AppID: "queue_concurrent", SubscriptionID: subscription.ID, Limit: 5, Now: now.Add(time.Second), Receipts: receipts})
			results <- result{items, err}
		}(puller)
	}
	close(start)
	wait.Wait()
	close(results)
	seen := map[string]struct{}{}
	for pulled := range results {
		if pulled.err != nil || len(pulled.items) != 5 {
			t.Fatalf("pull items=%d error=%v", len(pulled.items), pulled.err)
		}
		for _, item := range pulled.items {
			if _, duplicate := seen[item.ID]; duplicate {
				t.Fatalf("duplicate delivery %s", item.ID)
			}
			seen[item.ID] = struct{}{}
		}
	}
	if len(seen) != 10 {
		t.Fatalf("unique deliveries=%d", len(seen))
	}
}

func TestQueueV2LeaseSettlementFencingAndReplay(t *testing.T) {
	client := integrationPostgresClient(t)
	resetControlTables(t, client)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	createEventTestApp(t, client, now, domain.App{ID: "queue_source", Name: "queue_source", DeliveryMode: domain.DeliveryWebSocket, Enabled: true})
	createEventTestApp(t, client, now, domain.App{ID: "queue_worker", Name: "queue_worker", DeliveryMode: domain.DeliveryQueue, Enabled: true})
	subscription := queueTestSubscription(now, "sub_worker", "queue_worker")
	subscription.MaxAttempts = 3
	subscription.RetryDelaySeconds = 5
	if err := client.CreateQueueSubscription(ctx, subscription); err != nil {
		t.Fatal(err)
	}
	publication := eventPublication(now, "evt_queue_lease", []string{"queue_worker"})
	publication.Event.SourceAppID = "queue_source"
	publication.Jobs[0].SourceAppID = "queue_source"
	if _, _, err := client.PublishEvent(ctx, publication, "lease-key", store.EventRetention{Event: time.Hour, Idempotency: time.Hour}); err != nil {
		t.Fatal(err)
	}

	first, err := client.PullQueueDeliveries(ctx, store.QueuePullRequest{AppID: "queue_worker", SubscriptionID: subscription.ID, Limit: 1, Visibility: 30 * time.Second, Now: now, Receipts: []string{"receipt-one"}})
	if err != nil || len(first) != 1 || first[0].Attempt != 1 || first[0].Generation != 1 {
		t.Fatalf("first pull=%+v error=%v", first, err)
	}
	stale, err := client.SettleQueueDeliveries(ctx, "queue_worker", subscription.ID, []store.QueueSettlement{{Receipt: "receipt-one", Disposition: store.QueueAcknowledge}}, now.Add(31*time.Second))
	if err != nil || len(stale) != 1 || stale[0].Status != "invalid_receipt" {
		t.Fatalf("stale settlement=%+v error=%v", stale, err)
	}
	second, err := client.PullQueueDeliveries(ctx, store.QueuePullRequest{AppID: "queue_worker", SubscriptionID: subscription.ID, Limit: 1, Visibility: 30 * time.Second, Now: now.Add(37 * time.Second), Receipts: []string{"receipt-two"}})
	if err != nil || len(second) != 1 || second[0].Attempt != 2 {
		t.Fatalf("second pull=%+v error=%v", second, err)
	}
	staleAfterReclaim, err := client.SettleQueueDeliveries(ctx, "queue_worker", subscription.ID, []store.QueueSettlement{{Receipt: "receipt-one", Disposition: store.QueueAcknowledge}}, now.Add(38*time.Second))
	if err != nil || len(staleAfterReclaim) != 1 || staleAfterReclaim[0].Status != "invalid_receipt" {
		t.Fatalf("old owner settled reclaimed delivery: %+v error=%v", staleAfterReclaim, err)
	}
	staleHeartbeat, err := client.ExtendQueueLeases(ctx, "queue_worker", subscription.ID, []store.QueueLeaseExtension{{Receipt: "receipt-one", Extension: 30 * time.Second}}, now.Add(38*time.Second))
	if err != nil || len(staleHeartbeat) != 1 || staleHeartbeat[0].Status != "invalid_receipt" {
		t.Fatalf("old owner renewed reclaimed delivery: %+v error=%v", staleHeartbeat, err)
	}
	extended, err := client.ExtendQueueLeases(ctx, "queue_worker", subscription.ID, []store.QueueLeaseExtension{{Receipt: "receipt-two", Extension: 10 * time.Second}}, now.Add(38*time.Second))
	if err != nil || extended[0].Status != "extended" {
		t.Fatalf("extension=%+v error=%v", extended, err)
	}
	retried, err := client.SettleQueueDeliveries(ctx, "queue_worker", subscription.ID, []store.QueueSettlement{{Receipt: "receipt-two", Disposition: store.QueueRetry, Delay: 5 * time.Second, Reason: "transient"}}, now.Add(39*time.Second))
	if err != nil || retried[0].Status != "available" {
		t.Fatalf("retry=%+v error=%v", retried, err)
	}
	third, err := client.PullQueueDeliveries(ctx, store.QueuePullRequest{AppID: "queue_worker", SubscriptionID: subscription.ID, Limit: 1, Now: now.Add(45 * time.Second), Receipts: []string{"receipt-three"}})
	if err != nil || len(third) != 1 || third[0].Attempt != 3 {
		t.Fatalf("third pull=%+v error=%v", third, err)
	}
	dead, err := client.SettleQueueDeliveries(ctx, "queue_worker", subscription.ID, []store.QueueSettlement{{Receipt: "receipt-three", Disposition: store.QueueRetry, Reason: "still failing"}}, now.Add(46*time.Second))
	if err != nil || dead[0].Status != "dead_letter" {
		t.Fatalf("dead-letter=%+v error=%v", dead, err)
	}
	dlq, err := client.ListQueueDeadLetters(ctx, "queue_worker", subscription.ID, store.QueueDeadLetterQuery{Limit: 10})
	if err != nil || len(dlq) != 1 || dlq[0].Reason != "max_attempts_exhausted" {
		t.Fatalf("dlq=%+v error=%v", dlq, err)
	}
	if count, err := client.ReplayQueueDeadLetters(ctx, "queue_worker", subscription.ID, []string{dlq[0].DeliveryID}, now.Add(47*time.Second)); err != nil || count != 1 {
		t.Fatalf("replay count=%d error=%v", count, err)
	}
	replayed, err := client.PullQueueDeliveries(ctx, store.QueuePullRequest{AppID: "queue_worker", SubscriptionID: subscription.ID, Limit: 1, Now: now.Add(48 * time.Second), Receipts: []string{"receipt-four"}})
	if err != nil || len(replayed) != 1 || replayed[0].Generation != 2 || replayed[0].Attempt != 1 {
		t.Fatalf("replayed pull=%+v error=%v", replayed, err)
	}
	acked, err := client.SettleQueueDeliveries(ctx, "queue_worker", subscription.ID, []store.QueueSettlement{{Receipt: "receipt-four", Disposition: store.QueueAcknowledge}}, now.Add(49*time.Second))
	if err != nil || acked[0].Status != "acked" {
		t.Fatalf("ack=%+v error=%v", acked, err)
	}
	duplicate, err := client.SettleQueueDeliveries(ctx, "queue_worker", subscription.ID, []store.QueueSettlement{{Receipt: "receipt-four", Disposition: store.QueueAcknowledge}}, now.Add(50*time.Second))
	if err != nil || len(duplicate) != 1 || duplicate[0].Status != "invalid_receipt" {
		t.Fatalf("duplicate ACK mutated delivery: %+v error=%v", duplicate, err)
	}
}

func TestQueueV2SingleDeliveryHasOneConcurrentOwner(t *testing.T) {
	client := integrationPostgresClient(t)
	resetControlTables(t, client)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	createEventTestApp(t, client, now, domain.App{ID: "queue_source", Name: "source", DeliveryMode: domain.DeliveryWebSocket, Enabled: true})
	createEventTestApp(t, client, now, domain.App{ID: "queue_worker", Name: "worker", DeliveryMode: domain.DeliveryQueue, Enabled: true})
	subscription := queueTestSubscription(now, "sub_shared", "queue_worker")
	if err := client.CreateQueueSubscription(ctx, subscription); err != nil {
		t.Fatal(err)
	}
	publication := eventPublication(now, "evt_single_owner", []string{"queue_worker"})
	publication.Event.SourceAppID = "queue_source"
	publication.Jobs[0].SourceAppID = "queue_source"
	if _, _, err := client.PublishEvent(ctx, publication, "single-owner", store.EventRetention{Event: time.Hour, Idempotency: time.Hour}); err != nil {
		t.Fatal(err)
	}
	type result struct {
		items []domain.QueueDelivery
		err   error
	}
	results := make(chan result, 16)
	start := make(chan struct{})
	for index := 0; index < 16; index++ {
		go func(index int) {
			<-start
			items, err := client.PullQueueDeliveries(ctx, store.QueuePullRequest{AppID: "queue_worker", SubscriptionID: subscription.ID, Limit: 1, Now: now, Receipts: []string{fmt.Sprintf("owner-%d", index)}})
			results <- result{items, err}
		}(index)
	}
	close(start)
	owners := 0
	for index := 0; index < 16; index++ {
		result := <-results
		if result.err != nil {
			t.Errorf("pull failed: %v", result.err)
		}
		owners += len(result.items)
	}
	if owners != 1 {
		t.Fatalf("owners=%d, want exactly one valid owner", owners)
	}
}

func TestQueueV2OrderingKeyBlocksLaterDelivery(t *testing.T) {
	client := integrationPostgresClient(t)
	resetControlTables(t, client)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	createEventTestApp(t, client, now, domain.App{ID: "queue_source", Name: "queue_source", DeliveryMode: domain.DeliveryWebSocket, Enabled: true})
	createEventTestApp(t, client, now, domain.App{ID: "queue_ordered", Name: "queue_ordered", DeliveryMode: domain.DeliveryQueue, Enabled: true})
	subscription := queueTestSubscription(now, "sub_ordered", "queue_ordered")
	subscription.OrderingMode = domain.QueueOrderingKey
	if err := client.CreateQueueSubscription(ctx, subscription); err != nil {
		t.Fatal(err)
	}
	for index, eventID := range []string{"evt_order_1", "evt_order_2"} {
		publication := eventPublication(now.Add(time.Duration(index)*time.Second), eventID, []string{"queue_ordered"})
		publication.Event.SourceAppID = "queue_source"
		publication.Jobs[0].SourceAppID = "queue_source"
		if _, _, err := client.PublishEvent(ctx, publication, eventID, store.EventRetention{Event: time.Hour, Idempotency: time.Hour}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := client.pool.Exec(ctx, `UPDATE queue_deliveries SET ordering_key='customer:1' WHERE subscription_id=$1`, subscription.ID); err != nil {
		t.Fatal(err)
	}
	items, err := client.PullQueueDeliveries(ctx, store.QueuePullRequest{AppID: "queue_ordered", SubscriptionID: subscription.ID, Limit: 2, Now: now.Add(2 * time.Second), Receipts: []string{"first", "second"}})
	if err != nil || len(items) != 1 || items[0].Event.ID != "evt_order_1" {
		t.Fatalf("ordered pull=%+v error=%v", items, err)
	}
}

func TestQueueV2PriorityAgingPreventsStarvation(t *testing.T) {
	client := integrationPostgresClient(t)
	resetControlTables(t, client)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	createEventTestApp(t, client, now.Add(-time.Hour), domain.App{ID: "queue_source", Name: "queue_source", DeliveryMode: domain.DeliveryWebSocket, Enabled: true})
	createEventTestApp(t, client, now.Add(-time.Hour), domain.App{ID: "queue_fair", Name: "queue_fair", DeliveryMode: domain.DeliveryQueue, Enabled: true})
	subscription := queueTestSubscription(now.Add(-time.Hour), "sub_fair", "queue_fair")
	if err := client.CreateQueueSubscription(ctx, subscription); err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		id       string
		priority int
		at       time.Time
	}{{"evt_low_old", -10, now.Add(-21 * time.Minute)}, {"evt_high_new", 10, now}} {
		publication := eventPublication(item.at, item.id, []string{"queue_fair"})
		publication.Event.SourceAppID = "queue_source"
		publication.Event.Queue = &domain.QueueEvent{AvailableAt: item.at, Priority: item.priority, Metadata: []byte(`{}`)}
		publication.Jobs[0].SourceAppID = "queue_source"
		if _, _, err := client.PublishEvent(ctx, publication, item.id, store.EventRetention{Event: 2 * time.Hour, Idempotency: time.Hour}); err != nil {
			t.Fatal(err)
		}
	}
	items, err := client.PullQueueDeliveries(ctx, store.QueuePullRequest{AppID: "queue_fair", SubscriptionID: subscription.ID, Limit: 1, Now: now, Receipts: []string{"fair-receipt"}})
	if err != nil || len(items) != 1 || items[0].Event.ID != "evt_low_old" {
		t.Fatalf("aged pull=%#v error=%v", items, err)
	}
}

func TestQueueV2DrainStopsNewLeasesAndObservesInflightCompletion(t *testing.T) {
	client := integrationPostgresClient(t)
	resetControlTables(t, client)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	createEventTestApp(t, client, now, domain.App{ID: "queue_source", Name: "queue_source", DeliveryMode: domain.DeliveryWebSocket, Enabled: true})
	createEventTestApp(t, client, now, domain.App{ID: "queue_drain", Name: "queue_drain", DeliveryMode: domain.DeliveryQueue, Enabled: true})
	subscription := queueTestSubscription(now, "sub_drain", "queue_drain")
	if err := client.CreateQueueSubscription(ctx, subscription); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"evt_drain_one", "evt_drain_two"} {
		publication := eventPublication(now, id, []string{"queue_drain"})
		publication.Event.SourceAppID = "queue_source"
		publication.Jobs[0].SourceAppID = "queue_source"
		if _, _, err := client.PublishEvent(ctx, publication, id, store.EventRetention{Event: time.Hour, Idempotency: time.Hour}); err != nil {
			t.Fatal(err)
		}
	}
	leased, err := client.PullQueueDeliveries(ctx, store.QueuePullRequest{AppID: "queue_drain", SubscriptionID: subscription.ID, Limit: 1, Now: now, Receipts: []string{"drain-receipt"}})
	if err != nil || len(leased) != 1 {
		t.Fatalf("leased=%#v error=%v", leased, err)
	}
	drain, err := client.BeginQueueDrain(ctx, "queue_drain", subscription.ID, now, now.Add(time.Minute))
	if err != nil || drain.Status != "draining" || drain.InFlight != 1 {
		t.Fatalf("drain=%#v error=%v", drain, err)
	}
	blocked, err := client.PullQueueDeliveries(ctx, store.QueuePullRequest{AppID: "queue_drain", SubscriptionID: subscription.ID, Limit: 1, Now: now.Add(time.Second), Receipts: []string{"blocked"}})
	if err != nil || len(blocked) != 0 {
		t.Fatalf("blocked=%#v error=%v", blocked, err)
	}
	if _, err := client.SettleQueueDeliveries(ctx, "queue_drain", subscription.ID, []store.QueueSettlement{{Receipt: "drain-receipt", Disposition: store.QueueAcknowledge}}, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	drain, err = client.GetQueueDrain(ctx, "queue_drain", subscription.ID, now.Add(3*time.Second))
	if err != nil || drain.Status != "drained" || drain.InFlight != 0 || drain.CompletedAt == nil {
		t.Fatalf("completed drain=%#v error=%v", drain, err)
	}
}

func TestQueueV2ScheduleClaimIsSingleReplicaAndCrashReclaimable(t *testing.T) {
	client := integrationPostgresClient(t)
	resetControlTables(t, client)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	createEventTestApp(t, client, now, domain.App{ID: "queue_schedule", Name: "queue_schedule", DeliveryMode: domain.DeliveryQueue, Enabled: true})
	subscription := queueTestSubscription(now, "sub_schedule", "queue_schedule")
	if err := client.CreateQueueSubscription(ctx, subscription); err != nil {
		t.Fatal(err)
	}
	schedule := domain.QueueSchedule{ID: "qsch_single", AppID: "queue_schedule", SubscriptionID: subscription.ID, Name: "every-five", Enabled: true, CronExpression: "*/5 * * * *", Timezone: "UTC", EventType: "scheduled.tick", Data: []byte(`{"tick":true}`), Metadata: []byte(`{}`), NextRunAt: now, PolicyVersion: 1, CreatedAt: now, UpdatedAt: now}
	if err := client.CreateQueueSchedule(ctx, schedule); err != nil {
		t.Fatal(err)
	}
	type claimResult struct {
		items []domain.QueueSchedule
		err   error
	}
	results := make(chan claimResult, 2)
	start := make(chan struct{})
	for _, token := range []string{"replica-a", "replica-b"} {
		go func(token string) {
			<-start
			items, err := client.ClaimDueQueueSchedules(ctx, now, now.Add(15*time.Second), token, 10)
			results <- claimResult{items: items, err: err}
		}(token)
	}
	close(start)
	var claimed domain.QueueSchedule
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		if len(result.items) == 1 {
			claimed = result.items[0]
		} else if len(result.items) != 0 {
			t.Fatalf("claims=%d", len(result.items))
		}
	}
	if claimed.ID == "" {
		t.Fatal("no replica claimed due schedule")
	}
	eventID, deliveryID := "evt_sched_single", "qdl_sched_single"
	if err := client.CompleteQueueSchedule(ctx, store.QueueScheduleCompletion{ScheduleID: claimed.ID, ClaimToken: claimed.ClaimToken, ClaimGeneration: claimed.ClaimGeneration, EventID: eventID, DeliveryID: deliveryID, OccurrenceAt: now, NextRunAt: now.Add(5 * time.Minute), Now: now}); err != nil {
		t.Fatal(err)
	}
	var occurrences, deliveries int
	if err := client.pool.QueryRow(ctx, `SELECT count(*) FROM queue_schedule_occurrences WHERE schedule_id=$1`, schedule.ID).Scan(&occurrences); err != nil {
		t.Fatal(err)
	}
	if err := client.pool.QueryRow(ctx, `SELECT count(*) FROM queue_deliveries WHERE id=$1`, deliveryID).Scan(&deliveries); err != nil || occurrences != 1 || deliveries != 1 {
		t.Fatalf("occurrences=%d deliveries=%d error=%v", occurrences, deliveries, err)
	}
	if _, err := client.pool.Exec(ctx, `UPDATE queue_schedules SET next_run_at=$2,claim_token=NULL,claim_expires_at=NULL WHERE id=$1`, schedule.ID, now); err != nil {
		t.Fatal(err)
	}
	first, err := client.ClaimDueQueueSchedules(ctx, now, now.Add(time.Second), "crashed", 1)
	if err != nil || len(first) != 1 {
		t.Fatalf("first reclaim setup=%#v error=%v", first, err)
	}
	second, err := client.ClaimDueQueueSchedules(ctx, now.Add(2*time.Second), now.Add(3*time.Second), "replacement", 1)
	if err != nil || len(second) != 1 || second[0].ClaimGeneration <= first[0].ClaimGeneration {
		t.Fatalf("reclaimed=%#v error=%v", second, err)
	}
}

func TestQueueV2ResultCallbackIsGenerationFencedSignedWork(t *testing.T) {
	client := integrationPostgresClient(t)
	resetControlTables(t, client)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	createEventTestApp(t, client, now, domain.App{ID: "queue_source", Name: "queue_source", DeliveryMode: domain.DeliveryWebSocket, Enabled: true})
	createEventTestApp(t, client, now, domain.App{ID: "queue_callback", Name: "queue_callback", DeliveryMode: domain.DeliveryQueue, Enabled: true})
	subscription := queueTestSubscription(now, "sub_callback", "queue_callback")
	callbackURL := "https://callbacks.example/queue-result"
	subscription.SuccessCallbackURL = &callbackURL
	subscription.FailureCallbackURL = &callbackURL
	subscription.ResultCallbackMetadata = []byte(`{"integration":"orders"}`)
	if err := client.CreateQueueSubscription(ctx, subscription); err != nil {
		t.Fatal(err)
	}
	publication := eventPublication(now, "evt_queue_callback", []string{"queue_callback"})
	publication.Event.SourceAppID = "queue_source"
	publication.Jobs[0].SourceAppID = "queue_source"
	if _, _, err := client.PublishEvent(ctx, publication, "callback-key", store.EventRetention{Event: time.Hour, Idempotency: time.Hour}); err != nil {
		t.Fatal(err)
	}
	items, err := client.PullQueueDeliveries(ctx, store.QueuePullRequest{AppID: "queue_callback", SubscriptionID: subscription.ID, Limit: 1, Now: now, Receipts: []string{"callback-receipt"}})
	if err != nil || len(items) != 1 {
		t.Fatalf("items=%#v error=%v", items, err)
	}
	if _, err := client.SettleQueueDeliveries(ctx, "queue_callback", subscription.ID, []store.QueueSettlement{{Receipt: "callback-receipt", Disposition: store.QueueAcknowledge}}, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	claim, err := client.ClaimQueueResultCallback(ctx, now.Add(time.Second), now.Add(20*time.Second), "worker-one")
	if err != nil || claim.Generation != 1 || claim.Outcome != "success" || claim.URL != callbackURL || !bytes.Equal(claim.Secret, []byte("secret")) || bytes.Contains(claim.Body, []byte("callback-receipt")) {
		t.Fatalf("claim=%#v body=%s error=%v", claim, claim.Body, err)
	}
	var callbackBody struct {
		Metadata map[string]string `json:"metadata"`
	}
	if json.Unmarshal(claim.Body, &callbackBody) != nil || callbackBody.Metadata["integration"] != "orders" {
		t.Fatalf("missing safe metadata: %s", claim.Body)
	}
	transition := store.QueueResultCallbackTransition{CallbackID: claim.ID, Attempt: claim.Attempt, ClaimToken: claim.ClaimToken, ClaimGeneration: claim.ClaimGeneration, Status: "delivered", Reason: "http_success", Now: now.Add(2 * time.Second)}
	if err := client.FinishQueueResultCallback(ctx, transition); err != nil {
		t.Fatal(err)
	}
	if err := client.FinishQueueResultCallback(ctx, transition); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale callback finish error=%v", err)
	}
}

func TestQueueV2AdvancedPublishPolicySchedulesAndDeduplicates(t *testing.T) {
	client := integrationPostgresClient(t)
	resetControlTables(t, client)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	createEventTestApp(t, client, now, domain.App{ID: "queue_source", Name: "queue_source", DeliveryMode: domain.DeliveryWebSocket, Enabled: true})
	createEventTestApp(t, client, now, domain.App{ID: "queue_advanced", Name: "queue_advanced", DeliveryMode: domain.DeliveryQueue, Enabled: true})
	subscription := queueTestSubscription(now, "sub_advanced", "queue_advanced")
	subscription.DeduplicationSeconds = 60
	if err := client.CreateQueueSubscription(ctx, subscription); err != nil {
		t.Fatal(err)
	}
	for index, eventID := range []string{"evt_advanced_1", "evt_advanced_2"} {
		publication := eventPublication(now.Add(time.Duration(index)*time.Second), eventID, []string{"queue_advanced"})
		publication.Event.SourceAppID = "queue_source"
		publication.Event.Queue = &domain.QueueEvent{AvailableAt: now.Add(30 * time.Second), OrderingKey: "account:42", Priority: 7, Metadata: []byte(`{"trace":"abc"}`), DedupHash: "same-deduplication-hash"}
		publication.Jobs[0].SourceAppID = "queue_source"
		if _, _, err := client.PublishEvent(ctx, publication, eventID, store.EventRetention{Event: time.Hour, Idempotency: time.Hour}); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := client.pool.QueryRow(ctx, `SELECT count(*) FROM queue_deliveries WHERE subscription_id=$1`, subscription.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("deduplicated count=%d error=%v", count, err)
	}
	early, err := client.PullQueueDeliveries(ctx, store.QueuePullRequest{AppID: "queue_advanced", SubscriptionID: subscription.ID, Limit: 1, Now: now.Add(29 * time.Second), Receipts: []string{"early"}})
	if err != nil || len(early) != 0 {
		t.Fatalf("early pull=%+v error=%v", early, err)
	}
	due, err := client.PullQueueDeliveries(ctx, store.QueuePullRequest{AppID: "queue_advanced", SubscriptionID: subscription.ID, Limit: 1, Now: now.Add(30 * time.Second), Receipts: []string{"due"}})
	if err != nil || len(due) != 1 || due[0].OrderingKey != "account:42" || due[0].Priority != 7 || string(due[0].Metadata) != `{"trace": "abc"}` {
		t.Fatalf("scheduled pull=%+v error=%v", due, err)
	}
}

func queueTestSubscription(now time.Time, id, appID string) domain.QueueSubscription {
	return domain.QueueSubscription{ID: id, AppID: appID, Name: id, Enabled: true, MaxAttempts: 10, DefaultVisibilitySeconds: 60, MaxVisibilitySeconds: 300, MaxTotalLeaseSeconds: 3600, RetentionSeconds: 604800, MaxInFlight: 100, MaxBatchSize: 20, RetryDelaySeconds: 5, OrderingMode: domain.QueueOrderingNone, PolicyVersion: 1, CreatedAt: now, UpdatedAt: now}
}
