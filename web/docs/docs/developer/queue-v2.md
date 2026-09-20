---
title: Queue v2
description: Durable HTTP batch-pull workers, leases, settlement, ordering and DLQ.
---

# Queue v2

Queue v2 is RelayHub's durable HTTP pull service for workers outside the RelayHub cluster. It is app-scoped, PostgreSQL-backed and at-least-once. Use it when a worker needs explicit batch size, backpressure and concurrency control. Use callbacks when RelayHub should push to a stable HTTPS receiver, `/api/v1/stream` for a persistent WebSocket work stream, and Realtime v2 only for online rooms/presence.

The contract follows proven concepts from [Amazon SQS visibility leases](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html), [Google Pub/Sub subscriptions, ordering and retry policy](https://cloud.google.com/pubsub/docs/subscription-properties), and [Cloudflare HTTP pull consumers](https://developers.cloudflare.com/queues/configuration/pull-consumers/). RelayHub keeps a stricter fence: once a receipt expires or a delivery is replayed, the old receipt cannot settle the new generation.

## Delivery contract

- Named subscriptions belong to exactly one authenticated app. Every enabled subscription receives an independent delivery for a matching event.
- Delivery is at-least-once. Make the business operation idempotent using `event.id` or `delivery.id` before ACK.
- Pull returns an opaque receipt. RelayHub stores only its SHA-256 hash and accepts it only for the active, unexpired lease.
- A visibility timeout makes abandoned work available again. Reaching `max_attempts` moves it to that subscription's DLQ.
- ACK, retry and explicit dead-letter are batch operations with one outcome per receipt. Retry can carry a bounded delay.
- Lease extension is bounded by `max_total_lease_seconds`; heartbeats cannot hold work forever.
- Replay resets attempts, increments generation and issues a new receipt. Stale workers cannot ACK replayed work.
- Pausing stops new fan-out and pull. Resuming does not backfill events published while paused or disabled.

Queue v2 does not promise exactly-once execution or global FIFO. `ordering_mode: "key"` serializes non-terminal deliveries sharing an ordering key within one subscription. Different keys continue concurrently.

## Create and publish

All `/api/v2` calls use the normal app HMAC headers. Create a subscription from the consuming app:

```json
POST /api/v2/subscriptions
{
  "name": "orders",
  "event_types": ["order.created"],
  "max_attempts": 10,
  "default_visibility_seconds": 60,
  "max_total_lease_seconds": 3600,
  "max_in_flight": 100,
  "max_batch_size": 20,
  "retry_delay_seconds": 5,
  "ordering_mode": "key",
  "deduplication_seconds": 300,
  "max_dispatch_rate": 200
}
```

The producer can attach queue policy to its ordinary v1 event publication:

```json
{
  "type": "order.created",
  "target_app_ids": ["app_worker"],
  "data": {"order_id": "ord_42"},
  "queue": {
    "delay_seconds": 30,
    "ordering_key": "customer:7",
    "priority": 5,
    "deduplication_key": "order:ord_42:v1",
    "metadata": {"trace": "trace_abc"}
  }
}
```

`available_at` and `delay_seconds` are mutually exclusive and scheduling is bounded to 30 days. Priority is `-10..10`. Higher values start first, but each minute of eligible waiting ages a delivery by one class up to priority 10; once equal, the oldest delivery wins. This gives a priority `-10` delivery a 20-minute starvation bound against a continuous priority-10 stream. Deduplication is per subscription and applies only when that subscription configures a non-zero window.

## Recurring schedules

Create recurring work at `POST /api/v2/subscriptions/{subscriptionID}/schedules`. Schedules use exactly five cron fields, an explicit IANA timezone such as `Asia/Ho_Chi_Minh`, and a minimum one-minute interval. RelayHub computes wall-clock occurrences through timezone/DST changes and stores the next UTC instant.

```json
{
  "name": "daily-report",
  "cron_expression": "0 9 * * *",
  "timezone": "Asia/Ho_Chi_Minh",
  "event_type": "report.daily",
  "data": {"kind": "daily"},
  "priority": 2,
  "metadata": {"integration": "reports"}
}
```

Worker replicas claim due rows with `FOR UPDATE SKIP LOCKED`, expiring claim tokens and monotonically increasing claim generations. Each occurrence derives deterministic event/delivery IDs and has a unique `(schedule_id, scheduled_at)` ledger entry, so a crash or competing replica cannot create the occurrence twice. Invalid local wall times during a DST jump advance to the next valid cron occurrence.

## Result callbacks

A subscription may set HTTPS `success_callback_url` and/or `failure_callback_url`, plus up to 4 KiB of explicitly safe `result_callback_metadata`. Secret-like keys are rejected recursively. Success is queued after ACK; terminal failure is queued after explicit dead-letter or exhausted retries.

Result payloads contain only app, subscription, delivery and event IDs, generation, outcome, attempt count and configured safe metadata. They never contain receipts, event payloads or credentials. RelayHub signs the exact body with the app HMAC secret and uses the standard callback retry classification (six total attempts). Callback claims and completions are persisted and generation-fenced.

## Subscription drain

`POST .../drain` with `{"timeout_seconds":30}` atomically stops new leases while allowing current receipts to settle. Poll `GET .../drain` for `draining`, `drained`, or `timed_out`; retained available work is not deleted. A drain is terminal for that subscription, while worker-local SDK drain remains the graceful process-shutdown mechanism.

## Official TypeScript worker

```ts
import {
  DeadLetterDelivery,
  RelayHubClient,
  RetryDelivery,
} from "@relayhub/sdk/node";

const relayhub = new RelayHubClient({baseUrl, apiKey, hmacSecret});
const worker = relayhub.queue.work("sub_orders", async delivery => {
  try {
    await saveOrderIdempotently(delivery.event.id, delivery.event.data);
  } catch (error) {
    if (isPoisonMessage(error)) throw new DeadLetterDelivery("invalid_order");
    throw new RetryDelivery("dependency_busy", {delayMs: 10_000});
  }
}, {
  concurrency: 16,
  batchSize: 20,
  visibilitySeconds: 60,
  heartbeatSeconds: 20,
});

await shutdownSignal;
await worker.drain({timeoutMs: 30_000});
```

The worker long-polls, caps local concurrency, extends active leases, ACKs only after a successful handler, and drains in-flight handlers during shutdown.

## Official Go worker

```go
worker, err := client.WorkQueue(ctx, "sub_orders", func(ctx context.Context, delivery relayhub.QueueDelivery) relayhub.QueueResult {
    if err := saveOrderIdempotently(ctx, delivery.Event.ID, delivery.Event.Data); err != nil {
        if errors.Is(err, ErrInvalidOrder) {
            return relayhub.QueueDeadLetterResult("invalid_order")
        }
        return relayhub.QueueRetry(10*time.Second, "dependency_busy")
    }
    return relayhub.QueueACK()
}, relayhub.QueueWorkerOptions{
    Concurrency: 16,
    BatchSize: 20,
    Visibility: 60*time.Second,
    Heartbeat: 20*time.Second,
})
if err != nil { return err }

shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
return worker.Drain(shutdownCtx)
```

## Operations and DLQ

Use `GET .../metrics` for available, in-flight, acknowledged, dead-letter and oldest-available state. `GET .../dead-letters/export?format=json|ndjson&limit=100&cursor=...` exports one bounded page and returns the continuation in `X-RelayHub-Next-Cursor`. Replay or permanently delete only an explicit selection of at most 100 delivery IDs; there is no implicit “all matching” mutation. Replay is generation-fenced and deletion is irreversible.

Subscription create/update/pause/resume/drain/delete, schedule changes and DLQ replay/delete append safe records to the shared immutable audit log. Admin Queue v2 shows policy, depth, schedules and recent result callback outcomes without exposing callback URLs, bodies, receipts or secrets.

Prometheus exposes `relayhub_queue_outcomes_total{outcome=...}` without app IDs, subscription names, receipts or payload values as labels. Receipts and HMAC credentials must never be logged.

The complete route and bounds contract is in [OpenAPI](/openapi.json). Queue payload schemas are available as [subscription](/schemas/queue-subscription.schema.json), [schedule](/schemas/queue-schedule.schema.json), and [delivery](/schemas/queue-delivery.schema.json).
