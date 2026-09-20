---
title: Background jobs
description: Process durable work with subscriptions, leases and acknowledgements.
---

# Background jobs

Queues let workers choose when and how much work to process. Use them for document extraction, report generation, imports and other tasks independent of a user's request.

## How a job moves

1. The receiving app creates an enabled subscription for the event types it handles.
2. A producer publishes an event addressed or routed to that app.
3. A worker pulls work and receives a temporary lease for each delivery.
4. After saving its result, the worker acknowledges the delivery.
5. Failed or expired leases become eligible for retry; exhausted work moves to dead letters.

Create subscriptions before publishing. Paused subscriptions do not backfill events published while paused.

## Run a worker

Create a `RelayHubClient` with the receiving app's credentials as shown in [backend integration](/developer/quick-integrate). Create its subscription once:

```ts
const subscription = await client.queue.create({
  name: "orders",
  event_types: ["order.created"],
  max_attempts: 10,
  default_visibility_seconds: 60,
  max_total_lease_seconds: 3600,
  max_in_flight: 100,
  max_batch_size: 20,
  retry_delay_seconds: 5,
  ordering_mode: "none",
  deduplication_seconds: 0,
  max_dispatch_rate: 200,
});
```

Save the returned ID in worker configuration and reuse it on subsequent starts.

```ts
import {RetryDelivery} from "@relayhub/sdk/node";

const worker = client.queue.work(subscription.id, async delivery => {
  try {
    await saveOrderIdempotently(delivery.event.id, delivery.event.data);
  } catch {
    throw new RetryDelivery("processing_failed", {delayMs: 5000});
  }
}, {
  concurrency: 8,
  batchSize: 10,
  visibilitySeconds: 60,
  heartbeatSeconds: 20,
});

// During your application's shutdown sequence:
await worker.drain({timeoutMs: 30_000});
```

`saveOrderIdempotently` is your application code. The SDK acknowledges after your handler succeeds and extends active leases while work runs.

## Handle duplicates and failures

Delivery is at-least-once. Persist duplicate detection before applying business effects. A lease does not guarantee that a previous worker stopped executing.

Retry temporary failures. Dead-letter invalid input that needs investigation. Inspect failures before replaying work.

## Schedule and order work

Subscriptions support recurring schedules with five-field cron expressions and explicit timezones. Events can carry a delay, priority and ordering key. Key ordering serializes related work within one subscription while unrelated keys proceed.

The [API contract](/openapi.json) defines policy fields, settlement, schedules, metrics and dead-letter operations.
