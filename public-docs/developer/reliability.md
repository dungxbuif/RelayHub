# Durable events and queue reliability

## Publish safely

An accepted event has one durable job per target. The producer's `Idempotency-Key` and app ID identify one publication for 24 hours by default. If a request times out after committing, retry with the same key. The response includes the original event and original job snapshots with `Idempotent-Replayed: true`; it never creates duplicate jobs. A new key represents new work. Publication validates every target and commits event, jobs, indexes, idempotency record and stream entries together.

## Queue processing loop

1. Sign `GET /api/v1/queue?limit=20&wait=30` as the consuming target application.
2. For each `{event, job}`, perform your business operation. Use the event ID as a deduplication key in your own durable store.
3. After that operation commits, sign `POST /api/v1/events/{eventID}/ack` with an empty body.
4. Repeat. An empty array means no work became available during the wait.

A lease lasts 60 seconds. Other consumers of the same app cannot receive that job while its lease is active. If a process fails or never acknowledges, polling after expiry leases the job again and increments `attempts`. Delivery is at least once: a crash after business effects commit but before acknowledgement can cause redelivery. Keep processing within the lease interval or make overlapping retry attempts harmless. Lease renewal and lease tokens are not implemented. Acknowledgement is scoped to target plus event and may succeed from a prior consumer after its lease expires; it is deliberately idempotent.

Acknowledgement can move pending, leased or delivered work to acked. Unrelated applications cannot inspect or acknowledge the event/job. A source can read its event and jobs but only an addressed target can acknowledge its own job. Polling is isolated by authenticated app ID and uses bounded 100 ms polling with a maximum 30-second wait; cancellation stops waiting without a detached goroutine.

## Job transitions

| Current status | Allowed next statuses |
| --- | --- |
| `pending` | `leased`, `acked`, `dead_letter` |
| `leased` | `pending`, `delivered`, `acked`, `dead_letter` |
| `delivered` | `acked`, `dead_letter` |
| `acked` | `acked` (idempotent) |
| `dead_letter` | `pending`, `dead_letter` (idempotent) |

Queue lease expiry permits renewed leasing of the same job. Admin bearer controls can dead-letter unfinished work, or requeue leased/dead-letter work. Requeue clears the lease and removes terminal expiry. Acked work cannot be requeued or dead-lettered. Requeue fails if the event has expired. `delivered` is a reserved state for future delivery implementations; polling sets `leased`, and acknowledgement sets `acked`. This release has no callback delivery worker, exponential retry scheduler, automatic retry-attempt limit, or WebSocket transport.

## Retention and persistence

| Record | Default | Configuration |
| --- | --- | --- |
| Event | Seven days from publication | `RELAYHUB_EVENT_RETENTION` |
| Terminal job (`acked`, `dead_letter`) | Seven days from terminal transition | `RELAYHUB_JOB_RETENTION` |
| Producer-scoped idempotency result | 24 hours from publication | `RELAYHUB_IDEMPOTENCY_RETENTION` |

Configuration values are positive Go duration strings, such as `168h` or `24h`. Replayed publication and repeated terminal requests do not extend TTLs. Pending/leased jobs remain durable; if their event expires, the next queue poll removes them from availability and marks them dead-letter with terminal retention. Inactive queues therefore require a later poll to clean up such orphan jobs. The idempotency result retains its original response independently of the event TTL; configure a shorter event TTL only if that behavior is acceptable.

Redis owns persistence, using optimistic WATCH/MULTI transactions with bounded conflict retries. Publication watches the idempotency key, record IDs and target applications, so concurrent duplicate publishes have one winner and target disable cannot interleave with acceptance. Queue leases watch the target availability index, jobs and event records. Terminal transitions atomically update the job and queue index. Event and job JSON preserve arbitrary object data, including large integers and empty objects. Per-target streams store event/job IDs; they are trimmed to the event-retention window on publication and expire after an idle retention period. They are internal notifications, not a public Redis/Kafka protocol.

Redis durability still depends on the operator's Redis persistence and backup configuration. Use the deployment's persistent storage and appropriate Redis persistence policy. API acceptance means the Redis transaction committed; it does not claim a disk fsync or protection from loss of the Redis volume.

## Retry HTTP requests

Retry publication with the same key after network errors, timeouts or `5xx`; re-sign each request using the new timestamp while preserving body bytes. Retry acknowledgement safely after an ambiguous result. Correct invalid payloads or signatures before retrying `400`/`401`. `404` means missing/expired or unauthorized-to-read work. Inspect the current job after `409` before changing state again. There is no automatic HTTP callback retry or rate-limit contract in this task.

See [API schemas and the runnable signing example](./api-overview.md).

## Verification and implementation record

Task 3 added domain/service tests for validation, ownership, transitions, clock-driven lease recovery and cancelled polling; HTTP tests for every route, signature/body handling, bounds, replay headers and JSON errors; and real Redis tests for 16 concurrent duplicate publishers, competing leases, TTLs, ack idempotency, expiry cleanup and stream append. Documentation is mirrored in public Markdown and regenerated into the embedded docs snapshot. The pre-implementation decision was to enforce service policy with atomic Redis persistence and preserve the existing admin bearer/application signing split.
