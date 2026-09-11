# Durable events and queue reliability

## Publish safely

An accepted event has one durable job per target. The producer's `Idempotency-Key` and app ID identify one publication for 24 hours by default. If a request times out after committing, retry with the same key. The response includes the original event and original job snapshots with `Idempotent-Replayed: true`; it never creates duplicate jobs. A new key represents new work. Publication validates every target and commits event, jobs, indexes, idempotency record and stream entries together.

In the v1 PostgreSQL/NATS data plane, `202 Accepted` means the event,
producer-scoped replay result and every target sink have committed in one
PostgreSQL transaction. RelayHub then publishes those sink deliveries through a
recoverable outbox. A broker outage can delay delivery but does not revoke the
accepted response. If RelayHub retries an ambiguous publish, it reuses the same
delivery and broker message identities. Broker-side duplicate suppression has a
finite window, so the gateway also fences assignments durably by delivery,
application and connection. A physical duplicate after durable ACK never reaches
the handler. Unacknowledged work may still be redelivered with the same delivery
ID, so consumers must deduplicate their own committed side effects.

RelayHub persists one attempt immediately before each outbox broker call. A
crash or timeout around that call therefore consumes the attempt even when the
broker outcome is unknown. Repeated ambiguous successes are bounded: after the
configured attempt limit, RelayHub dead-letters the retained delivery before
making another broker call and reports unhealthy readiness for operator action.
Claiming a batch alone consumes no attempts, so rows the dispatcher never visits
remain unchanged.

Event `data` must be a JSON object encoded as valid UTF-8. Invalid bytes return
`400 invalid_request` before idempotency lookup, storage or notification, including
requests using an existing key. Valid Unicode, large integers and empty objects
retain their values. The complete HTTP request remains limited to 1 MiB.

## Queue processing loop

1. Sign `GET /api/v1/queue?limit=20&wait=30` as the consuming target application.
2. For each `{event, job}`, perform your business operation. Use the event ID as a deduplication key in your own durable store.
3. After that operation commits, sign `POST /api/v1/events/{eventID}/ack` with an empty body.
4. Repeat. An empty array means no work became available during the wait.

A lease lasts 60 seconds. Other consumers of the same app cannot receive that job while its lease is active. If a process fails or never acknowledges, polling after expiry leases the job again and increments `attempts`. Delivery is at least once: a crash after business effects commit but before acknowledgement can cause redelivery. Keep processing within the lease interval or make overlapping retry attempts harmless. Lease renewal and lease tokens are not implemented. Acknowledgement is scoped to target plus event and may succeed from a prior consumer after its lease expires; it is deliberately idempotent.

Acknowledgement can move pending, leased or delivered work to acked. Unrelated applications cannot inspect or acknowledge the event/job. A source can read its event and jobs but only an addressed target can acknowledge its own job. Polling is isolated by authenticated app ID and uses bounded 100 ms polling with a maximum 30-second wait; Redis I/O honors context deadlines. Each long-poll Redis attempt has a maximum 250 ms deadline (or the remaining wait, if shorter), bounding explicit cancellation during an active socket read. Timed-out attempts retry within the requested wait; cancellation returns without a detached Redis goroutine.

## Job transitions

| Current status | Allowed next statuses |
| --- | --- |
| `pending` | `leased`, `acked`, `dead_letter` |
| `leased` | `pending`, `delivered`, `acked`, `dead_letter` |
| `delivered` | `acked`, `dead_letter` |
| `acked` | `acked` (idempotent) |
| `dead_letter` | `pending`, `dead_letter` (idempotent) |

Queue lease expiry permits renewed leasing of the same job. Admin bearer controls can dead-letter unfinished work, or requeue leased/dead-letter work. Requeue clears the lease and retry schedule, resets both `attempts` and `callback_attempts` to zero, increments the callback generation, and removes terminal expiry. Acked work cannot be requeued or dead-lettered. Requeue fails if the event has expired. Callback `2xx` sets `delivered`; polling sets `leased`, and acknowledgement sets `acked`. Callback work uses the bounded retry lifecycle below. Standard [WebSocket notifications](./websocket.md) are available as best-effort hints alongside the durable queue.

## Retention and persistence

| Record | Default | Configuration |
| --- | --- | --- |
| Event | Seven days from publication | `RELAYHUB_EVENT_RETENTION` |
| Terminal job (`delivered`, `acked`, `dead_letter`) | Seven days from terminal transition | `RELAYHUB_JOB_RETENTION` |
| Producer-scoped idempotency result | 24 hours from publication | `RELAYHUB_IDEMPOTENCY_RETENTION` |

Configuration values are positive Go duration strings, such as `168h` or `24h`. Replayed publication and repeated terminal requests do not extend TTLs. Pending/leased jobs remain durable; if their event expires, the next queue poll removes them from availability and marks them dead-letter with terminal retention. Inactive queues therefore require a later poll to clean up such orphan jobs. The idempotency result retains its original response independently of the event TTL; configure a shorter event TTL only if that behavior is acceptable.

Redis owns persistence, using optimistic WATCH/MULTI transactions with bounded conflict retries. Publication watches the idempotency key, record IDs and target applications, so concurrent duplicate publishes have one winner and target disable cannot interleave with acceptance. Queue leases watch the target availability index, jobs and event records. Terminal transitions atomically update the job and queue index. Event and job JSON preserve arbitrary object data, including large integers and empty objects. Per-target streams store event/job IDs; they are trimmed to the event-retention window on publication and expire after an idle retention period. They are internal notifications, not a public Redis/Kafka protocol.

Redis durability still depends on the operator's Redis persistence and backup configuration. Use the deployment's persistent storage and appropriate Redis persistence policy. API acceptance means the Redis transaction committed; it does not claim a disk fsync or protection from loss of the Redis volume.

## Retry HTTP requests

Retry publication with the same key after network errors, timeouts or `5xx`; re-sign each request using the new timestamp while preserving body bytes. Retry acknowledgement safely after an ambiguous result. Correct invalid payloads or signatures before retrying `400`/`401`. `404` means missing/expired or unauthorized-to-read work. Inspect the current job after `409` before changing state again. Callback HTTP retry classification is described below.

See [API schemas and the runnable signing example](./api-overview.md).

## Receive signed callbacks

Run `relayhub worker` alongside `relayhub api`. A target enters callback work only when its mode is `callback` or `all` and its callback URL is non-empty. Mode and URL are checked atomically at publication and checked again immediately before sending. PostgreSQL stores an independent callback delivery for this sink, so stream acknowledgement does not consume its callback retry budget. A disabled or no-longer-eligible target ends that callback delivery without making an HTTP request. Consumers should deduplicate by event ID across delivery paths.

Each request is `POST` to the configured URL, using the exact persisted event envelope bytes as the body. It includes `Content-Type: application/json`, `X-RelayHub-Event-Id`, `X-RelayHub-Timestamp` (Unix seconds), and `X-RelayHub-Signature` (lowercase hex HMAC-SHA256). No API key is sent. Use the target application's HMAC secret to verify:

```text
canonical = timestamp + "\n" + "POST" + "\n" + request_target + "\n" + hex(SHA256(raw_body))
signature = hex(HMAC_SHA256(target_hmac_secret, canonical))
```

`request_target` includes the escaped path and unchanged query string, for example `/hooks/a%2Fb?kind=order`. Hash the received raw bytes before decoding JSON. Compare signatures in constant time, check timestamp freshness, and persist the event ID for deduplication. See the [signing example](./api-overview.md). A receiver should respond with `2xx` only after committing its work. Callback success is `delivered`, which differs from explicit target `acked`.

## Callback retries and dead-letter

| Result | Durable outcome |
| --- | --- |
| HTTP `200–299` | `delivered` |
| Network/timeout, HTTP `408`, `425`, `429`, `500–599` | Retry, then dead-letter when exhausted |
| Other `4xx` | Immediate `dead_letter` |
| Redirects and other unexpected status codes | Immediate `dead_letter`; redirects are never followed |

There is one initial attempt plus five retries: `max_retries=5`, `max_attempts=6`.

| Failed delivery attempt | Next action |
| --- | --- |
| 1 | Retry after 1 second |
| 2 | Retry after 5 seconds |
| 3 | Retry after 15 seconds |
| 4 | Retry after 60 seconds |
| 5 | Retry after 300 seconds |
| 6 | `dead_letter`, no further automatic delivery |

On `429`, valid non-negative `Retry-After` delta-seconds or a future HTTP-date replaces the normal delay and is capped at 300 seconds. Zero means immediately eligible. Invalid, negative, and past values use the normal schedule. The attempt limit still applies. The durable job exposes `attempts` (total queue/callback claim reservations), `callback_attempts` (callback dispatch reservations only; absent means zero), optional `retry_at`, `last_reason`, `callback`, and `callback_generation`; reasons contain safe categories, never response bodies, secrets, signatures, or event data.

Each attempt defaults to ten seconds (`RELAYHUB_CALLBACK_TIMEOUT`), including reading the response. At most 1 MiB of response body is discarded before closing. Redirects are rejected even on the same host, so signed headers cannot be forwarded across hosts. Worker concurrency defaults to eight (`RELAYHUB_WORKER_CONCURRENCY`, range 1–1024).

Dead-letter jobs are inspectable through the existing job API. After correcting the receiver, an administrator can use `POST /api/v1/jobs/{jobID}/requeue`; this starts a new attempt budget if the retained event still exists. `POST /api/v1/jobs/{jobID}/dead-letter` stops unfinished work. Expired events cannot be requeued. Queue leases and abandoned claims before dispatch do not consume the callback retry budget. `callback_attempts` increments atomically at the dispatch boundary after checking the token and deadline, and alone controls callback delays and the six-attempt limit. As with any network operation, a crash after reserving a dispatch but before receiving/persisting its result leaves an ambiguous attempt; receivers must still deduplicate. Admin requeue resets both attempt counters.

## Worker persistence and recovery

The v1 callback stream is the private JetStream `RH_CALLBACKS` work queue. The worker uses explicit acknowledgement and bounds in-flight work by `RELAYHUB_WORKER_CONCURRENCY`. PostgreSQL atomically reserves each HTTP attempt with a worker token and absolute lease. Immediately before dispatch it re-reads the enabled state, delivery mode, callback URL and current encrypted credential version. An active lease delays a second worker with delayed NACK; after lease expiry a replacement gets a new attempt and the prior token can no longer persist an outcome. The HTTP deadline ends one second before the lease so the worker retains time to commit the result.

A successful, retry, or dead-letter transition and its attempt record are committed before broker action. Success is ACKed; a transient failure receives delayed NACK using the persisted schedule. Permanent or exhausted failure is published to `RH_DLQ` with a deterministic message ID, recorded in PostgreSQL, then ACKed. A crash before persistence leaves the message for redelivery. A crash after a successful transition but before ACK is recognized as complete and does not make another HTTP request. A crash around the external request can still be ambiguous, so receivers must deduplicate by event ID.

Shutdown cancels consumption and drains the subscription within `RELAYHUB_SHUTDOWN_TIMEOUT`; unfinished messages remain recoverable in JetStream. Worker outcome counters use bounded status/category labels. Logs include only application, event, job, attempt and outcome identifiers; event bodies, callback URLs and credentials are excluded.

The `store_error` outcome also counts unexpected load, dispatch-start, persistence
and stream acknowledgement errors. Missing or stale claims and intentional
shutdown cancellation are excluded. Error details never become metric labels or
callback log fields.
