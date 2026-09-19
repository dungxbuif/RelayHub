# Durable events and queue reliability

## Publish safely

An accepted event has one durable job per target. The producer's `Idempotency-Key` and app ID identify one publication for 24 hours by default. If a request times out after committing, retry with the same key. The response includes the original event and original job snapshots with `Idempotent-Replayed: true`; it never creates duplicate jobs. A new key represents new work. Publication validates every target and commits event, jobs, indexes, idempotency record and stream entries together.

Event `data` must be a JSON object encoded as valid UTF-8. Invalid bytes return
`400 invalid_request` before idempotency lookup, storage or notification, including
requests using an existing key. Valid Unicode, large integers and empty objects
retain their values. The complete HTTP request remains limited to 1 MiB.

## Durable consumption

The v1 release uses PostgreSQL plus private NATS JetStream for durable delivery.
Applications consume durable work through signed callbacks or the standard
`/api/v1/stream` WebSocket protocol. The earlier Redis HTTP polling queue is not
part of the public release contract. Consumers should deduplicate by event ID in
their own durable store, commit side effects before reporting success, and treat
redelivery as possible after network failures or process crashes.

## Job transitions

| Current status | Allowed next statuses |
| --- | --- |
| `pending` | `leased`, `acked`, `dead_letter` |
| `leased` | `pending`, `delivered`, `acked`, `dead_letter` |
| `delivered` | `acked`, `dead_letter` |
| `acked` | `acked` (idempotent) |
| `dead_letter` | `pending`, `dead_letter` (idempotent) |

Callback `2xx` sets `delivered`; stream acknowledgements and callback outcomes are persisted through PostgreSQL/JetStream delivery state. Callback work uses the bounded retry lifecycle below. Standard [WebSocket notifications](./websocket.md) are available as best-effort hints alongside durable delivery.

## Retention and persistence

| Record | Default | Configuration |
| --- | --- | --- |
| Event | Seven days from publication | `RELAYHUB_EVENT_RETENTION` |
| Terminal job (`delivered`, `acked`, `dead_letter`) | Seven days from terminal transition | `RELAYHUB_JOB_RETENTION` |
| Producer-scoped idempotency result | 24 hours from publication | `RELAYHUB_IDEMPOTENCY_RETENTION` |

Configuration values are positive Go duration strings, such as `168h` or `24h`. Replayed publication and repeated terminal requests do not extend TTLs. Pending delivery rows remain durable until terminal handling or retention cleanup. The idempotency result retains its original response independently of the event TTL; configure a shorter event TTL only if that behavior is acceptable.

PostgreSQL owns authoritative persistence. Publication, routing-rule resolution, event rows, job rows and idempotency rows commit in one transaction, so concurrent duplicate publishes have one winner and target disable cannot interleave with acceptance. NATS JetStream is a private broker used for callback and realtime fanout signals; it is not a public Kafka/MQ protocol. Event and job JSON preserve arbitrary object data, including large integers and empty objects.

Durability depends on PostgreSQL storage, JetStream persistence for in-flight broker work, and backups for both volumes. API acceptance means the PostgreSQL transaction committed; it does not claim a disk fsync beyond the configured database/storage policy.

## Retry HTTP requests

Retry publication with the same key after network errors, timeouts or `5xx`; re-sign each request using the new timestamp while preserving body bytes. Retry acknowledgement safely after an ambiguous result. Correct invalid payloads or signatures before retrying `400`/`401`. `404` means missing/expired or unauthorized-to-read work. Inspect the current job after `409` before changing state again. Callback HTTP retry classification is described below.

See [API schemas and the runnable signing example](./api-overview.md).

## Verification and implementation record

The current v1 reliability suite covers domain/service validation, ownership, transitions, clock-driven lease recovery, HTTP route/auth/body/idempotency behavior, PostgreSQL persistence, private NATS delivery, stream acknowledgement, callback retry state and docs parity. Public documentation is built as standalone Docusaurus output; only Admin assets are regenerated into the backend snapshot. The shipped decision is to enforce service policy with PostgreSQL transactions, private NATS delivery signals and the existing admin bearer/application signing split.

## Task 5 implementation decision (before implementation)

Callback-eligible delivery state is written at acceptance and mirrored to the private `RH_CALLBACKS` JetStream work queue. A token-fenced PostgreSQL lease reserves each callback before HTTP delivery; the attempt deadline is shorter than the lease, and PostgreSQL persists outcomes and retry indexes before broker acknowledgement. WebSocket notifications remain best-effort hints. Deliver raw persisted event JSON with Task 2 HMAC signing, reject redirects, bound response reads, classify only safe categories, and publish job notifications after persistence. Default concurrency is eight, callback timeout ten seconds, reclaim idle thirty seconds. Shutdown stops claims and grants active work the configured grace period.

Tests establish classifier and HTTP contracts, deterministic worker behavior, PostgreSQL claim/reclaim/atomic retry isolation, and NATS callback/stream signal handling. Full tests, race, vet, generated documentation parity, Docker build and runtime smoke checks verify the final implementation. Internal reliability/deployment docs and public Markdown/deployment artifacts are affected; regenerating `web/embed.go` updates the human and agent-readable embedded surface.

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

There is one initial attempt plus five retries: `max_retries=5`, `max_attempts=6`. This resolves the earlier ambiguous phrase “five failed attempts”; every delay below is reachable.

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

Dead-letter jobs are inspectable through the existing job API while retained. The v1 public API does not expose requeue/dead-letter mutation endpoints; after correcting the receiver, publish a new intentional event with a new idempotency key or use future operator tooling when it is added. Expired events cannot be replayed. `callback_attempts` increments atomically at the dispatch boundary after checking the token and deadline, and alone controls callback delays and the six-attempt limit. As with any network operation, a crash after reserving a dispatch but before receiving/persisting its result leaves an ambiguous attempt; receivers must still deduplicate.

## Worker persistence and recovery

The v1 callback stream is the private JetStream `RH_CALLBACKS` work queue. The worker uses explicit acknowledgement and its own bounded pool for `RELAYHUB_WORKER_CONCURRENCY`; broker pending limits provide flow control but do not define HTTP concurrency. PostgreSQL atomically reserves each HTTP attempt with a worker token and absolute lease. Immediately before dispatch it re-reads the enabled state, delivery mode, callback URL and current encrypted credential version. An active lease delays a second worker with delayed NACK; after lease expiry a replacement gets a new attempt and the prior token can no longer persist an outcome. The HTTP deadline ends one second before the lease so the worker retains time to commit the result. RelayHub normalizes retry and lease timestamps to PostgreSQL's microsecond precision so an ambiguous completion can be retried idempotently.

A successful, retry, or dead-letter transition and its attempt record are committed before broker action. Success is ACKed; a transient failure receives delayed NACK using the persisted schedule. Permanent or exhausted failure is published to `RH_DLQ` with a deterministic message ID, recorded in PostgreSQL, then ACKed. A crash before persistence leaves the message for redelivery. A crash after a successful transition but before ACK is recognized as complete and does not make another HTTP request. A crash around the external request can still be ambiguous, so receivers must deduplicate by event ID.

Shutdown first stops admission, explicitly NACKs work that has not entered the callback pool, drains the subscription while its receive context remains active, and waits for admitted calls within `RELAYHUB_SHUTDOWN_TIMEOUT`. Only expiry of that timeout cancels active work; unfinished messages remain recoverable in JetStream. Malformed envelopes and stale missing delivery IDs are terminal poison and are ACKed, while temporary PostgreSQL failures receive an explicit delayed NACK. Persisted permanent failures still follow the DLQ-before-ACK path. Worker outcome counters use bounded status/category labels. Logs include only application, event, job, attempt and outcome identifiers; event bodies, callback URLs and credentials are excluded.

The `store_error` outcome also counts unexpected load, dispatch-start, persistence
and stream acknowledgement errors. Missing or stale claims and intentional
shutdown cancellation are excluded. Error details never become metric labels or
callback log fields.

Task 5 validation covers classifier delays/statuses/Retry-After, byte-exact signed requests, redirect/header isolation, bounded drains and timeouts, fake-store worker concurrency/cancellation and notification order, plus real PostgreSQL/NATS competing workers, crash reclaim, stale generations, retry promotion and namespace isolation. The runnable Compose stack uses the same image for API and worker.

## Task 5 review fix round 1 decision (before implementation)

The current regression suite verifies callback retry retention, stale dispatch fencing, blocked-head reclaim handling and the separation between durable stream leases and callback retry budgets. PostgreSQL/NATS integration tests cover routing-rule persistence, NATS realtime fanout and full-stack routed publish through `/api/v1/stream`.
