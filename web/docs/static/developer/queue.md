# Queue workers

Queue workers is the app-scoped, at-least-once HTTP batch-pull contract at
`/api/v2/subscriptions`. It provides named subscriptions, visibility leases,
opaque generation-fenced receipts, ACK/retry/dead-letter settlement, bounded
lease extension, pause/resume, metrics and explicit DLQ replay/delete.

Advanced publication policy is attached to `POST /api/v1/events` under `queue`:
schedule or delay, ordering key, priority, deduplication key and metadata.
Subscriptions control event-type filters, attempt/retention limits, keyed FIFO,
deduplication window, dispatch rate, in-flight and batch limits.

Use the official TypeScript `relayhub.queue.work(...)` or Go `WorkQueue(...)`
worker for long polling, concurrency, lease heartbeat and graceful drain. Persist
business state idempotently before ACK. Queue workers is not exactly-once and does not
provide global FIFO.

See [OpenAPI](../openapi.json), [subscription schema](../schemas/queue-subscription.schema.json)
and [delivery schema](../schemas/queue-delivery.schema.json).
