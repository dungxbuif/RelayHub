# RelayHub Queue v2 Plan

**Goal:** Add a public, app-scoped HTTP batch-pull queue with named subscriptions, opaque fenced receipts, explicit settlement, bounded lease extension and advanced delivery policy without exposing NATS.

**Architecture:** PostgreSQL remains durable truth for subscriptions and queue deliveries. Event publication fans out transactionally to every enabled target-app subscription. Pull uses `FOR UPDATE SKIP LOCKED`; random receipt hashes plus delivery generation fence ACK/retry/dead-letter/extension. Redis enforces shared pull admission/rate policy, while long polling uses bounded retry/wakeup and never holds a database transaction open.

## Task 1: Subscription and delivery schema
- Add subscription policy and queue-delivery migrations with strict bounds/indexes.
- Transactionally fan routed events into independent named subscriptions.
- Preserve the existing v1 default stream/queue behavior unchanged.

## Task 2: Pull, settlement and lease extension
- Add bounded pull (`1..100`, wait `0..30s`) with visibility policy and opaque random receipts.
- Batch ACK, retry and explicit dead-letter with per-item results and generation fencing.
- Add bounded lease heartbeat/extension and automatic retry/dead-letter after max attempts.

## Task 3: Public API and metrics
- Add signed app-scoped subscription CRUD, pause/resume, pull, settle and extend routes.
- Expose depth, in-flight, oldest age and outcomes without cross-app disclosure.
- Add OpenAPI, route manifest and integration tests for concurrency/stale receipts.

## Task 4: Advanced policy
- Add ordering-key FIFO, delay/schedule, deduplication window, priority classes, rate/parallelism limits and retention.
- Add explicit batch DLQ replay/delete/export with audit and safe bounds.
- Keep callbacks metadata-only and generation fenced.

## Task 5: SDK workers and release gate
- Add TypeScript and Go pull workers with lease heartbeat, concurrency, graceful drain and explicit retry/dead-letter results.
- Update Docusaurus, schemas and RelayHub Skill; no Python SDK.
- Run backend integration/race, SDK, Admin, docs and packaging CI gates.
