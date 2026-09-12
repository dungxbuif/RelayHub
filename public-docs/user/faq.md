# User FAQ

## Does accepted work survive restarts?

Events, jobs and delivery state are persisted in PostgreSQL before 202. NATS JetStream carries private work signals for callback and stream delivery. Survival depends on PostgreSQL storage, NATS persistence and tested backups. The default event/job retention is seven days. An API response means the database transaction committed; it does not replace storage-level durability and restore drills.

## Is WebSocket enough for reliable delivery?

No. Realtime notifications have no replay. Use signed callbacks or `/api/v1/stream` for durable processing after reconnect, deduplicate by event ID, and acknowledge only after side effects commit. Socket.IO is incompatible; use standard WebSocket clients.

## How do I get an immediate result?

Use [remote functions](../developer/functions.md). A live owner handler is needed;
there is no offline RPC queue. HTTP 200 with `ok:false` is a handler error, while
503 means unavailable and 504 means no timely reply.

## Are tenants or SDKs available?

This release uses application identities and has no tenant API or published
RelayHub SDK. Use standard HTTP/WebSocket libraries and the [Skill](../skills.md).

## How do I recover a failed callback?

Repair the receiver, inspect the job with signed GET, then publish a new intentional event or use future operator tooling when requeue is added. See [retries and dead letter](../developer/reliability.md).
