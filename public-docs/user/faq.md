# User FAQ

## Does accepted work survive restarts?

Events/jobs are persisted in Redis before 202. Survival depends on Redis retention,
persistence and backups. The default event/job retention is seven days. An API
response does not guarantee a disk fsync. Keep the Redis volume and test restores.

## Is WebSocket enough for reliable delivery?

No. Notifications have no replay. Poll the durable queue after reconnect and as a
fallback. Deduplicate processing and acknowledge only after side effects commit.
Socket.IO is incompatible; use standard WebSocket clients.

## How do I get an immediate result?

Use [remote functions](../developer/functions.md). A live owner handler is needed;
there is no offline RPC queue. HTTP 200 with `ok:false` is a handler error, while
503 means unavailable and 504 means no timely reply.

## Are tenants or SDKs available?

This release uses application identities and has no tenant API or published
RelayHub SDK. Use standard HTTP/WebSocket libraries and the [Skill](../skills.md).

## How do I recover a failed callback?

Repair the receiver, inspect the job with signed GET, then have an operator requeue
the dead-letter job. See [retries and dead letter](../developer/reliability.md).
