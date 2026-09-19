# RelayHub architecture

The Go binary runs either `api` or `worker`. Both roles are horizontally
scalable and require no sticky load-balancer session. Every process has a unique
instance ID plus a random restart generation; Redis heartbeats and fenced
connection ownership prevent a stale replica from deleting state owned by a
newer process.

RelayHub deliberately separates durable truth from shared ephemeral state:

- PostgreSQL is the durable system of record for applications, credentials,
  routing rules, accepted events, delivery state, idempotency, outbox rows,
  function registrations and RPC outcomes.
- NATS JetStream is durable work transport and wakeup delivery. Core NATS is
  ephemeral cross-replica realtime fan-out and control.
- Redis stores TTL-bound Admin sessions, distributed token buckets, connection
  fences, instance ownership and bounded live state. Redis loss never deletes an
  accepted event; it makes readiness fail and Redis-dependent admission fail
  closed while durable PostgreSQL/NATS recovery remains available.

The canonical local stack therefore has five services: API, worker, PostgreSQL,
NATS and Redis. Redis runs without a volume because it is reconstructible; it is
not Queue v2's durable settlement mechanism. Future Redis Streams use is
reserved for bounded Realtime history only.

Public event envelopes use `id`, `type`, `source_app_id`, `target_app_ids`,
`data`, and `created_at`. There is no tenant model or signature field inside the
envelope. Application HTTP auth is API key plus HMAC; operators use admin bearer;
browser WebSocket uses a short-lived token. Functions execute in owner
applications.

Event object data is validated as UTF-8 before idempotency lookup, durable writes
or notifications. Raw JSON numbers and valid Unicode remain intact. Inbound
WebSocket messages and complete RPC envelopes have a 64 KiB limit. Outbound event
notifications follow the accepted event size (publication HTTP body capped at
1 MiB), plus stored-event and notification envelope overhead.

Partial app PATCH and routing changes are persisted through PostgreSQL
transactions. Atomic disable, credential rotation and monotonic timestamps remain
independent of editable fields. Integration tests use disposable PostgreSQL,
NATS and Redis dependencies and explicitly prove two API and two worker graphs
share state without request affinity.

Source is split into `backend/`, `web/` and `sdks/`. `web/admin` is embedded into
the Go binary at `/admin/`; `web/docs` is a standalone Docusaurus build mounted by
operator routing at `/docs/`. Machine-readable contracts and integration assets
remain under `web/docs/static/`. See [contract maintenance](../developer/contracts.md),
[deployment](../developer/deployment-stack.md) and
[ADR 0003](adr/0003-redis-ephemeral-state.md).
