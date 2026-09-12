# RelayHub architecture

The Go binary runs either `api` or `worker`. PostgreSQL holds application credentials,
routing rules, events, delivery rows, idempotency records, callback/retry state,
function registrations and RPC outcomes. Private NATS JetStream carries durable
stream delivery and private Core NATS subjects carry cross-instance realtime fan-out.
API instances fan out NATS notifications to local standard WebSocket sessions.
Workers send signed callbacks and persist outcomes. Event acceptance is durable
before realtime notifications; PostgreSQL and NATS persistence determine crash
recovery.

Public event envelopes use `id`, `type`, `source_app_id`, `target_app_ids`, `data`,
and `created_at`. There is no tenant model or signature field inside the envelope.
Application HTTP auth is API key plus HMAC; operators use admin bearer; browser
WebSocket uses a short-lived token. Functions execute in owner applications.

Event object data is validated as UTF-8 before idempotency lookup, durable writes
or notifications. Raw JSON numbers and valid Unicode remain intact. Inbound
WebSocket messages and complete RPC envelopes have a 64 KiB limit. Outbound event
notifications follow the accepted event size (publication HTTP body capped at
1 MiB), plus stored-event and notification envelope overhead.

Partial app PATCH and routing changes are persisted through PostgreSQL transactions.
Atomic disable, credential rotation and monotonic timestamps remain independent
of editable fields. Integration tests use isolated PostgreSQL schemas/databases
and NATS subjects so cleanup does not touch unrelated state.

`public-docs/` is canonical public Markdown and the dependency-free HTML console.
The Go build embeds all assets, including OpenAPI 3.1, JSON Schema, llms references
and a deterministic Skill ZIP. `/docs` redirects 308 to `/docs/`; use explicit
`.md` paths, not extensionless user/developer pages. See
[contract maintenance](../developer/contracts.md) and
[deployment](../developer/deployment-stack.md).

The human entrypoint has User, Developer, API Reference and Skills links. Stable
Markdown and machine contracts remain available with JavaScript disabled. A later
generator can replace the human console while preserving these URLs.
