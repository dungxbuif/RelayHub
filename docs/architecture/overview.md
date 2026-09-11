# RelayHub architecture

The Go binary runs either `api` or `worker`. Redis holds application credentials,
events, jobs, queue indexes, callback stream/retries and RPC state. API instances
fan out Redis notifications to local standard WebSocket sessions. Workers send
signed callbacks and persist outcomes before acknowledgement. Event acceptance
is durable before notifications; Redis persistence policy determines crash durability.

Public event envelopes use `id`, `type`, `source_app_id`, `target_app_ids`, `data`,
and `created_at`. There is no tenant model or signature field inside the envelope.
Application HTTP auth is API key plus HMAC; operators use admin bearer; browser
WebSocket uses a short-lived token. Functions execute in owner applications.

Event object data is validated as UTF-8 before idempotency lookup, durable writes
or notifications. Raw JSON numbers and valid Unicode remain intact. Inbound
WebSocket messages and complete RPC envelopes have a 64 KiB limit. Outbound event
notifications follow the accepted event size (publication HTTP body capped at
1 MiB), plus stored-event and notification envelope overhead.

Partial app PATCH uses editable-field compare-and-swap in Redis Lua. On a stale
snapshot the service re-reads, merges and validates again, up to 16 attempts;
exhaustion is a 409 conflict. Atomic disable, credential rotation and monotonic
timestamps remain independent of editable fields. Test Redis clients use unique
namespaces, and cleanup scans/deletes only the owning namespace.

`public-docs/` is canonical public Markdown and the dependency-free HTML console.
The Go build embeds all assets, including OpenAPI 3.1, JSON Schema, llms references
and a deterministic Skill ZIP. `/docs` redirects 308 to `/docs/`; use explicit
`.md` paths, not extensionless user/developer pages. See
[contract maintenance](../developer/contracts.md) and
[deployment](../developer/deployment-stack.md).

The human entrypoint has User, Developer, API Reference and Skills links. Stable
Markdown and machine contracts remain available with JavaScript disabled. A later
generator can replace the human console while preserving these URLs.
