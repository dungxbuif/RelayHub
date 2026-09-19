---
name: relayhub-integration
description: Integrate RelayHub signed HTTP, routed events, callbacks, standard WebSocket streams/channels, and remote function handlers.
---

# RelayHub integration

Use when building or diagnosing a RelayHub producer, consumer, receiver or function
handler. RelayHub relays data; it does not execute user code. Use the repository TypeScript or Go SDK when available; otherwise use standard HTTP and WebSocket clients.

## Discover before implementation

Fetch [the stable index](https://relayhub.dungxbuif.com/docs/llms.txt) and
[OpenAPI](https://relayhub.dungxbuif.com/docs/openapi.json), or read the bundled
[OpenAPI snapshot](references/openapi.json). Follow the
[authentication reference](references/authentication.md),
[event API](https://relayhub.dungxbuif.com/docs/developer/api-overview.md),
[reliability](https://relayhub.dungxbuif.com/docs/developer/reliability.md),
[WebSocket](https://relayhub.dungxbuif.com/docs/developer/websocket.md), and
[functions](https://relayhub.dungxbuif.com/docs/developer/functions.md).
The bundled reference is a release snapshot. Resolve differences with the deployed
version before changing a client; do not invent routes, SDKs or tenant fields.

## Handle applications and credentials safely

An authorized operator creates apps with admin bearer `POST /api/v1/apps` and
receives `app_id`, `api_key`, `hmac_secret` once. Signed GET/PATCH may affect only
the authenticated app. Admin `DELETE /api/v1/apps/{appID}` disables it; admin
`POST /api/v1/apps/{appID}/rotate-secret` atomically replaces both credentials.
Application signatures cannot authorize admin routes. Rotation/disable does not
revoke already-issued socket tokens or established connections.

Use credentials only from an approved secret manager or process environment.
Never print, commit, paste into generated output, or put credentials in browser
code. Do not write secrets to examples, transcripts, issue reports, logs or tests.
Redact signatures, authorization headers and WebSocket query tokens. Report
status/error codes and opaque resource IDs only. Consult
[security](https://relayhub.dungxbuif.com/docs/security.md).

## Sign exact bytes

Serialize JSON once to UTF-8 bytes. Compute lowercase SHA256 hex of those bytes
(empty bytes for GET requests). Join Unix-seconds timestamp, uppercase method,
exact escaped path plus query, and body hash with LF, without a trailing newline.
HMAC-SHA256 that canonical text using the complete issued secret's UTF-8 bytes;
encode lowercase hex. Send `X-RelayHub-Api-Key`, `X-RelayHub-Timestamp`, and
`X-RelayHub-Signature`. Never decode the secret suffix, reorder the query or
reserialize after signing. Default clock skew is ±300 seconds.

## Publish durably

Signed `POST /api/v1/events` needs a stable `Idempotency-Key` and an object:

Encode event data as valid UTF-8 JSON. Malformed bytes fail before idempotency or
storage. The complete publication body is limited to 1 MiB. Outbound event
notifications may exceed 64 KiB and include envelope overhead; inbound WebSocket
messages and complete RPC envelopes retain their 64 KiB limits.
`{"type":"order.created","target_app_ids":["app_target"],"data":{"order_id":42}}`.
Expect 202 `{event,jobs}`. There are 1–100 unique enabled targets and a 1 MiB body
limit. Retry a lost response with the same key; replay returns the original
publication with `Idempotent-Replayed: true`. Keys last 24h by default; after
expiry reuse creates new work. Use event/job GET for authoritative current state.

Consumers should use callbacks for server-to-server durable delivery, or the
standard `/api/v1/stream` WebSocket protocol for application-owned stream
delivery. HTTP polling queues are not part of the v1 release contract. Realtime channels are online-only and must not be used as the sole path for work that must survive disconnects.

## Callbacks, retries and dead letter

Set callback URL and `delivery_mode: callback` or `all`. Receivers verify target
app HMAC against the exact callback body/target, deduplicate event ID, persist,
then return 2xx. Envelope fields are `id`, `type`, `source_app_id`, `target_app_ids`,
`data`, `created_at`. Consult reliability docs for callback headers and Retry-After.
Network/408/425/429/5xx failures retry after 1s, 5s, 15s, 60s and 300s, then dead letter
(six callback dispatches maximum). Durable stream leases do not consume the callback attempt budget.
After repairing the receiver, use the persisted event/job state for operator
follow-up. Never silently discard failures.

## WebSocket and reconnect

Mint signed `POST /api/v1/socket/token` with
`{"scopes":["ws:connect"],"ttl_seconds":600}`. Use standard RFC 6455 at
`wss://relayhub.dungxbuif.com/ws?token=<URL-encoded-token>`; Socket.IO is incompatible.
On `ready`, send `{"type":"subscribe","topics":["events","jobs"]}`.
Client frames cannot set `app_id`. Event/job frames are hints, not acknowledgements.
Reconnect with backoff/jitter and a fresh token, re-subscribe, then resume durable stream or callback recovery. Browser Origin must match the configured allowlist. Keep HMAC on
the backend; browsers receive only short-lived socket tokens.

## Remote functions

Owner registers signed `POST /api/v1/functions` with
`{"name":"calculate","timeout_seconds":5}` and subscribes its socket to `functions`.
One selected owner connection receives `rpc.invoke`; reply on that connection
before `deadline`, with exact invocation ID and `rpc.result`, `ok:true,result`
or `ok:false,error:{code,message}`. Complete RPC wire frames fit 64 KiB.

Any signed app knowing the function ID can invoke
`POST /api/v1/functions/{functionID}/invoke` with `{"input":{}}` and a stable
`Idempotency-Key` (maximum 256 UTF-8 bytes). HTTP 200 includes `ok`; handler
`ok:false` is still 200. No handler gives 503 `function_unavailable`; no timely
reply gives 504 `function_timeout`. Same caller/key replays that outcome for 24h
without redispatch. There is no offline function queue or automatic retry.

## Diagnose and verify

Parse `{error:{code,message}}` separately from RPC `ok:false`. Fix 400/413 inputs;
fix credentials/clock/exact signing on 401; fix ownership/scope/Origin on 403.
On uncertain network or 5xx publication/invocation responses, back off and reuse
the original idempotency key. A terminal unavailable/timeout replay stays terminal;
a new logical attempt needs a new key and explicit intent.

Exercise disposable apps with synthetic data through create → signed read →
publish → target lease → durable processing → ack. Validate the
[event](https://relayhub.dungxbuif.com/docs/schemas/event-envelope.schema.json),
[client frame](https://relayhub.dungxbuif.com/docs/schemas/client-frame.schema.json),
and [server frame](https://relayhub.dungxbuif.com/docs/schemas/server-frame.schema.json)
contracts. Check `/healthz`, `/readyz`, and
[troubleshooting](https://relayhub.dungxbuif.com/docs/troubleshooting.md).
Record redacted outcomes only.
