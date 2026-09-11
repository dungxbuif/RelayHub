# RelayHub MVP Design

**Status:** Approved from the product decisions recorded in this project conversation.

## Goal

RelayHub is a self-hosted integration provider for applications in the homelab. An application integrates once with RelayHub and can then receive durable events, consume a managed queue, subscribe through a standard WebSocket client, and expose request/response functions without operating its own WebSocket or message-broker server.

The public origin is `https://relayhub.dungxbuif.com`. The external LAN proxy only routes this origin to the RelayHub API container. RelayHub serves its API, WebSocket endpoint, UI/docs, and machine-readable integration resources itself.

## MVP boundary

The MVP must ship these usable vertical flows:

1. An administrator creates and manages applications. A new application receives an API key and HMAC secret exactly once.
2. An authenticated producer publishes an event with an idempotency key. RelayHub persists the event and creates one job per target application.
3. A target application can receive that event through standard WebSocket pub/sub, pull it from its durable queue, or have a worker deliver it to an HTTP callback.
4. Queue consumers acknowledge jobs. Failed callback deliveries retry with bounded exponential delays and end in a dead-letter state.
5. A target application can register a named remote function, listen through WebSocket, and return a response to a synchronous HTTP invocation.
6. Users and developers can open `/docs`; agents can fetch Markdown, OpenAPI, JSON Schema, `llms.txt`, `llms-full.txt`, and downloadable skill resources.

The MVP does not provide a Kafka-compatible wire protocol, Socket.IO protocol, arbitrary code execution, a multi-node control plane, billing, OAuth login, a visual workflow editor, or a public SaaS tenancy model. The queue API is a RelayHub contract backed by Redis, so Kafka can be added later without changing client integrations.

## Technology

- Go 1.24+ for the API and worker binaries.
- `net/http` with `github.com/go-chi/chi/v5` for HTTP routing.
- `github.com/gorilla/websocket` for RFC 6455 server connections. Browser `WebSocket`, `ws`, OkHttp, Gorilla, and other standards-compliant clients can connect. Socket.IO clients cannot connect because Socket.IO is a different application protocol.
- Redis 7 for application metadata, event/job state, idempotency keys, Streams, sorted retry schedules, and Pub/Sub.
- `github.com/redis/go-redis/v9` as the Redis client.
- HMAC-SHA256 request signing and short-lived HMAC-signed WebSocket tokens. No external identity provider is required for the homelab MVP.
- Static HTML/CSS/JavaScript and Markdown for `/docs`; the Go binary embeds and serves the built files.
- Docker multi-stage builds. The deployed stack has exactly three services: `relayhub-api`, `relayhub-worker`, and `relayhub-redis`.

## HTTP and authentication contract

All JSON endpoints use `application/json`. Errors use:

```json
{
  "error": {
    "code": "machine_readable_code",
    "message": "Human-readable explanation"
  }
}
```

Administrative routes require `Authorization: Bearer <RELAYHUB_ADMIN_TOKEN>`.

Application routes require these headers:

- `X-RelayHub-Api-Key`
- `X-RelayHub-Timestamp`, Unix seconds within 300 seconds of server time
- `X-RelayHub-Signature`, lowercase hex HMAC-SHA256 of `timestamp + "\n" + method + "\n" + request-target + "\n" + SHA256(body)` using the application's secret

Requests with an invalid signature, expired timestamp, disabled application, or unknown API key return `401`. The body limit is 1 MiB. Producers send `Idempotency-Key` when publishing events or invoking functions.

## Resource model

### Application

```json
{
  "id": "app_...",
  "name": "orders-web",
  "callback_url": "https://orders.internal/events",
  "delivery_mode": "queue",
  "enabled": true,
  "created_at": "2026-09-11T10:00:00Z"
}
```

`delivery_mode` is `queue`, `websocket`, `callback`, or `all`. Secrets are returned only from `POST /api/v1/apps` and key rotation.

### Event envelope

```json
{
  "id": "evt_...",
  "type": "order.created",
  "source_app_id": "app_...",
  "target_app_ids": ["app_..."],
  "data": {"order_id": "ord_123"},
  "created_at": "2026-09-11T10:00:00Z"
}
```

### Delivery job

A job has an ID, event ID, target app ID, status, attempt count, next-attempt timestamp, last error, and timestamps. Status is `pending`, `leased`, `delivered`, `acked`, or `dead_letter`. Redis Streams carry work; Redis hashes remain the queryable source of state.

### Remote function

A function belongs to an application and has a stable name plus timeout from 1 to 30 seconds. RelayHub never runs user code. An online application receives an `rpc.invoke` WebSocket frame and answers with `rpc.result`. The waiting HTTP request receives that result. Offline or timed-out handlers produce `503 function_unavailable` or `504 function_timeout`.

Function HTTP requests retain the 1 MiB complete body limit. The existing WebSocket bound remains 64 KiB for each complete serialized `rpc.invoke` or `rpc.result` message, including the envelope, input/result/error and JSON escaping. Reject an invocation with `400 invalid_request` before dispatch if its serialized frame would exceed 64 KiB; test both accepted and rejected boundaries. This does not raise the Task 4 socket limit.

## API surface

### Operations

- `GET /healthz`: process liveness; no Redis dependency.
- `GET /readyz`: verifies Redis access.
- `GET /metrics`: Prometheus text metrics for request, event, delivery, retry, dead-letter, and WebSocket counts.

### Applications and credentials

- `POST /api/v1/apps`
- `GET /api/v1/apps`
- `GET /api/v1/apps/{appID}`
- `PATCH /api/v1/apps/{appID}`
- `DELETE /api/v1/apps/{appID}` disables the application.
- `POST /api/v1/apps/{appID}/rotate-secret`
- `POST /api/v1/socket/token` issues a token scoped to the authenticated application, valid for at most 15 minutes.

### Events and queue

- `POST /api/v1/events`
- `GET /api/v1/events/{eventID}`
- `GET /api/v1/queue?limit=20&wait=0`: leases jobs for the authenticated target app; `limit` is 1-100 and `wait` is 0-30 seconds.
- `POST /api/v1/events/{eventID}/ack`
- `GET /api/v1/jobs/{jobID}`
- `POST /api/v1/jobs/{jobID}/requeue`
- `POST /api/v1/jobs/{jobID}/dead-letter`

Publishing returns `202` and the same event on an idempotent replay. Queue leases expire after 60 seconds and can be redelivered. Acknowledgement is idempotent.

### WebSocket

- `GET /ws?token=<short-lived-token>` upgrades to WebSocket.

Client frames:

```json
{"type":"subscribe","topics":["events","jobs","functions"]}
{"type":"ping"}
{"type":"rpc.result","invocation_id":"inv_...","ok":true,"result":{"value":42}}
```

Server frames:

```json
{"type":"ready","app_id":"app_...","connection_id":"conn_..."}
{"type":"subscribed","topics":["events"]}
{"type":"event","event":{}}
{"type":"job.updated","job":{}}
{"type":"rpc.invoke","invocation_id":"inv_...","function":"calculate","input":{},"deadline":"..."}
{"type":"pong"}
{"type":"error","code":"...","message":"..."}
```

Unknown frame types and malformed JSON return an error frame without crashing the connection. Each connection has bounded outbound buffering; slow clients are disconnected.

### Functions

- `POST /api/v1/functions`
- `GET /api/v1/functions`
- `DELETE /api/v1/functions/{functionID}`
- `POST /api/v1/functions/{functionID}/invoke`

Registration is authenticated as the owning application. Invocation may be performed by another authenticated application. RelayHub sends only to a connection of the owner subscribed to `functions`.

## Durable delivery flow

1. The API validates authentication, schema, targets, and idempotency.
2. It stores the event and target jobs atomically with a Redis transaction, then appends jobs to a Redis Stream.
3. It publishes event/job notifications to application-specific Pub/Sub channels for online WebSocket connections.
4. Queue clients lease jobs from their target application's pending index and acknowledge them explicitly.
5. The worker consumes callback jobs. A `2xx` response marks delivered. `408`, `425`, `429`, `5xx`, network errors, and timeouts retry; other `4xx` responses dead-letter immediately.
6. Retry delays are `1s, 5s, 15s, 60s, 300s`. One initial attempt plus five retries means `max_retries=5` and `max_attempts=6`; the sixth failed delivery becomes dead-letter. `Retry-After` is honored up to 300 seconds.

Callback requests include the event envelope and headers `X-RelayHub-Event-Id`, `X-RelayHub-Timestamp`, and `X-RelayHub-Signature`. The target app's secret signs the body so receivers can verify RelayHub.

## Storage and retention

- Event and terminal job state expire after 7 days by default.
- Idempotency records expire after 24 hours.
- Function registrations and application records persist until deleted.
- The Redis deployment enables AOF (`appendonly yes`) so a restart does not intentionally discard queued work.
- Redis keys begin with a configurable prefix, default `relayhub`.

## Deployment

The API and worker use the same source image with different commands. Static docs and the minimal web console are embedded into the API binary. Only the API exposes port `8080`. Redis is internal to the `relayhub` network and has a named data volume. The stack defines health checks and does not set fixed `container_name` values, allowing Compose project scoping.

Cloudflare may provide DNS, TLS, WAF, and CDN in front of the external Traefik entrypoint. `/ws` must allow WebSocket upgrade and should bypass response caching. API and function-invocation routes must not be cached.

## Observability and safety

- Structured JSON logs include request ID, app ID when known, event/job/invocation IDs, latency, and outcome. Secrets, API keys, signatures, and event payloads are never logged.
- Graceful shutdown stops accepting requests, closes WebSocket sessions, and gives the worker time to finish its active delivery.
- Readiness fails when Redis is unavailable; liveness remains healthy while the process can serve.
- CORS is disabled by default and configured through an explicit origin allowlist.
- WebSocket origin checks use the same allowlist. Empty/non-browser origins are allowed because native/server clients commonly omit `Origin`.

## Documentation deliverables

Human docs cover administration, app integration, event publishing, queue consumption, WebSocket clients, function handlers, security, deployment, operations, and troubleshooting. Machine-readable artifacts include:

- `/docs/openapi.json`
- `/docs/schemas/event-envelope.schema.json`
- `/docs/llms.txt`
- `/docs/llms-full.txt`
- `/docs/skills/relayhub-integration/SKILL.md`
- `/docs/skills/relayhub-integration.zip`

The Skills page provides copyable snippets and direct downloads. Examples must use the actual API paths and signature algorithm shipped by the server.

## Verification

Completion requires all of the following evidence:

- Unit tests for validation, signing, token expiry/scopes, state transitions, retry classification, and WebSocket frame parsing.
- Integration tests against real Redis for app creation, idempotent event publish, queue lease/ack, retry/dead-letter, WebSocket delivery, and function request/response.
- HTTP contract tests for status codes and error envelopes.
- Race detector on Go tests.
- Static checks with `go vet` and formatting checks.
- Docker images build, Compose config validates, all three services become healthy, and an end-to-end script exercises app creation, event delivery, queue ack, WebSocket delivery, remote function invocation, and docs routes.
- Documentation link/endpoint checks and validation of OpenAPI and JSON Schema syntax.

