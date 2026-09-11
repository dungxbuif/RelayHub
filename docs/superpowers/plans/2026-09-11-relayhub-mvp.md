# RelayHub MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a self-hosted RelayHub MVP with authenticated application management, durable events and queues, standard WebSocket delivery, HTTP callback retry/DLQ, remote functions, embedded integration docs, and a verified three-container deployment.

**Architecture:** One Go module builds `relayhub-api` and `relayhub-worker`. Both use a Redis-backed store package; the API owns HTTP/WebSocket/docs, while the worker owns callback delivery and retry scheduling. Client contracts live behind service interfaces so Redis can later be replaced or complemented by Kafka without changing public APIs.

**Tech Stack:** Go 1.24+, chi v5, Gorilla WebSocket, go-redis v9, Redis 7, Prometheus client, Docker Compose, static HTML/Markdown, OpenAPI 3.1, JSON Schema 2020-12.

**Spec:** `docs/superpowers/specs/2026-09-11-relayhub-mvp-design.md`

## Global Constraints

- The public origin is exactly `https://relayhub.dungxbuif.com`; external Traefik routes the whole origin to `relayhub-api`.
- Deployment has exactly three services: `relayhub-api`, `relayhub-worker`, and `relayhub-redis`.
- Redis is the MVP state, queue, retry schedule, and Pub/Sub backend; client APIs must not expose Redis or Kafka semantics.
- WebSocket is RFC 6455 and must work with browser `WebSocket`, Node `ws`, OkHttp, and other standards clients; Socket.IO is explicitly unsupported.
- Administrative HTTP routes use `Authorization: Bearer <RELAYHUB_ADMIN_TOKEN>`.
- Application HTTP routes use API key, Unix timestamp, and HMAC-SHA256 signing as defined by the spec.
- Maximum request body is 1 MiB; signing timestamp skew is 300 seconds; WebSocket token lifetime is at most 15 minutes.
- Queue lease is 60 seconds; pull limits are 1-100; long-poll wait is 0-30 seconds.
- Callback retry delays are exactly `1s, 5s, 15s, 60s, 300s`; one initial attempt plus five retries means `max_retries=5`, `max_attempts=6`, and dead-letter on the sixth failed delivery.
- Event/job retention defaults to 7 days; idempotency retention defaults to 24 hours; Redis AOF is enabled.
- Secrets, API keys, signatures, and event payloads must never appear in logs.
- Every technical task updates internal and public docs; public API changes update OpenAPI/JSON Schema and AI-readable docs.

---

### Task 1: Executable skeleton, configuration, health, metrics, and embedded docs

**Files:**
- Create: `go.mod`
- Create: `cmd/relayhub/main.go`
- Create: `internal/config/config.go`
- Create: `internal/config/config_test.go`
- Create: `internal/httpapi/router.go`
- Create: `internal/httpapi/operations.go`
- Create: `internal/httpapi/operations_test.go`
- Create: `internal/observability/metrics.go`
- Create: `internal/platform/clock.go`
- Create: `internal/store/store.go`
- Create: `internal/store/redisstore/client.go`
- Create: `web/embed.go`
- Create: `Dockerfile`
- Create: `.gitignore`
- Modify: `docs/developer/deployment-stack.md`
- Modify: `public-docs/developer/README.md`

**Interfaces:**
- Produces `config.Load() (config.Config, error)` with validated `HTTPAddr`, `RedisURL`, `AdminToken`, `SigningSecret`, `DocsDir`, `AllowedOrigins`, and retention settings.
- Produces `store.HealthChecker` with `Ping(context.Context) error`.
- Produces `httpapi.NewRouter(httpapi.Dependencies) http.Handler` and `httpapi.Dependencies{Health store.HealthChecker, Docs fs.FS, Metrics http.Handler}`.
- Produces `clock.Clock` with `Now() time.Time` for later deterministic expiry/retry tests.

- [ ] **Step 1: Write configuration tests**

Add table-driven tests that prove missing admin/signing secrets fail, defaults match the spec, invalid durations fail, and a comma-separated origin allowlist is normalized without wildcards.

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `go test ./internal/config -run 'TestLoad' -v`

Expected: compile failure because `config.Load` does not exist.

- [ ] **Step 3: Implement the minimal config and store boundary**

Use environment variables prefixed `RELAYHUB_`; keep defaults in one typed `Config`, parse with the standard library, and return errors naming the invalid variable without printing its value.

- [ ] **Step 4: Write HTTP operation and docs tests**

Test `GET /healthz` returns `200 {"status":"ok"}` without touching Redis, `GET /readyz` returns 200/503 from a real fake `HealthChecker`, `GET /metrics` returns Prometheus text, `/docs` redirects to `/docs/`, `/docs/` serves the embedded index, `/docs/llms.txt` serves plain text, and unknown routes return the standard JSON error.

- [ ] **Step 5: Run operation tests and verify RED**

Run: `go test ./internal/httpapi -run 'Test(Health|Ready|Metrics|Docs|NotFound)' -v`

Expected: compile failure because router and handlers do not exist.

- [ ] **Step 6: Implement the router, operations, metrics, and embed layer**

Create the chi router with recovery, request ID, 1 MiB body guard, JSON errors, health/readiness/metrics routes, and embedded `public-docs`. Keep Redis construction in `cmd/relayhub/main.go`; handle SIGTERM/SIGINT with graceful HTTP shutdown.

- [ ] **Step 7: Verify Task 1**

Run: `gofmt -w cmd internal web && go test ./internal/config ./internal/httpapi -v && go vet ./...`

- [ ] **Step 8: Reconcile docs and commit**

Document all runtime variables, health semantics, `/docs` ownership, and the single-binary commands. Commit: `feat: bootstrap RelayHub API and embedded docs`.

---

### Task 2: Application lifecycle and request authentication

**Files:**
- Create: `internal/domain/app.go`
- Create: `internal/service/apps.go`
- Create: `internal/service/apps_test.go`
- Create: `internal/auth/signature.go`
- Create: `internal/auth/signature_test.go`
- Create: `internal/auth/token.go`
- Create: `internal/auth/token_test.go`
- Create: `internal/httpapi/apps.go`
- Create: `internal/httpapi/apps_test.go`
- Create: `internal/httpapi/auth.go`
- Create: `internal/store/redisstore/apps.go`
- Create: `internal/store/redisstore/apps_integration_test.go`
- Modify: `internal/store/store.go`
- Modify: `internal/httpapi/router.go`
- Modify: `docs/developer/auth.md`
- Modify: `docs/developer/api-overview.md`
- Modify: `public-docs/developer/auth.md`
- Modify: `public-docs/developer/api-overview.md`

**Interfaces:**
- Produces `service.AppService` methods `Create`, `List`, `Get`, `Update`, `Disable`, `RotateSecret`, and `AuthenticateAPIKey`.
- Produces `auth.Sign(secret []byte, timestamp, method, requestTarget string, body []byte) string` and `auth.Verify(...) error`.
- Produces `auth.TokenIssuer.Issue(appID string, scopes []string, ttl time.Duration) (string, error)` and `Verify(token string, requiredScope string) (auth.Claims, error)`.
- Extends `store.Store` with application CRUD and API-key index operations.

- [ ] **Step 1: Write failing signing and token tests**

Use hand-calculated literals to cover canonical signing, changed body/path rejection, 300-second boundary, expired token, missing scope, and the 15-minute maximum TTL.

- [ ] **Step 2: Verify RED**

Run: `go test ./internal/auth -v`

Expected: compile failure because signing/token interfaces do not exist.

- [ ] **Step 3: Implement signing and token primitives**

Use constant-time comparisons, URL-safe base64 for tokens, explicit version/expiry/scopes claims, and injected clock. Do not log credential material.

- [ ] **Step 4: Write failing service and HTTP contract tests**

Cover one-time credential return, secret rotation invalidating the old key, disabled app rejection, admin-only create/list, signed app reads/updates, invalid callback URLs, and standard error envelopes.

- [ ] **Step 5: Verify RED**

Run: `go test ./internal/service ./internal/httpapi -run 'Test(App|Admin|Signed|SocketToken)' -v`

Expected: compile failure or 404 because application handlers do not exist.

- [ ] **Step 6: Implement application service, middleware, and handlers**

Generate opaque IDs with prefixes, generate 32-byte API keys/secrets from `crypto/rand`, hash API keys for lookup, encrypt no values in logs, validate HTTPS callback URLs except explicit `http://` loopback/internal hosts allowed by config, and expose the exact routes from the spec.

- [ ] **Step 7: Add Redis integration tests and implementation**

Run Redis in tests through `testcontainers-go` when Docker is available; otherwise honor `RELAYHUB_TEST_REDIS_URL`. Verify create/get/update/disable/rotate and API-key index atomicity with Redis transactions.

- [ ] **Step 8: Verify Task 2**

Run: `gofmt -w internal && go test ./internal/auth ./internal/service ./internal/httpapi -v && go test -tags=integration ./internal/store/redisstore -v`

- [ ] **Step 9: Reconcile docs and commit**

Replace draft signing examples with exact canonicalization and copyable Go/Node/browser-safe examples. Commit: `feat: add application credentials and request authentication`.

---

### Task 3: Durable event publish, queue lease/ack, idempotency, and job controls

**Files:**
- Create: `internal/domain/event.go`
- Create: `internal/domain/job.go`
- Create: `internal/service/events.go`
- Create: `internal/service/events_test.go`
- Create: `internal/httpapi/events.go`
- Create: `internal/httpapi/events_test.go`
- Create: `internal/store/redisstore/events.go`
- Create: `internal/store/redisstore/events_integration_test.go`
- Modify: `internal/store/store.go`
- Modify: `internal/httpapi/router.go`
- Modify: `docs/developer/api-overview.md`
- Modify: `docs/developer/reliability.md`
- Modify: `public-docs/developer/api-overview.md`
- Modify: `public-docs/developer/reliability.md`

**Interfaces:**
- Produces `service.EventService.Publish(ctx, sourceAppID string, input PublishEvent, idempotencyKey string) (Event, []Job, bool, error)` where the boolean is `replayed`.
- Produces `Lease(ctx, targetAppID string, limit int, wait time.Duration) ([]LeasedEvent, error)` and `Ack(ctx, targetAppID, eventID string) error`.
- Produces `GetEvent`, `GetJob`, `RequeueJob`, and `DeadLetterJob` with ownership checks.
- Extends `store.Store` with atomic publish/idempotency, target pending indexes, leases, state transitions, and stream append.

- [ ] **Step 1: Write failing domain/state tests**

Cover target validation, duplicate target normalization, payload size, legal job transitions, idempotent ack, expired lease redelivery, and forbidden cross-app reads.

- [ ] **Step 2: Verify RED**

Run: `go test ./internal/service -run 'Test(Event|Queue|Job)' -v`

Expected: compile failure because event service does not exist.

- [ ] **Step 3: Implement domain and event service**

Keep state transitions in domain methods. Require a non-empty event type, 1-100 targets, JSON object data, and `Idempotency-Key`. Return `202` with `Idempotent-Replayed: true` on a replay.

- [ ] **Step 4: Write failing HTTP contract tests**

Cover publish, replay, missing signature/key, missing idempotency key, invalid target, event read, queue parameter bounds, lease/ack, job read, requeue, dead-letter, and exact status/error bodies.

- [ ] **Step 5: Verify RED**

Run: `go test ./internal/httpapi -run 'Test(Event|Queue|Job)' -v`

Expected: 404 or handler compilation failures.

- [ ] **Step 6: Implement HTTP handlers and Redis atomic operations**

Use a Lua script or WATCH/MULTI to atomically create event/jobs/idempotency records and append Stream work. Keep per-app pending and lease indexes; return expired leases to pending during lease operations.

- [ ] **Step 7: Add real Redis integration coverage**

Prove concurrent duplicate publishes create one event and one job per target, two consumers cannot lease the same job simultaneously, ack is idempotent, lease expiry redelivers, and retention TTLs are applied.

- [ ] **Step 8: Verify Task 3**

Run: `gofmt -w internal && go test ./internal/service ./internal/httpapi -v && go test -tags=integration ./internal/store/redisstore -run 'Test(Event|Queue)' -v`

- [ ] **Step 9: Reconcile docs and commit**

Document event schema, idempotency, lease lifecycle, ownership, and example producer/consumer loops. Commit: `feat: add durable events and managed queues`.

---

### Task 4: RFC 6455 WebSocket authentication and pub/sub delivery

**Files:**
- Create: `internal/realtime/hub.go`
- Create: `internal/realtime/session.go`
- Create: `internal/realtime/protocol.go`
- Create: `internal/realtime/protocol_test.go`
- Create: `internal/realtime/hub_test.go`
- Create: `internal/httpapi/websocket.go`
- Create: `internal/httpapi/websocket_test.go`
- Create: `internal/store/redisstore/pubsub.go`
- Modify: `internal/service/events.go`
- Modify: `internal/httpapi/router.go`
- Modify: `docs/developer/registration-flow.md`
- Create: `docs/developer/websocket.md`
- Modify: `public-docs/developer/registration-flow.md`
- Create: `public-docs/developer/websocket.md`

**Interfaces:**
- Produces `realtime.Hub.Connect`, `PublishEvent`, `PublishJob`, and `InvokeFunction` using an application-scoped Redis Pub/Sub bridge.
- Produces typed `realtime.ClientFrame` and `realtime.ServerFrame` JSON contracts from the spec.
- Consumes tokens from Task 2 and event/job notifications from Task 3.

- [ ] **Step 1: Write failing protocol tests**

Cover valid subscribe/ping/rpc-result frames, malformed JSON, unknown type, unauthorized topic, oversized frame, and exact server error envelopes.

- [ ] **Step 2: Verify RED**

Run: `go test ./internal/realtime -v`

Expected: compile failure because protocol types do not exist.

- [ ] **Step 3: Implement protocol and bounded session queues**

Use one reader and one writer goroutine per connection, maximum 64 queued outbound frames, 64 KiB frame limit, ping/pong deadlines, and deterministic shutdown. Disconnect slow clients when their queue is full.

- [ ] **Step 4: Write failing WebSocket integration tests**

Use `httptest.Server` plus real Gorilla clients to prove invalid/expired tokens fail before upgrade, valid tokens receive `ready`, subscription gates delivery, browser-style allowed origins work, disallowed origins fail, server clients without Origin work, and an event published through HTTP reaches the correct app only.

- [ ] **Step 5: Verify RED**

Run: `go test ./internal/httpapi -run 'TestWebSocket' -v`

Expected: 404 or failed upgrade because `/ws` is absent.

- [ ] **Step 6: Implement WebSocket handler, hub, and Redis Pub/Sub bridge**

Validate `ws:connect`, enforce topics, publish application-scoped channels, and wire event/job state changes to the hub. Never accept an app ID from a client frame as authority.

- [ ] **Step 7: Verify Task 4**

Run: `gofmt -w internal && go test ./internal/realtime ./internal/httpapi -run 'Test(WebSocket|Protocol|Hub)' -v && go test -race ./internal/realtime ./internal/httpapi`

- [ ] **Step 8: Reconcile docs and commit**

Add browser and Node `ws` examples and explicitly explain Socket.IO incompatibility. Commit: `feat: add standard WebSocket event delivery`.

---

### Task 5: Callback worker, retries, dead-letter, and graceful worker command

**Files:**
- Create: `internal/delivery/classify.go`
- Create: `internal/delivery/classify_test.go`
- Create: `internal/delivery/callback.go`
- Create: `internal/delivery/callback_test.go`
- Create: `internal/worker/worker.go`
- Create: `internal/worker/worker_test.go`
- Create: `internal/store/redisstore/worker.go`
- Modify: `cmd/relayhub/main.go`
- Modify: `internal/store/store.go`
- Modify: `internal/observability/metrics.go`
- Modify: `docs/developer/reliability.md`
- Modify: `docs/developer/deployment-stack.md`
- Modify: `public-docs/developer/reliability.md`

**Interfaces:**
- Produces `delivery.Classify(status int, headers http.Header, err error, attempt int, now time.Time) Outcome` with `Delivered`, `RetryAt`, or `DeadLetter`.
- Produces `delivery.Callback.Deliver(ctx, app domain.App, event domain.Event) Result` with signed callback headers.
- Produces `worker.Run(ctx context.Context) error`, selected by `relayhub worker`; default command remains `relayhub api`.
- Extends the store with consumer-group claim/ack, retry sorted-set schedule, and terminal transition operations.

- [ ] **Step 1: Write failing retry classification tests**

Use literal times to cover 2xx delivered; 408/425/429/5xx/network/timeout retry; other 4xx dead-letter; exact five-delay sequence; capped valid `Retry-After`; invalid `Retry-After`; and sixth total failure dead-letter (five retries).

- [ ] **Step 2: Verify RED**

Run: `go test ./internal/delivery -run 'TestClassify' -v`

Expected: compile failure because classifier does not exist.

- [ ] **Step 3: Implement classification and signed callback client**

Use a bounded HTTP client timeout, no automatic cross-host redirects, 1 MiB response drain limit, target-app secret signing, and safe error strings without payloads or credentials.

- [ ] **Step 4: Write failing worker behavior tests**

Use a deterministic fake store plus `httptest.Server` to prove success, scheduled retry, immediate DLQ, max-attempt DLQ, crash/restart reclaim, concurrency limit, and graceful cancellation.

- [ ] **Step 5: Verify RED**

Run: `go test ./internal/worker -v`

Expected: compile failure because worker does not exist.

- [ ] **Step 6: Implement worker and Redis consumer operations**

Consume Streams with a named group, promote due retries, claim abandoned messages, transition job state before acknowledging work, and publish `job.updated` notifications. Make concurrency configurable with a default of 8.

- [ ] **Step 7: Verify Task 5**

Run: `gofmt -w internal cmd && go test ./internal/delivery ./internal/worker -v && go test -race ./internal/delivery ./internal/worker && go test -tags=integration ./internal/store/redisstore -run 'TestWorker' -v`

- [ ] **Step 8: Reconcile docs and commit**

Document callback signing, retryable codes, delays, DLQ recovery, and worker operations. Commit: `feat: add callback delivery worker and retry lifecycle`.

---

### Task 6: Remote functions over WebSocket

**Files:**
- Create: `internal/domain/function.go`
- Create: `internal/service/functions.go`
- Create: `internal/service/functions_test.go`
- Create: `internal/httpapi/functions.go`
- Create: `internal/httpapi/functions_test.go`
- Create: `internal/store/redisstore/functions.go`
- Create: `internal/store/redisstore/functions_integration_test.go`
- Modify: `internal/realtime/hub.go`
- Modify: `internal/realtime/session.go`
- Modify: `internal/httpapi/router.go`
- Create: `docs/developer/functions.md`
- Create: `public-docs/developer/functions.md`

**Interfaces:**
- Produces `service.FunctionService.Register`, `List`, `Delete`, and `Invoke`.
- `Invoke` creates `inv_...`, subscribes to `relayhub:rpc:result:<invocationID>`, sends `rpc.invoke` only to owner sessions subscribed to `functions`, and waits for result, caller cancellation, or timeout.
- WebSocket `rpc.result` handling validates that the responding session owns the function/invocation before publishing the result.

- [ ] **Step 1: Write failing function service tests**

Cover valid names, unique `(appID,name)`, timeout bounds 1-30 seconds, owner-only mutation, caller auth, unavailable handler, successful result, handler error, mismatched responder, timeout, cancellation, and idempotent invocation replay.

- [ ] **Step 2: Verify RED**

Run: `go test ./internal/service -run 'TestFunction' -v`

Expected: compile failure because function service does not exist.

- [ ] **Step 3: Implement function domain/service/store**

Persist registrations in Redis and invocation idempotency/result state for 24 hours. RelayHub routes JSON input/output only and never executes user code. Function HTTP requests retain the 1 MiB complete body bound; each complete serialized `rpc.invoke` and `rpc.result` frame retains Task 4's 64 KiB bound, including envelope and JSON escaping. Validate the outbound invocation frame before dispatch, returning `400 invalid_request` if it exceeds 64 KiB; test accepted and rejected boundaries without raising the socket limit.

- [ ] **Step 4: Write failing HTTP + WebSocket end-to-end test**

Open a real WebSocket as the function owner, subscribe to `functions`, invoke through signed HTTP as another app, assert the exact `rpc.invoke` frame, send `rpc.result`, and assert the HTTP response. Repeat offline and timeout cases.

- [ ] **Step 5: Verify RED**

Run: `go test ./internal/httpapi -run 'TestFunction' -v`

Expected: 404 because function routes are absent.

- [ ] **Step 6: Wire HTTP handlers and WebSocket result routing**

Return `201` register, `200` successful invocation/handler error envelope, `503 function_unavailable`, and `504 function_timeout`. Enforce the registered timeout even if the caller asks for longer.

- [ ] **Step 7: Verify Task 6**

Run: `gofmt -w internal && go test ./internal/service ./internal/httpapi -run 'TestFunction' -v && go test -race ./internal/realtime ./internal/service ./internal/httpapi && go test -tags=integration ./internal/store/redisstore -run 'TestFunction' -v`

- [ ] **Step 8: Reconcile docs and commit**

Document the function lifecycle and copyable browser/Node/Go handler examples, with offline/timeout semantics. Commit: `feat: add remote functions over WebSocket`.

---

### Task 7: Complete public docs, OpenAPI, schemas, Skills download, and minimal console

**Files:**
- Create: `public-docs/openapi.json`
- Create: `public-docs/schemas/event-envelope.schema.json`
- Create: `public-docs/schemas/client-frame.schema.json`
- Create: `public-docs/schemas/server-frame.schema.json`
- Create: `public-docs/skills/relayhub-integration/SKILL.md`
- Create: `public-docs/skills/relayhub-integration/references/authentication.md`
- Create: `public-docs/skills/relayhub-integration/references/openapi.json`
- Create: `scripts/build-skill.sh`
- Create: `scripts/check-docs.py`
- Create: `scripts/check-contracts.sh`
- Modify: `public-docs/index.html`
- Modify: `public-docs/llms.txt`
- Modify: `public-docs/llms-full.txt`
- Modify: `docs/README.md`
- Modify: `docs/developer/README.md`
- Modify: `docs/developer/api-overview.md`
- Modify: `docs/developer/auth.md`
- Modify: `docs/developer/registration-flow.md`
- Modify: `docs/developer/reliability.md`
- Modify: `docs/developer/deployment-stack.md`
- Modify: `docs/developer/skills.md`
- Modify: `docs/user/getting-started.md`
- Modify: `docs/user/faq.md`
- Modify: `public-docs/README.md`
- Modify: `public-docs/developer/README.md`
- Modify: `public-docs/developer/api-overview.md`
- Modify: `public-docs/developer/auth.md`
- Modify: `public-docs/developer/registration-flow.md`
- Modify: `public-docs/developer/reliability.md`
- Modify: `public-docs/developer/skills.md`
- Modify: `public-docs/user/getting-started.md`
- Modify: `public-docs/user/faq.md`
- Modify: `web/embed.go`

**Interfaces:**
- `/docs/openapi.json` is the canonical public HTTP contract.
- `/docs/schemas/*.json` are canonical frame/event schemas.
- `/docs/skills/relayhub-integration/SKILL.md` and `.zip` are direct-download agent resources.
- The human page has User, Developer, API, and Skills navigation and copy buttons for commands/snippets.

- [ ] **Step 1: Write failing contract/doc checks**

Extend the checker to parse every JSON artifact, validate OpenAPI 3.1 structure, verify every documented internal link and HTML href, require all public API routes from the router manifest in OpenAPI, require the Skills files/zip, and start the real Go API to verify `/docs` content types and redirects.

- [ ] **Step 2: Verify RED**

Run: `python3 scripts/check-docs.py && ./scripts/check-contracts.sh`

Expected: failures for missing OpenAPI, schemas, Skills download, and undocumented runtime routes.

- [ ] **Step 3: Generate canonical contracts and Skills package**

Write OpenAPI and schemas from the implemented behavior. The Skills instructions must teach discovery, signing, events, queues, WebSocket, functions, error handling, and link to stable references. Build the zip reproducibly with sorted files and normalized timestamps.

- [ ] **Step 4: Finish the human docs console**

Keep the UI dependency-free and accessible. Add visible tabs, task-oriented pages, copy buttons, code examples, mobile layout, and links to Markdown/JSON/Skills resources. Do not add implementation-only details to onboarding flows.

- [ ] **Step 5: Rebuild `llms-full.txt` from public Markdown**

Use a deterministic script so `llms-full.txt` cannot drift. Include endpoint catalog, constraints, error semantics, and all integration examples without secrets.

- [ ] **Step 6: Verify Task 7**

Run: `./scripts/build-skill.sh && python3 scripts/check-docs.py && ./scripts/check-contracts.sh && go test ./internal/httpapi -run 'TestDocs' -v`

- [ ] **Step 7: Reconcile docs and commit**

Run the checks after a clean rebuild and commit: `docs: publish complete RelayHub integration reference`.

---

### Task 8: Production Docker stack, CI, and end-to-end acceptance

**Files:**
- Create: `compose.yaml`
- Create: `.env.example`
- Create: `scripts/e2e.sh`
- Create: `scripts/sign-request.go`
- Create: `.github/workflows/ci.yml`
- Modify: `Dockerfile`
- Modify: `public-docs/deploy/docker-compose.relayhub.yml`
- Modify: `public-docs/deploy/README.md`
- Modify: `public-docs/deploy/traefik/labels.yml`
- Create: `docs/operations/runbook.md`
- Create: `README.md`

**Interfaces:**
- `docker compose up --build -d` starts exactly API, worker, and Redis.
- API image command defaults to `api`; worker overrides command with `worker`.
- Only API publishes `${RELAYHUB_PORT:-8080}:8080`; Redis uses named volume `relayhub-data` and internal network `relayhub`.
- `scripts/e2e.sh` is repeatable, fails fast, and cleans only resources it creates.

- [ ] **Step 1: Write the acceptance script before deployment config**

The script must wait for `/readyz`, create producer/consumer apps, calculate real signatures, publish/replay an event, lease/ack it, observe a WebSocket event with a standards client, start a function handler and invoke it, force one retry then a successful callback, verify job state, and fetch docs/OpenAPI/Skills artifacts.

- [ ] **Step 2: Run acceptance and verify RED**

Run: `./scripts/e2e.sh`

Expected: failure because the Compose stack is absent.

- [ ] **Step 3: Implement Docker image and Compose stack**

Use a non-root distroless runtime, embed public docs, add HTTP health checks, enable Redis AOF with a named volume, set dependency health conditions, avoid fixed container names, and include graceful stop periods. Correct the old docs mount path by removing runtime mounts entirely.

- [ ] **Step 4: Add CI gates**

CI runs formatting check, `go vet`, unit tests, race tests, integration tests with Redis service, docs/contracts, Docker build, Compose config validation, and end-to-end acceptance. Cache only Go modules/build data.

- [ ] **Step 5: Run the complete local gate**

Run:

```bash
test -z "$(gofmt -l .)"
go vet ./...
go test ./...
go test -race ./...
go test -tags=integration ./...
./scripts/build-skill.sh
python3 scripts/check-docs.py
./scripts/check-contracts.sh
docker build -t relayhub:test .
docker compose config --quiet
./scripts/e2e.sh
```

Expected: every command exits 0 and the e2e report identifies each required flow as PASS.

- [ ] **Step 6: Inspect runtime state and logs**

Verify `docker compose ps` shows three healthy services, API is the only published port, Redis data survives an API/worker restart, queued event survives a Redis restart, and logs contain IDs/outcomes but no generated credential or payload values.

- [ ] **Step 7: Reconcile docs and commit**

Update the root quick start, environment reference, backup/restore, upgrade, troubleshooting, Cloudflare/Traefik guidance, and acceptance commands. Commit: `build: ship verified three-service RelayHub stack`.

---

### Task 9: Requirement audit and release review

**Files:**
- Create: `docs/reviews/MVP-VERIFICATION.md`
- Modify: any code/docs found incomplete by the audit

**Interfaces:**
- Consumes the design spec, this plan, git diff, test output, live Compose state, and E2E output.
- Produces a requirement-by-requirement evidence table with `PASS`, `FAIL`, or `UNVERIFIED`; release is allowed only when no item is `FAIL` or `UNVERIFIED`.

- [ ] **Step 1: Audit every spec section**

For each resource, route, state transition, security rule, retention rule, documentation artifact, deployment invariant, and verification gate, record the exact file/test/runtime evidence.

- [ ] **Step 2: Fix all gaps through TDD**

For each behavior gap, write a failing regression test, confirm the expected failure, implement the smallest fix, and rerun the focused plus affected suite. Reconcile internal and public docs for every changed contract.

- [ ] **Step 3: Request whole-branch review**

Review the full diff from `7f7c3f2` through `HEAD` for spec compliance, correctness, concurrency, security, operability, and doc accuracy. Resolve all Critical/Important findings and record rulings for any residual disputed item.

- [ ] **Step 4: Run fresh final verification**

Run the complete local gate from Task 8 again after the final review fixes. Capture command, exit code, test count, and key runtime assertions in `docs/reviews/MVP-VERIFICATION.md`.

- [ ] **Step 5: Commit the verified release state**

Commit: `chore: verify RelayHub MVP release candidate`.
