# RelayHub MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Không tự bắt đầu implementation từ yêu cầu hoàn thiện tài liệu.

**Goal:** Hai app tích hợp một provider tại relayhub.dungxbuif.com, có realtime có quyền và job OCR bền chạy trên Mac.

**Architecture:** Go API/dispatcher giữ project và job ledger trong PostgreSQL, phát dispatch qua transactional outbox vào JetStream. Centrifugo chạy riêng cho WSS; app/worker chỉ dùng HTTPS/WSS SDK.

**Tech Stack:** Go, PostgreSQL, NATS JetStream, Centrifugo OSS, React/TypeScript/Vite, Docker Compose.

**Spec:** [SPEC.md](../SPEC.md), [SYSTEM_DESIGN.md](../SYSTEM_DESIGN.md), [API_CONTRACT.md](../API_CONTRACT.md), [OPERATIONS.md](../OPERATIONS.md).

## Global constraints

- Domain duy nhất `relayhub.dungxbuif.com`; API `/api/v1`, WSS `/connection/websocket`.
- API/worker public có credentials; admin chỉ Tailnet + login; không public broker/database.
- Project isolation từ credentials, không tin project trong body.
- PostgreSQL transaction commit là accepted boundary; NATS dispatch retry qua outbox.
- At-least-once, không hứa exactly-once tác dụng phụ.
- TypeScript SDK trước; worker riêng cho handler tin cậy, không arbitrary-code runtime.
- Ghim version/digest, ARM64 compatibility khi dùng Mac/Pi; không dùng latest.
- Không triển khai Kubernetes, Kafka, Redis, billing hoặc tunnel riêng trong MVP.
- Các lệnh build/test dưới đây chạy từ `/path/to/RelayHub/src`, luôn prefix `rtk`.

## Trạng thái và thứ tự

Source bootstrap có tại `src/`; các Task 0–6 bên dưới chưa hoàn thành. Bootstrap không thay thế Task 1 auth/provisioning. Task 0 → 1 → 2 → 3 → 4 → 5 → 6. Mỗi task có artifact và acceptance độc lập; không cần thêm agent để hoàn thành planning này.

## File map dự kiến

File map bên dưới tương đối với `src/`; một số file bootstrap đã tồn tại, cần mở rộng theo task. Planning artifacts luôn nằm tại `../planning/`, các links spec nằm ở thư mục cha.

```text
cmd/relayhub/main.go               API/dispatcher/CLI entrypoint
internal/platform/                config, DB, HTTP errors, lifecycle
internal/projects/                keys, scopes, admin sessions
internal/jobs/                    ledger, outbox, claim, retry, attempts
internal/realtime/                token, channel policy, engine client
internal/dispatch/                NATS publish, reconcile, event bridge
migrations/                       schema SQL
api/openapi.json                  public contract
sdk/typescript/src/               server, realtime, worker SDK
web/src/                          admin UI
examples/ocr/                     sample app/worker integration
examples/second-app/              isolation fixture
deploy/                           Compose, Centrifugo/NATS/edge config
tests/integration/                real DB/broker/API tests
tests/e2e/                        browser + fault/recovery scenarios
```

## Task 0 — Validate deployment assumptions

**Files:** Create `planning/CAPACITY_REPORT.md`, `planning/DEPENDENCY_LOCK.md`, `deploy/README.md`.

**Interfaces:** Consumes OPERATIONS deployment gates; produces measured capacity and exact version/image manifest for Task 1. No live mutation.

- [ ] Read actual VPS resources and current ingress through existing authorized read-only access; record timestamp, architecture, RAM, disk, service ports and provider placement budget. Do not read/export secret values.
- [ ] Check candidate stable Go/Node/PostgreSQL/NATS/Centrifugo releases from official sources; record version, digest, supported architectures, license and compatibility. Reject image without target architecture.
- [ ] Verify one-node memory engine plus connection/subscription token support and required OSS features in pinned Centrifugo. Verify NATS work queue, explicit ACK and reject-new capacity behavior.
- [ ] Document DNS/TLS insertion point, admin Tailnet route and existing domains regression list. Output concrete config paths from inventory; do not guess host paths.
- [ ] Review capacity against trial in OPERATIONS. If insufficient, record revised placement proposal before infrastructure execution. Commit only the three task artifacts after review.

**Acceptance:** An implementer can select exact images and ingress files without retrieving secrets or guessing architecture. This is a deployment gate, not a reason to block local coding if VPS access is unavailable.

## Task 1 — Foundation, project auth and provisioning

**Files:** Create `go.mod`, `cmd/relayhub/main.go`, `internal/platform/{config,http,db}.go`, `internal/projects/{keys,admin,handlers}.go`, `migrations/001_projects.sql`, `api/openapi.json`, `deploy/compose.dev.yml`, `tests/integration/projects_test.go`.

**Interfaces:** HTTP contract is API_CONTRACT. Internal middleware returns `Principal{ProjectID string, KeyID string, Scopes []string}` from bearer key. Healthz returns minimal liveness; readiness is internal.

- [ ] Encode every MVP endpoint/request/error from API_CONTRACT into OpenAPI, including separate realtime session/grant and attempt-scoped progress.
- [ ] Add executable isolation test using real PostgreSQL: create projects A/B and scoped keys, create resource in A, fetch via B →404; absent key →401; wrong scope →403. Include revoke →401 and admin CSRF rejection.
- [ ] Run `rtk go test ./tests/integration -run TestProjectIsolation -count=1`; verify failure before implementation.
- [ ] Implement key middleware and project-scoped queries. Store random key verifier hash, never plaintext; one-time key output. Admin local password hashing/session/CSRF follow OPERATIONS.

```go
// Required query shape: ownership is part of lookup, not a later UI filter.
row := db.QueryRowContext(ctx,
    "SELECT id FROM queues WHERE project_id=$1 AND id=$2",
    principal.ProjectID, queueID)
```

- [ ] Start pinned dev DB via `rtk docker compose -f deploy/compose.dev.yml up -d`; run `rtk go test ./internal/projects ./tests/integration -count=1` and OpenAPI schema validation using a version-pinned validator.
- [ ] Verify secret redaction and readiness under DB outage. Commit scoped files with message `feat(relayhub): add project provisioning and scoped auth`.

**Acceptance:** Two projects provisioned; no cross-project resource reads; admin inaccessible via public routing fixture.

## Task 2 — Durable enqueue and outbox dispatch

**Files:** Create `migrations/002_jobs.sql`, `internal/jobs/{store,enqueue,outbox}.go`, `internal/dispatch/publisher.go`, `tests/integration/enqueue_test.go`, `deploy/nats.conf`.

**Interfaces:** `Enqueue(ctx, principal, input, idempotencyKey) → {jobId,status}`; `DispatchPending(ctx)` publishes dispatch records. Jobs ledger authoritative, NATS record contains job ID/project/dispatch generation.

- [ ] Test same-key/same-body → same ID, changed-body →409, quota overflow →429, oversized body →413. Kill response after DB commit, resend same key and assert one job/outbox logical dispatch.
- [ ] Run `rtk go test ./tests/integration -run TestEnqueue -count=1`; confirm failure.
- [ ] Implement transaction with unique `(project_id,queue_id,idempotency_key)`, canonical payload hash, job policy snapshot and outbox. Serialize quota check per project to avoid parallel bypass.

```sql
BEGIN;
-- Lock project quota row; resolve existing key before counting new job.
-- Insert jobs + idempotency_records + outbox in this same transaction.
COMMIT;
-- HTTP 202 is allowed only after successful commit.
```

- [ ] Implement publisher with stable `Nats-Msg-Id`; mark sent only after PubAck. Conditional status update cannot overwrite running/succeeded. Configure bounded file-backed work stream and reject-new behavior.
- [ ] Test NATS offline during enqueue: accepted job retained; restore NATS → queued. Inject crash after PubAck before sent flag; duplicate dispatch does not create extra job.
- [ ] Run `rtk go test ./internal/jobs ./internal/dispatch ./tests/integration -count=1`; commit `feat(relayhub): persist jobs with transactional outbox`.

**Acceptance:** Accepted jobs survive API/NATS restart; Postgres write failure never returns accepted.

## Task 3 — Worker leases, retry and replay

**Files:** Create `migrations/003_attempts.sql`, `internal/jobs/{claim,heartbeat,complete,retry,replay}.go`, `internal/dispatch/reconcile.go`, `tests/integration/leases_test.go`.

**Interfaces:** API claim/heartbeat/complete/fail from API_CONTRACT; lease uses DB time and compare-and-set; `Reconcile(ctx)` handles expired leases and retry deadlines.

- [ ] Add real-clock-controlled tests: two concurrent claims start one attempt; expired lease complete →409; terminal duplicate complete →200 only for matching hash; mismatched result →409.
- [ ] Run `rtk go test ./tests/integration -run TestLease -count=1`; confirm failure.
- [ ] Implement atomic job lock/attempt insert, lease hash, bounded deadlines and handlerVersion match. Reject stale updates with conditional mutation:

```sql
UPDATE attempts SET lease_expires_at = $1
WHERE id=$2 AND project_id=$3 AND lease_hash=$4
  AND status='running' AND lease_expires_at > CURRENT_TIMESTAMP;
-- Exactly one row required; otherwise LEASE_LOST.
```

- [ ] Commit result + completion outbox before ACK. Reconciler treats live lease redelivery as defer; terminal redelivery as ACK. Retry_wait schedule creates new generation via outbox; failed jobs remain queryable. Replay creates linked new job with its own idempotency key.
- [ ] Fault test API restart during handler, worker death, lost complete response and retry exhaustion. Assert one terminal ledger and idempotent fixture side effect, not absence of redelivery.
- [ ] Run `rtk go test ./internal/jobs ./internal/dispatch ./tests/integration -count=1`; commit `feat(relayhub): add worker leases and recovery`.

**Acceptance:** Runaway/stale workers cannot complete newer attempts; recovery needs no in-memory lease state.

## Task 4 — Realtime service and frontend integration

**Files:** Create `internal/realtime/{tokens,channels,publish}.go`, `deploy/centrifugo.json`, `sdk/typescript/src/realtime.ts`, `tests/e2e/realtime.spec.ts`, `examples/second-app/README.md`.

**Interfaces:** `/realtime/sessions`, `/realtime/grants`, `/realtime/publish`; SDK `connect()`, `subscribe(channel,{getGrant,onMessage,onRecoveryFailed})` from INTEGRATION_FLOWS.

- [ ] Add browser tests for correct channel message, project-B same-user denial, wrong channel grant, expired token refresh, and client publish rejection.
- [ ] Run `rtk pnpm exec playwright test tests/e2e/realtime.spec.ts`; confirm failure with fixture backend/token service.
- [ ] Implement project/user encoded principal, channel mapping and short-lived connection/subscription tokens compatible with pinned Centrifugo; run server standalone with memory history limits.

```ts
const sub = realtime.subscribe("jobs/123", {
  getGrant: () => appApi.post("/jobs/123/realtime-grant"),
  onMessage: applyIfNewer,
  onRecoveryFailed: refreshSnapshot,
});
```

- [ ] Enforce attempt progress destination from stored job, not worker request. Test worker cannot publish to different job/channel. Add backpressure/coalescing in SDK fixture.
- [ ] Restart Centrifugo and assert reconnect + snapshot fallback; subscribe-before-snapshot fixture excludes stale updates using state version.
- [ ] Run realtime Go integration tests and Playwright; commit `feat(relayhub): add scoped realtime sessions and subscriptions`.

**Acceptance:** One socket supports multiple authorized subscriptions; no assumption that publish acceptance means client read.

## Task 5 — Server/worker SDK, OCR and dashboard

**Files:** Create `sdk/typescript/src/{server,worker,errors}.ts`, `sdk/typescript/test/worker.test.ts`, `web/src/{App,api,Jobs,JobDetail,Projects}.tsx`, `examples/ocr/{backend,worker}.ts`, `tests/e2e/ocr.spec.ts`.

**Interfaces:** `RelayServer.jobs.enqueue`, `RelayWorker.handle(queue,{version},handler)`, handler context `{signal,progress}`; no raw NATS SDK in app-facing examples.

- [ ] Test HTTP retry preserves idempotency key; heartbeat stops on terminal; lease loss aborts handler signal; uncertain complete retries complete rather than rerunning handler. Run `rtk pnpm --dir sdk/typescript test` and verify failure before implementation.
- [ ] Implement bounded long-poll slots and jittered retries; exact error classifications from API_CONTRACT. Separate frontend export from server secrets modules.
- [ ] Build admin project/jobs/attempts view against real API, cursor pagination and explicit replay action creating new job. Display accepted vs queued distinctly; never show hidden payload/secret by default.
- [ ] Integrate OCR adapter using existing app interfaces discovered at execution time. If API unavailable, synthetic adapter remains labeled fixture and real-OCR acceptance stays unchecked.

```ts
worker.handle("extract-text", {version:"v1"}, async (job, ctx) => {
  const existing = await results.find(job.data.appJobId);
  if (existing) return existing;
  const result = await extract(job.data.fileId, {signal:ctx.signal,
    onProgress: percent => ctx.progress({percent})});
  return results.saveIdempotently(job.data.appJobId, result);
});
```

- [ ] Run SDK tests, frontend build and `rtk pnpm exec playwright test tests/e2e/ocr.spec.ts`; verify closed browser does not stop job and reopening loads result.
- [ ] Commit `feat(relayhub): add SDK dashboard and OCR integration`.

**Acceptance:** Two apps use same provider domain/SDK credentials model; one real OCR path works without app-local WebSocket or MQ server.

## Task 6 — Deploy readiness, fault matrix and restore

**Files:** Create `deploy/{compose.yml,edge-routing.conf,backup.sh,restore.sh}`, `tests/e2e/faults.spec.ts`, `planning/VALIDATION_REPORT.md`, `planning/RELEASE_CHECKLIST.md`.

**Interfaces:** Uses pinned dependencies and actual ingress paths from Task 0; follows OPERATIONS defaults and routing priority from SYSTEM_DESIGN.

- [ ] Write route contract tests: admin denied public, API 404 stays JSON, hooks 404 in MVP, WSS Upgrade succeeds, claim long poll does not proxy-timeout. Run failure against unconfigured test edge.
- [ ] Create Compose and edge config from inventory with internal service ports, persisted volumes and external secret injection; run `rtk docker compose -f deploy/compose.yml config --quiet` without printing interpolated secrets.
- [ ] Run complete fault matrix and capacity trial in OPERATIONS; record measured results, not just commands. Include project quota concurrency races and storage outage.
- [ ] Backup and restore isolated instance; replay reconstructed dispatch from Postgres ledger, verify terminal jobs do not rerun and permissions remain isolated. Measure RPO/RTO against targets.
- [ ] Prepare rollback: prior image digests, backup location and compatible migration strategy. Never run destructive down-volume rollback. Review concrete ingress diff before live execution under applicable authorization.
- [ ] Run `rtk go test ./...`, SDK tests, frontend build, E2E once on final candidate. Record failures or unavailable real integration explicitly; commit only task-owned artifacts.

**Acceptance:** No open failure in MVP matrix; live capacity and backup restore proven before release claim. Planning completion does not imply this gate passed.

## Coverage map

| Requirement | Task |
|---|---|
| Project/key isolation, admin auth | 1 + 6 |
| Stable API and error contract | 1 + 5 |
| Durable enqueue/idempotency/quota | 2 |
| Lease/retry/failure/replay | 3 |
| Socket tokens/reconnect/channel rights | 4 |
| Trusted function handler, SDK, OCR | 5 |
| Same-domain ingress, ARM64, metrics/capacity/backup | 0 + 6 |

Public functions runtime, webhook ingress/egress, event fan-out API, scheduler, billing and tunnel portal remain later-phase scope in [ROADMAP.md](../ROADMAP.md), not omissions in MVP.

## Execution entrypoint sau source bootstrap

- Hoàn thành [SOURCE_BOOTSTRAP.md](SOURCE_BOOTSTRAP.md): process/config/health/error boundaries, chưa có engine.
- Tiếp theo Task 0 xác minh dependencies; Task 1 thêm auth/project/DB. Không thay response 501 bằng mock in-memory rồi đánh dấu task xong.
- Thêm contract/compatibility tests ở Task 4: SDK Centrifugo và raw WebSocket đúng protocol; Socket.IO không nằm trong supported matrix.
- Mỗi milestone cập nhật [STATUS.md](STATUS.md), runtime OpenAPI và src/README cùng code.

## Documentation deliverables — mandatory amendment 2026-09-11

Read [DOCUMENTATION_STRATEGY.md](../DOCUMENTATION_STRATEGY.md) before each task. Every feature task updates internal docs, public integration docs and agent-readable contracts together.

- Task 1: initialize Docusaurus under `public-docs/` (relative to RelayHub root), Markdown-first; public `/docs/` routes; guide availability matches runtime auth behavior. Pin dependencies and test Markdown/MDX compatibility.
- Tasks 2–3: publish queue/worker semantics, retry/idempotency examples, errors and troubleshooting alongside implementation; test examples against actual API.
- Task 4: publish Centrifugo SDK and raw WebSocket compatibility guide; explicitly exclude Socket.IO compatibility.
- Task 5: implement public Skills navbar tab, canonical skill packages, copy/raw/ZIP actions and version/checksum manifest. Generate llms.txt, llms-full.txt, raw Markdown, resources.json and versioned OpenAPI from reviewed public sources. Use skill-creation instructions when actually authoring packages.
- Task 6: build site; fetch all exports over HTTP; verify copied/downloaded skill content and complete archives; scan publication allowlist for private data; verify public docs and protected admin routes on the same hostname.

Public docs tooling lives outside `src/web` (admin dashboard). Source schema stays canonical; public API export is generated. Do not treat current public-docs README as a completed website or released skill.
