# RelayHub DLQ Replay and Event Lifecycle Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans and superpowers:test-driven-development task-by-task.

**Goal:** Add generation-fenced single/batch DLQ replay, complete persisted event timelines, and safe Admin workflows without republishing events or weakening existing callback/stream guarantees.

**Architecture:** PostgreSQL remains authoritative. A replay advances the existing delivery to a new generation in one transaction, resets only generation-owned dispatch state, creates a generation-specific broker wake-up, stores a replay-idempotency result and appends an audit row. Every callback claim, stream assignment, broker envelope and settlement carries the delivery generation so work from an older generation cannot mutate the replay. Timeline responses normalize persisted event, delivery, outbox, attempt and audit records; text logs are never parsed.

**Tech Stack:** Go 1.27, PostgreSQL 17, NATS JetStream, React 18/Vite, TanStack Query, Vitest and Playwright.

**Spec:** `docs/superpowers/specs/2026-09-20-relayhub-platform-expansion-design.md` sections 8, 10-11, 21-22 and delivery item 6.

## Global constraints

- No Python SDK and no reverse-proxy implementation.
- Replay is valid only from `dead_letter`; it does not create or republish an event.
- Every mutable delivery path is generation-fenced before replay is exposed.
- Single and batch POSTs require `Idempotency-Key`; one key cannot be rebound to another selection.
- Batch selection is explicit, unique and bounded to 100 delivery IDs; there is no “replay all matching”.
- Original delivery attempts and audit history are append-only. Generation is recorded on new attempts and timeline entries.
- Admin errors are bounded and never disclose payloads, callback URLs, credentials, headers, SQL or broker internals.

---

### Task 1: Define lifecycle contracts and generation schema

**Files:**

- Modify: `backend/internal/adminread/models.go`
- Modify: `backend/internal/store/store.go`
- Create: `backend/internal/store/postgres/migrations/012_delivery_generations.sql`
- Modify: `backend/internal/store/postgres/migrations_test.go`
- Create/modify matching model and migration tests.

**Produces:** delivery `generation >= 1`; generation on attempts and assignment/lease state; replay request/result/idempotency models; safe attempt detail; normalized timeline item types; replay-idempotency table with request fingerprint and bounded result; supporting indexes and constraints.

- [ ] Write model serialization and migration invariant tests and verify RED.
- [ ] Backfill generation 1 without changing current delivery state.
- [ ] Add narrow lifecycle store interfaces rather than mutation methods to `AdminReadStore`.
- [ ] Reject empty, duplicate, oversized and malformed selections before storage.
- [ ] Commit `feat: define delivery lifecycle contracts`.

### Task 2: Fence every existing callback and stream transition by generation

**Files:**

- Modify: `backend/internal/store/callbacks.go`
- Modify: `backend/internal/store/store.go`
- Modify: `backend/internal/store/postgres/callbacks.go`
- Modify: `backend/internal/store/postgres/delivery_assignments.go`
- Modify: `backend/internal/store/postgres/outbox.go`
- Modify: `backend/internal/worker/jetstream.go`
- Modify: `backend/internal/streamgateway/**`
- Modify: `backend/internal/streamprotocol/**` and matching schemas/docs.

- [ ] Write stale callback, stale stream ACK/NACK/progress and stale outbox-message tests and verify RED.
- [ ] Carry generation in broker envelopes, callback dispatches, stream delivery frames and client settlement frames.
- [ ] Include generation in every SQL transition predicate and idempotent terminal comparison.
- [ ] Keep current generation-1 clients compatible where the v1 durable protocol contract permits; fail closed when a replayed generation lacks a matching receipt.
- [ ] Pass callback/stream race integration and commit `feat: fence delivery generations`.

### Task 3: Implement atomic idempotent single and batch replay

**Files:**

- Create: `backend/internal/store/postgres/admin_lifecycle.go`
- Create: `backend/internal/store/postgres/admin_lifecycle_integration_test.go`
- Modify: `backend/internal/store/postgres/audit.go`
- Modify: `backend/internal/store/postgres/admin_read.go`

**Transaction:** lock the sorted explicit delivery set; verify every row is dead-letter; compare/store the idempotency fingerprint; increment generation; clear callback/assignment terminal lease fields; reset attempts for the new generation while retaining old attempt rows; recreate or reset outbox state with generation-specific message ID/payload; append one audit outcome per delivery plus a bounded batch summary; return the durable replay result.

- [ ] Prove duplicate/concurrent same-key submissions return byte-equivalent results and create one generation only.
- [ ] Prove same key plus different selection conflicts and mixed-validity batches roll back completely.
- [ ] Prove replayed callback and stream deliveries create exactly one eligible wake-up each.
- [ ] Prove old generation claims/receipts cannot settle after commit.
- [ ] Commit `feat: replay dead-letter deliveries atomically`.

### Task 4: Build persisted event detail and lifecycle timeline

**Files:**

- Modify: `backend/internal/adminread/models.go`
- Modify: `backend/internal/store/postgres/admin_read.go`
- Create: `backend/internal/store/postgres/admin_timeline_integration_test.go`
- Modify: `backend/internal/service/admin_reads.go`

**Timeline sources:** event creation, delivery creation/state, outbox dispatch, stream assignment/settlement, callback attempts, dead-letter transition and operator replay audit. Ordering is stable ascending `(occurred_at, source_rank, stable_id)` and types are an allowlist.

- [ ] Write real-PostgreSQL timeline tests covering callback, stream, retry, dead letter and replay generations.
- [ ] Return canonical event detail plus delivery summaries and safe attempt history.
- [ ] Never infer events from log text or expose payload in list/timeline rows.
- [ ] Commit `feat: expose persisted event lifecycle timelines`.

### Task 5: Expose authenticated replay and timeline HTTP APIs

**Files:**

- Create: `backend/internal/service/admin_lifecycle.go`
- Create: `backend/internal/service/admin_lifecycle_test.go`
- Modify: `backend/internal/httpapi/admin_reads.go`
- Create: `backend/internal/httpapi/admin_lifecycle_test.go`
- Modify: `backend/internal/httpapi/router.go`
- Modify: `backend/internal/httpapi/routes.go`
- Modify: `backend/cmd/relayhub/main.go`
- Modify: `web/docs/static/openapi.json` and generated Skill.

**Endpoints:** `GET /api/v1/admin/events/{eventID}/timeline`, expanded safe event/DLQ details, `POST /api/v1/admin/dlq/{deliveryID}/replay`, and `POST /api/v1/admin/dlq/replay`.

- [ ] Require Admin auth, cookie CSRF and valid `Idempotency-Key` on replay POSTs.
- [ ] Map not-found/conflict/invalid input to generic stable errors and set `Idempotent-Replayed: true` on duplicate results.
- [ ] Add route/OpenAPI/auth parity and response redaction tests.
- [ ] Commit `feat: expose Admin lifecycle APIs`.

### Task 6: Build Event Timeline and DLQ replay workflows in React Admin

**Files:**

- Modify: `web/admin/src/api/adminReads.ts`
- Create: `web/admin/src/components/Timeline.tsx`
- Create: `web/admin/src/components/ConfirmationDialog.tsx`
- Create: `web/admin/src/pages/EventDetailPage.tsx`
- Create: `web/admin/src/pages/DeadLetterDetailPage.tsx`
- Modify: `web/admin/src/pages/EventsPage.tsx`
- Modify: `web/admin/src/pages/DeadLettersPage.tsx`
- Modify: `web/admin/src/app/App.tsx`
- Create/modify matching Vitest tests.

**UI behavior:** linked detail rows; semantic chronological timeline with generation/attempt badges; safe detail panels; checkbox selection capped at 100; single/batch confirmation naming the exact IDs; one generated idempotency key retained for retries; disabled duplicate submit while pending; result summary and query invalidation after success.

- [ ] Test loading/empty/error/degraded states, keyboard dialog focus, narrow layout and no secret rendering.
- [ ] Test replay duplicate-click suppression and retry reuses the same key.
- [ ] Build deterministically and regenerate `backend/web/embed.go`.
- [ ] Commit `feat: add DLQ replay and lifecycle timeline UI`.

### Task 7: Prove crash, concurrency and multi-replica behavior

**Files:**

- Modify: `backend/internal/httpapi/horizontal_scale_integration_test.go`
- Create: `backend/internal/httpapi/admin_lifecycle_integration_test.go`
- Create: `web/admin/e2e/admin-lifecycle.spec.ts`
- Modify: `.github/workflows/admin-ci.yml`

- [ ] Race two replicas replaying the same key and assert one generation/work item/audit outcome.
- [ ] Pause after PostgreSQL commit before broker dispatch, restart the worker and prove recovery.
- [ ] Submit stale callback and stream settlements after replay and prove fencing.
- [ ] Browser-test single/batch confirmation, timeline order, 401/403 recovery, responsive selection and sentinel absence.
- [ ] Commit `test: verify DLQ lifecycle across replicas`.

### Task 8: Close docs and release gate

**Files:**

- Modify: `README.md`
- Modify: `docs/operations/runbook.md`
- Modify: `web/docs/docs/control-panel/overview.md`
- Modify: `web/docs/static/security.md`
- Modify: `web/docs/static/llms-full.txt`

- [ ] Document replay versus publisher idempotency, generation fencing, selection limits, audit behavior and recovery.
- [ ] Regenerate OpenAPI, Skill, llms-full and embedded Admin.
- [ ] Run Admin npm CI/audit, Go unit/race/integration, docs build/audit, SDK tests, contracts and Docker build.
- [ ] Self-review SQL locking order, idempotency rebinding, stale-worker fencing, CSRF, payload exposure and generated artifacts.
- [ ] Commit `docs: close DLQ lifecycle release gate`.

## Completion evidence

This phase is complete only when a dead-letter callback or stream delivery can be replayed once or as an explicit bounded batch; duplicate/concurrent submits are idempotent; old generations cannot settle; event timelines are reconstructed entirely from persisted state; every action is audited; browser workflows are accessible and reveal no protected data; and the full release gate passes.
