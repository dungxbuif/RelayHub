# RelayHub Admin Read API and Dashboard Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans and superpowers:test-driven-development task-by-task.

**Goal:** Replace Admin placeholders with bounded cluster-wide dashboard metrics and searchable event, DLQ and audit read models backed by Redis and PostgreSQL truth.

**Architecture:** PostgreSQL remains authoritative for events, deliveries and audit rows. API/worker replicas record bounded short-window counters into minute-aligned Redis buckets; Admin services combine those aggregates with PostgreSQL state counts and instance diagnostics. The React client consumes typed JSON only, refreshes every five seconds without overlapping requests, and never parses Prometheus exposition.

**Tech Stack:** Go 1.27, PostgreSQL 17, Redis 7.4, React 18, TanStack Query 5, Recharts 3, Vitest, Playwright.

**Spec:** `docs/superpowers/specs/2026-09-20-relayhub-platform-expansion-design.md` sections 8-9, 21-22 and delivery item 5.

## Global constraints

- No Python SDK and no reverse-proxy implementation.
- All lists use opaque versioned cursors, stable descending `(timestamp,id)` order and `1..100` limits.
- Filters are allowlisted; payloads, secrets, URLs, headers, Redis keys and raw database errors never enter Admin responses.
- Redis metrics are reconstructible and bounded. Redis failure degrades charts explicitly but cannot block accepted events or mutate PostgreSQL truth.
- Dashboard counts use PostgreSQL for durable delivery state; Redis is used only for rolling rates/outcomes and cluster ephemeral status.
- This phase is read-only. Replay, timeline mutation, connection termination and app/rule editing remain in their dedicated later phases.

---

### Task 1: Define bounded Admin read contracts and cursor codec

**Files:**

- Create: `backend/internal/adminread/models.go`
- Create: `backend/internal/adminread/cursor.go`
- Create: `backend/internal/adminread/cursor_test.go`
- Modify: `backend/internal/store/store.go`

**Produces:** versioned base64url cursor encoding `(timestamp,id)`, validated `ListOptions`, event/DLQ/audit summaries, dashboard durable counts and store interfaces. Cursor decode rejects malformed, oversized, wrong-version and filter-mismatched values with a generic invalid-argument error.

- [ ] Write cursor/filter/limit tests and verify RED.
- [ ] Implement immutable response models and a maximum 100-item page contract.
- [ ] Add narrow `AdminReadStore` interfaces without expanding mutation interfaces.
- [ ] Run `go -C backend test ./internal/adminread ./internal/store -count=1`.
- [ ] Commit `feat: define bounded Admin read contracts`.

### Task 2: Implement PostgreSQL event, DLQ, audit and durable-count reads

**Files:**

- Create: `backend/internal/store/postgres/admin_read.go`
- Create: `backend/internal/store/postgres/admin_read_integration_test.go`
- Create: `backend/internal/store/postgres/migrations/011_admin_read_indexes.sql`
- Modify: `backend/internal/store/postgres/migrations_test.go`

**Queries:**

- events: filters `type`, `source_app_id`, `from`, `to`; no payload in list rows;
- DLQ: callback/stream deliveries in `dead_letter`, filters `source_app_id`, `target_app_id`, `sink`, `reason`, `from`, `to`;
- audit: filters `actor_type`, `action`, `resource_type`, `resource_id`, `outcome`, `from`, `to`; metadata remains allowlisted and bounded;
- dashboard: pending, retrying, dead-letter and oldest-pending age from one consistent read-only transaction.

- [ ] Write real-PostgreSQL pagination/filter/query-plan integration tests and verify RED.
- [ ] Add only indexes proven by the fixed query shapes.
- [ ] Implement keyset pagination with `limit+1`, stable ties and context cancellation.
- [ ] Prove no payload/credential/callback URL appears in serialized list results.
- [ ] Run race integration tests and commit `feat: add PostgreSQL Admin read models`.

### Task 3: Add bounded Redis rolling metrics and cluster ephemeral state

**Files:**

- Create: `backend/internal/redisstate/dashboard.go`
- Create: `backend/internal/redisstate/dashboard_integration_test.go`
- Modify: `backend/internal/redisstate/keyspace.go`
- Modify: `backend/internal/observability/metrics.go`
- Modify: `backend/internal/httpapi/metrics.go`
- Modify: `backend/internal/broker/nats/client.go`
- Modify: realtime/stream connection lifecycle call sites as required.

**Produces:** atomic bucket increments and bounded reads for request count/status classes, event outcomes, callback outcomes, connection deltas and NATS state events. Buckets are UTC aligned, support `5m`, `15m`, `1h`, `6h`, `24h` windows with validated steps, expire after 25 hours and contain only fixed field names. NATS current state includes last-change time and instance ID; aggregate active connections are heartbeat/TTL based so crashed replicas age out.

- [ ] Write two-client Redis tests for aggregation, TTL, bucket boundaries, stale replica expiry, malformed records and Redis outage degradation.
- [ ] Implement atomic Lua updates using Redis server time and fixed-cardinality fields.
- [ ] Wire non-blocking recording after existing Prometheus observations; recording failures increment a bounded internal failure metric only.
- [ ] Run unit and race integration tests; commit `feat: aggregate rolling dashboard metrics`.

### Task 4: Expose authenticated Admin dashboard and list endpoints

**Files:**

- Create: `backend/internal/service/admin_reads.go`
- Create: `backend/internal/service/admin_reads_test.go`
- Create: `backend/internal/httpapi/admin_reads.go`
- Create: `backend/internal/httpapi/admin_reads_test.go`
- Modify: `backend/internal/httpapi/router.go`
- Modify: `backend/internal/runtime/runtime.go`
- Modify: `backend/cmd/relayhub/main.go`
- Modify: `web/docs/static/openapi.json`

**Endpoints:** `GET /api/v1/admin/dashboard`, `/metrics`, `/events`, `/events/{id}`, `/dlq`, `/dlq/{id}` and `/audit`. All accept cookie session or bootstrap bearer auth; safe reads require no CSRF. Responses expose `data`, `next_cursor`, `generated_at` and explicit component freshness/degradation.

- [ ] Write auth, validation, pagination, timeout, redaction and partial-degradation HTTP tests and verify RED.
- [ ] Implement a service deadline and parallel independent reads while preserving deterministic responses.
- [ ] Register routes and runtime dependencies; keep all mutation routes unchanged.
- [ ] Update OpenAPI and generated Skill, then pass static contract parity.
- [ ] Commit `feat: expose Admin dashboard read APIs`.

### Task 5: Build live Overview charts and searchable read-only modules

**Files:**

- Create: `web/admin/src/api/adminReads.ts`
- Create: `web/admin/src/components/MetricCard.tsx`
- Create: `web/admin/src/components/TimeSeriesChart.tsx`
- Create: `web/admin/src/components/DataTable.tsx`
- Create: `web/admin/src/hooks/usePausableRefresh.ts`
- Replace: `web/admin/src/pages/OverviewPage.tsx`
- Create: `web/admin/src/pages/EventsPage.tsx`
- Create: `web/admin/src/pages/DeadLettersPage.tsx`
- Create: `web/admin/src/pages/AuditLogsPage.tsx`
- Modify: `web/admin/src/app/App.tsx`
- Create/modify matching Vitest tests.

**UI behavior:** cards for RPS, error rate, cluster/instance WebSockets, NATS state, delivery counts and oldest pending age; charts for request rate, status classes, event outcomes, delivery outcomes, latency percentiles when available and NATS events. Default refresh is five seconds, pause/resume is visible, an unfinished request prevents overlap, filters are URL-addressable without secrets, pagination is cursor-based and degraded/empty/loading/error states are distinct.

- [ ] Write fake-timer refresh, abort, non-overlap, chart accessibility, filter and pagination tests and verify RED.
- [ ] Implement typed query functions and reusable accessible visualization/table components.
- [ ] Replace only the four read-only placeholders; leave later workflow pages honest.
- [ ] Test reduced motion, narrow viewport and keyboard interaction.
- [ ] Run typecheck, Vitest and deterministic build; regenerate Go embed.
- [ ] Commit `feat: add live Admin dashboard and read views`.

### Task 6: Prove multi-replica aggregation and browser behavior

**Files:**

- Create: `backend/internal/httpapi/admin_reads_integration_test.go`
- Modify: `backend/internal/httpapi/horizontal_scale_integration_test.go`
- Create: `web/admin/e2e/admin-dashboard.spec.ts`
- Modify: `.github/workflows/admin-ci.yml`

- [ ] Drive traffic/outcomes through two API graphs and assert a third reader observes the combined Redis series plus shared PostgreSQL counts.
- [ ] Verify one Redis outage returns explicit degraded metrics while PostgreSQL lists still work.
- [ ] Browser-test pause/resume, no overlapping refresh, responsive charts, event/DLQ/audit filters, cursor navigation, 401 recovery and absence of sensitive sentinels.
- [ ] Pass race integration and real-stack desktop/mobile Playwright.
- [ ] Commit `test: verify multi-replica Admin reads`.

### Task 7: Close contracts, docs and release gate

**Files:**

- Modify: `README.md`
- Modify: `docs/operations/runbook.md`
- Modify: `web/docs/docs/control-panel/overview.md`
- Modify: `web/docs/static/security.md`
- Modify: `web/docs/static/llms-full.txt`

- [ ] Document metric ownership, rolling-window limits, degraded behavior, filters/cursors and read-only DLQ boundary.
- [ ] Regenerate OpenAPI, Skill, llms-full and embedded Admin deterministically.
- [ ] Run Admin npm CI/typecheck/unit/build/audit, Go unit/race/integration, docs build/audit, SDK tests, contracts and Docker build.
- [ ] Self-review the whole phase for SQL injection, cursor tampering, unbounded cardinality, secret exposure and stale generated artifacts.
- [ ] Commit `docs: close Admin read API release gate`.

## Completion evidence

The phase is complete only when multiple replicas contribute to one bounded dashboard window, durable counts and lists remain correct during Redis loss, every list cursor is stable and opaque, the browser shows real data with explicit freshness/degradation, no response or failure artifact contains protected fields, and the full release gate produces fresh passing output.
