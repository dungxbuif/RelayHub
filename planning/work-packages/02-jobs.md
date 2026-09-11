# Durable jobs và worker gateway — Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans task-by-task. All implementation checkboxes start unchecked.

**Goal:** Durable jobs và worker gateway.

**Architecture:** See [engineering contract](../ENGINEERING_DETAILS.md); app-facing protocols remain HTTPS/WSS, broker private.

**Tech Stack:** Go, PostgreSQL, JetStream, Centrifugo; React/TypeScript, Docusaurus where relevant.

**Spec:** [SPEC](../../SPEC.md), [API](../../API_CONTRACT.md), [operations](../../OPERATIONS.md).

**Dependencies:** Depends F1–F3; DB ledger and outbox remain authoritative.

## Global constraints

Paths in this plan are repo-root-relative. Commands run from `src/` and are prefixed with `rtk`; pnpm scripts and integration harness are introduced by F1/F4 and the relevant task before invocation. Use real dependencies for contract tests. Update runtime OpenAPI only as endpoints are implemented. All tasks end with scoped commit after tests; do not stage unrelated files.

## J1 — Ledger migrations và enqueue transaction

**Files:** src/migrations/003_ledger.sql; src/internal/jobs/enqueue.go; src/internal/jobs/canonical.go; src/internal/jobs/store.go.

**Interfaces:** Enqueue(project,request,key)→job; policy snapshot; 202 after DB commit; idempotency exact contract.

**Implementation decisions:** Enforce nonterminal quota inside project row lock; integer and Unicode canonicalization verified.

**Acceptance cases:** 100 concurrent identical requests→one job; same key/different payload→409; NATS off→202 accepted; DB off→503; payload >64KiB→413; reordered JSON keys→same hash.

- [ ] Write tests named for the acceptance cases; each test must fail because the required behavior is missing. Capture actual versus expected HTTP/state transitions.
- [ ] Run RED: `rtk go test -tags=integration ./tests/integration -run TestEnqueue -count=1`; verify expected failure, not unavailable test infrastructure.
- [ ] Implement the specified interfaces in the listed files, following ENGINEERING_DETAILS; keep immutable API/project boundaries.
- [ ] Run GREEN: `rtk go test -tags=integration ./tests/integration -run TestEnqueue -count=1`; confirm acceptance cases and related regression tests pass.
- [ ] Reconcile internal/public docs: API_CONTRACT.md; public-docs/docs/queues/enqueue.md; update raw/agent exports and relevant skills or record why no skill changes apply.
- [ ] Review changed files, record evidence in STATUS and commit only J1 files.

## J2 — Outbox publisher và broker provisioning

**Files:** src/internal/dispatch/publisher.go; src/internal/dispatch/topology.go; src/deploy/nats.conf; src/cmd/relayhub/main.go.

**Interfaces:** dispatch mode; stable message ID; exact filter consumer per project/queue/version; owner/TTL outbox claims.

**Implementation decisions:** Network publish outside DB transaction; mark sent only if claim_owner matches; resource provisioning idempotent.

**Acceptance cases:** Two publishers→no lost row; crash after PubAck→duplicate notification safely tolerated; broker full→outbox unsent; status running never regresses to queued.

- [ ] Write tests named for the acceptance cases; each test must fail because the required behavior is missing. Capture actual versus expected HTTP/state transitions.
- [ ] Run RED: `rtk go test -tags=integration ./tests/integration -run TestOutbox -count=1`; verify expected failure, not unavailable test infrastructure.
- [ ] Implement the specified interfaces in the listed files, following ENGINEERING_DETAILS; keep immutable API/project boundaries.
- [ ] Run GREEN: `rtk go test -tags=integration ./tests/integration -run TestOutbox -count=1`; confirm acceptance cases and related regression tests pass.
- [ ] Reconcile internal/public docs: SYSTEM_DESIGN.md; public-docs/docs/queues/delivery.md; update raw/agent exports and relevant skills or record why no skill changes apply.
- [ ] Review changed files, record evidence in STATUS and commit only J2 files.

## J3 — Claim, heartbeat và attempt persistence

**Files:** src/migrations/004_attempts.sql; src/internal/jobs/claim.go; src/internal/jobs/heartbeat.go; src/internal/httpapi/workers.go.

**Interfaces:** Claim returns persisted attempt/lease; heartbeat checks credential queue and worker_key_id; version routing exact.

**Implementation decisions:** Lock order usage→job→attempt; hash lease token, never store plaintext; broker ACK handle not a correctness dependency.

**Acceptance cases:** Two claimers→one running attempt; wrong queue scope→403; wrong version gets no job; API restart retains lease; expiry cannot be extended past run deadline.

- [ ] Write tests named for the acceptance cases; each test must fail because the required behavior is missing. Capture actual versus expected HTTP/state transitions.
- [ ] Run RED: `rtk go test -tags=integration ./tests/integration -run TestClaimLease -count=1`; verify expected failure, not unavailable test infrastructure.
- [ ] Implement the specified interfaces in the listed files, following ENGINEERING_DETAILS; keep immutable API/project boundaries.
- [ ] Run GREEN: `rtk go test -tags=integration ./tests/integration -run TestClaimLease -count=1`; confirm acceptance cases and related regression tests pass.
- [ ] Reconcile internal/public docs: API_CONTRACT.md; public-docs/docs/workers/leases.md; update raw/agent exports and relevant skills or record why no skill changes apply.
- [ ] Review changed files, record evidence in STATUS and commit only J3 files.

## J4 — Complete, fail, retry, replay và reconciliation

**Files:** src/internal/jobs/complete.go; src/internal/jobs/replay.go; src/internal/dispatch/reconcile.go; src/internal/jobs/queries.go; src/migrations/005_audit.sql.

**Interfaces:** Terminal transaction+completion outbox; scheduler creates one new generation; GET jobs/attempts supports project filters.

**Implementation decisions:** Restore dispatch from ledger; old generation ACK/no execution; clock tests use DB deadlines and synchronize transactions without arbitrary long sleeps.

**Acceptance cases:** Lost complete response→idempotent success; stale attempt→409; five failures→failed; replay→new ID; live expired-job deadline→failed; two reconcilers do not double decrement usage.

- [ ] Write tests named for the acceptance cases; each test must fail because the required behavior is missing. Capture actual versus expected HTTP/state transitions.
- [ ] Run RED: `rtk go test -tags=integration ./tests/integration -run TestJobRecovery -count=1`; verify expected failure, not unavailable test infrastructure.
- [ ] Implement the specified interfaces in the listed files, following ENGINEERING_DETAILS; keep immutable API/project boundaries.
- [ ] Run GREEN: `rtk go test -tags=integration ./tests/integration -run TestJobRecovery -count=1`; confirm acceptance cases and related regression tests pass.
- [ ] Reconcile internal/public docs: OPERATIONS.md; public-docs/docs/queues/retry-replay.md; update raw/agent exports and relevant skills or record why no skill changes apply.
- [ ] Review changed files, record evidence in STATUS and commit only J4 files.
