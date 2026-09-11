# Release candidate, operations và deployment — Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans task-by-task. All implementation checkboxes start unchecked.

**Goal:** Release candidate, operations và deployment.

**Architecture:** See [engineering contract](../ENGINEERING_DETAILS.md); app-facing protocols remain HTTPS/WSS, broker private.

**Tech Stack:** Go, PostgreSQL, JetStream, Centrifugo; React/TypeScript, Docusaurus where relevant.

**Spec:** [SPEC](../../SPEC.md), [API](../../API_CONTRACT.md), [operations](../../OPERATIONS.md).

**Dependencies:** Depends every MVP package; no fake verification from docs completion.

## Global constraints

Paths in this plan are repo-root-relative. Commands run from `src/` and are prefixed with `rtk`; pnpm scripts and integration harness are introduced by F1/F4 and the relevant task before invocation. Use real dependencies for contract tests. Update runtime OpenAPI only as endpoints are implemented. All tasks end with scoped commit after tests; do not stage unrelated files.

## D1 — Production Compose, ingress và monitoring

**Files:** src/deploy/compose.yml; src/deploy/edge-routing.conf; src/deploy/README.md; src/tests/e2e/routes.spec.ts.

**Interfaces:** One domain; public /docs/ before protected dashboard catch-all; API and WSS public auth, private admin upstream; sanitized configuration.

**Implementation decisions:** Inventory actual existing edge config; record rollback diff. No speculative host routes or public host ports for broker.

**Acceptance cases:** /docs/raw returns text; admin public denied with spoofed IP header; websocket Upgrade passes; API404 stays JSON; NATS/DB not reachable public; metrics redact tokens.

- [ ] Write tests named for the acceptance cases; each test must fail because the required behavior is missing. Capture actual versus expected HTTP/state transitions.
- [ ] Run RED: `rtk docker compose -f deploy/compose.yml config --quiet && rtk pnpm exec playwright test tests/e2e/routes.spec.ts`; verify expected failure, not unavailable test infrastructure.
- [ ] Implement the specified interfaces in the listed files, following ENGINEERING_DETAILS; keep immutable API/project boundaries.
- [ ] Run GREEN: `rtk docker compose -f deploy/compose.yml config --quiet && rtk pnpm exec playwright test tests/e2e/routes.spec.ts`; confirm acceptance cases and related regression tests pass.
- [ ] Reconcile internal/public docs: OPERATIONS.md; public-docs/docs/status-and-limits.md; update raw/agent exports and relevant skills or record why no skill changes apply.
- [ ] Review changed files, record evidence in STATUS and commit only D1 files.

## D2 — Fault, load và restore drill

**Files:** src/tests/e2e/faults.spec.ts; src/scripts/backup.sh; src/scripts/restore.sh; planning/VALIDATION_REPORT.md.

**Additional verification:** Create and execute `src/scripts/test-restore.sh` against disposable volumes and `src/scripts/test-load.sh --duration 30m` against the isolated candidate. Run `rtk pnpm exec playwright test tests/e2e/faults.spec.ts`; Go tests alone do not satisfy this task. Record commands, measured RPO/RTO and report paths.

**Interfaces:** Full matrix in TEST_MATRIX; backups independent of VPS, RPO/RTO measured; real capacity trial.

**Implementation decisions:** Record failures with evidence; restore production data only in protected test environment, publish summaries not credentials/payloads.

**Acceptance cases:** Kill publisher/worker/API/broker at defined boundaries; restore DB into isolated stack; no terminal replay; 30m load reports latency/RAM/disk; cross-project remains blocked.

- [ ] Write tests named for the acceptance cases; each test must fail because the required behavior is missing. Capture actual versus expected HTTP/state transitions.
- [ ] Run RED: `rtk go test -race ./... && rtk go test -tags=integration ./tests/integration -count=1`; verify expected failure, not unavailable test infrastructure.
- [ ] Implement the specified interfaces in the listed files, following ENGINEERING_DETAILS; keep immutable API/project boundaries.
- [ ] Run GREEN: `rtk go test -race ./... && rtk go test -tags=integration ./tests/integration -count=1`; confirm acceptance cases and related regression tests pass.
- [ ] Reconcile internal/public docs: OPERATIONS.md; public-docs/docs/reliability.md; update raw/agent exports and relevant skills or record why no skill changes apply.
- [ ] Review changed files, record evidence in STATUS and commit only D2 files.

## D3 — Release review và staged rollout

**Files:** planning/RELEASE_CHECKLIST.md; planning/STATUS.md; public-docs/docs/changelog.md.

**Interfaces:** Release version, image digests, migration compatibility, rollback and public docs/skills availability aligned.

**Implementation decisions:** Ship only on release authorization; preserve volume data, no destructive down -v. Mark MVP complete only after real integration and operational gates pass.

**Acceptance cases:** Final candidate tests pass; DNS/TLS smoke on provider plus existing-domain regressions; rollback rehearsal; unreleased features absent from supported matrix.

- [ ] Write tests named for the acceptance cases; each test must fail because the required behavior is missing. Capture actual versus expected HTTP/state transitions.
- [ ] Run RED: `rtk pnpm exec playwright test tests/e2e`; verify expected failure, not unavailable test infrastructure.
- [ ] Implement the specified interfaces in the listed files, following ENGINEERING_DETAILS; keep immutable API/project boundaries.
- [ ] Run GREEN: `rtk pnpm exec playwright test tests/e2e`; confirm acceptance cases and related regression tests pass.
- [ ] Reconcile internal/public docs: README.md; public-docs/docs/changelog.md; update raw/agent exports and relevant skills or record why no skill changes apply.
- [ ] Review changed files, record evidence in STATUS and commit only D3 files.
