# Realtime compatibility và SDK — Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans task-by-task. All implementation checkboxes start unchecked.

**Goal:** Realtime compatibility và SDK.

**Architecture:** See [engineering contract](../ENGINEERING_DETAILS.md); app-facing protocols remain HTTPS/WSS, broker private.

**Tech Stack:** Go, PostgreSQL, JetStream, Centrifugo; React/TypeScript, Docusaurus where relevant.

**Spec:** [SPEC](../../SPEC.md), [API](../../API_CONTRACT.md), [operations](../../OPERATIONS.md).

**Dependencies:** Depends F2; progress needs J3–J4. Realtime publish can be built before queue recovery.

## Global constraints

Paths in this plan are repo-root-relative. Commands run from `src/` and are prefixed with `rtk`; pnpm scripts and integration harness are introduced by F1/F4 and the relevant task before invocation. Use real dependencies for contract tests. Update runtime OpenAPI only as endpoints are implemented. All tasks end with scoped commit after tests; do not stage unrelated files.

## R1 — Token, channel policy và Centrifugo integration

**Files:** src/internal/realtime/tokens.go; src/internal/realtime/channels.go; src/internal/realtime/client.go; src/deploy/centrifugo.json.

**Interfaces:** Session+grant API; grant returns logical channel and wireChannel; token subject project+user, TTL5m.

**Implementation decisions:** Verify OSS auth/connection quota hooks in dependency spike; document enforced limits accurately.

**Acceptance cases:** A token never authorizes B with same user ID; wrong channel grant rejected; connection token alone cannot subscribe private channel; publish failure→503.

- [ ] Write tests named for the acceptance cases; each test must fail because the required behavior is missing. Capture actual versus expected HTTP/state transitions.
- [ ] Run RED: `rtk go test -tags=integration ./tests/integration -run TestRealtimeAuthorization -count=1`; verify expected failure, not unavailable test infrastructure.
- [ ] Implement the specified interfaces in the listed files, following ENGINEERING_DETAILS; keep immutable API/project boundaries.
- [ ] Run GREEN: `rtk go test -tags=integration ./tests/integration -run TestRealtimeAuthorization -count=1`; confirm acceptance cases and related regression tests pass.
- [ ] Reconcile internal/public docs: API_CONTRACT.md; public-docs/docs/realtime/authentication.md; update raw/agent exports and relevant skills or record why no skill changes apply.
- [ ] Review changed files, record evidence in STATUS and commit only R1 files.

## R2 — SDK Centrifugo và raw WebSocket fixtures

**Files:** src/examples/realtime-native/index.ts; src/examples/realtime-raw/index.ts; src/tests/e2e/realtime.spec.ts; src/internal/jobs/progress.go.

**Interfaces:** Protocol from pinned Centrifugo; native client fixture uses wireChannel; worker progress locked to assigned channel.

**Implementation decisions:** No hand-written RelayHub JSON auth protocol; snapshot version resolves races. Real browser + real Centrifugo, not mock gateway.

**Acceptance cases:** Both native SDK and raw WebSocket receive publication; deny Socket.IO handshake; restart→resubscribe/snapshot; expiry refresh rechecks app permission; unauthorized progress blocked.

- [ ] Write tests named for the acceptance cases; each test must fail because the required behavior is missing. Capture actual versus expected HTTP/state transitions.
- [ ] Run RED: `rtk pnpm exec playwright test tests/e2e/realtime.spec.ts`; verify expected failure, not unavailable test infrastructure.
- [ ] Implement the specified interfaces in the listed files, following ENGINEERING_DETAILS; keep immutable API/project boundaries.
- [ ] Run GREEN: `rtk pnpm exec playwright test tests/e2e/realtime.spec.ts`; confirm acceptance cases and related regression tests pass.
- [ ] Reconcile internal/public docs: INTEGRATION_FLOWS.md; public-docs/docs/realtime/compatibility.md; update raw/agent exports and relevant skills or record why no skill changes apply.
- [ ] Review changed files, record evidence in STATUS and commit only R2 files.

## R3 — TypeScript server/worker SDK

**Files:** src/sdk/typescript/package.json; src/sdk/typescript/src/server.ts; src/sdk/typescript/src/worker.ts; src/sdk/typescript/src/errors.ts; src/sdk/typescript/test/worker.test.ts.

**Interfaces:** RelayServer.jobs.enqueue; RelayWorker.handle(queue,{version},handler); ctx.signal/progress; frontend export excludes credentials code.

**Implementation decisions:** Generate fresh eventId for progress; bounded retry with jitter and Retry-After; shutdown stops new claims and drains within deadline, then aborts handlers and stops heartbeats so the declared lease-expiry recovery takes over.

**Acceptance cases:** Network timeout preserves enqueue key; uncertain complete retries complete only; lost lease aborts signal; long-poll concurrency bounded; 401 never infinite retry; progress failure does not fail job.

- [ ] Write tests named for the acceptance cases; each test must fail because the required behavior is missing. Capture actual versus expected HTTP/state transitions.
- [ ] Run RED: `rtk pnpm --dir sdk/typescript test`; verify expected failure, not unavailable test infrastructure.
- [ ] Implement the specified interfaces in the listed files, following ENGINEERING_DETAILS; keep immutable API/project boundaries.
- [ ] Run GREEN: `rtk pnpm --dir sdk/typescript test`; confirm acceptance cases and related regression tests pass.
- [ ] Reconcile internal/public docs: src/sdk/typescript/README.md; public-docs/docs/sdks/typescript.md; update raw/agent exports and relevant skills or record why no skill changes apply.
- [ ] Review changed files, record evidence in STATUS and commit only R3 files.
