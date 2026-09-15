# Dashboard, reference app và Skills — Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans task-by-task. All implementation checkboxes start unchecked.

**Goal:** Dashboard, reference app và Skills.

**Architecture:** See [engineering contract](../ENGINEERING_DETAILS.md); app-facing protocols remain HTTPS/WSS, broker private.

**Tech Stack:** Go, PostgreSQL, JetStream, Centrifugo; React/TypeScript, Docusaurus where relevant.

**Spec:** [SPEC](../../SPEC.md), [API](../../API_CONTRACT.md), [operations](../../OPERATIONS.md).

**Dependencies:** Depends F4, J4 and R3; public artifacts only claim implemented behavior.

## Global constraints

Paths in this plan are repo-root-relative. Commands run from `src/` and are prefixed with `rtk`; pnpm scripts and integration harness are introduced by F1/F4 and the relevant task before invocation. Use real dependencies for contract tests. Update runtime OpenAPI only as endpoints are implemented. All tasks end with scoped commit after tests; do not stage unrelated files.

## P1 — Admin dashboard

**Files:** src/web/package.json; src/web/src/pages/Login.tsx; src/web/src/pages/Projects.tsx; src/web/src/pages/Jobs.tsx; src/web/src/pages/JobDetail.tsx.

**Interfaces:** Uses admin session APIs and project-prefixed reads; never stores project backend key in browser.

**Implementation decisions:** Shared requestId on errors, loading/empty/failed states; no full payload logging.

**Acceptance cases:** Login/CSRF/logout; list cursor; accepted≠queued UI; read failure shown; replay needs confirmation and new jobId; inspect keys page has metadata only.

- [ ] Write tests named for the acceptance cases; each test must fail because the required behavior is missing. Capture actual versus expected HTTP/state transitions.
- [ ] Run RED: `rtk pnpm exec playwright test tests/e2e/dashboard.spec.ts`; verify expected failure, not unavailable test infrastructure.
- [ ] Implement the specified interfaces in the listed files, following ENGINEERING_DETAILS; keep immutable API/project boundaries.
- [ ] Run GREEN: `rtk pnpm exec playwright test tests/e2e/dashboard.spec.ts`; confirm acceptance cases and related regression tests pass.
- [ ] Reconcile internal/public docs: public-docs/docs/guides/project-setup.md; src/web/README.md; update raw/agent exports and relevant skills or record why no skill changes apply.
- [ ] Review changed files, record evidence in STATUS and commit only P1 files.

## P2 — App mẫu queue + realtime và second-project isolation

**Files:** src/examples/queue-realtime/backend.ts; src/examples/queue-realtime/worker.ts; src/examples/second-app/index.ts; src/tests/e2e/queue-realtime.spec.ts.

**Interfaces:** App DB + outbox publisher; worker storage idempotency; browser subscribe then snapshot with version.

**Implementation decisions:** Self-contained sample handler with deterministic results and app-owned persistence; test against real RelayHub/PostgreSQL/NATS/Centrifugo. No external business app adapter, engine or repository changes are required. External apps integrate after provider completion.

**Acceptance cases:** Enqueue→progress→persisted result; close browser and resume; kill worker after side effect then retry does not duplicate fixture effect; project-B cannot see job.

- [ ] Write tests named for the acceptance cases; each test must fail because the required behavior is missing. Capture actual versus expected HTTP/state transitions.
- [ ] Run RED: `rtk pnpm exec playwright test tests/e2e/queue-realtime.spec.ts`; verify expected failure, not unavailable test infrastructure.
- [ ] Implement the specified interfaces in the listed files, following ENGINEERING_DETAILS; keep immutable API/project boundaries.
- [ ] Run GREEN: `rtk pnpm exec playwright test tests/e2e/queue-realtime.spec.ts`; confirm acceptance cases and related regression tests pass.
- [ ] Reconcile internal/public docs: public-docs/docs/guides/queue-realtime-example.md; INTEGRATION_FLOWS.md; update raw/agent exports and relevant skills or record why no skill changes apply.
- [ ] Review changed files, record evidence in STATUS and commit only P2 files.

## P3 — Skills tab, downloads và agent exports

**Files:** public-docs/skill-packages/realtime-integration/SKILL.md; public-docs/skill-packages/queue-worker/SKILL.md; public-docs/scripts/export-resources.mjs; public-docs/docs/skills/index.md.

**Interfaces:** One canonical skill package→copy/raw/ZIP/checksum. Generate llms.txt/full, raw pages, resources.json and versioned OpenAPI.

**Implementation decisions:** Follow skill-creator when authoring actual packages; unsupported clients get manual install guidance; use publish allowlist.

**Acceptance cases:** Copy equals source; ZIP includes all relative references; no absolute path/traversal archive entries; checksums stable; HTTP fetch works without JS/login; planned API excluded from runnable quickstarts.

- [ ] Write tests named for the acceptance cases; each test must fail because the required behavior is missing. Capture actual versus expected HTTP/state transitions.
- [ ] Run RED: `rtk pnpm --dir ../public-docs test && rtk pnpm --dir ../public-docs build`; verify expected failure, not unavailable test infrastructure.
- [ ] Implement the specified interfaces in the listed files, following ENGINEERING_DETAILS; keep immutable API/project boundaries.
- [ ] Run GREEN: `rtk pnpm --dir ../public-docs test && rtk pnpm --dir ../public-docs build`; confirm acceptance cases and related regression tests pass.
- [ ] Reconcile internal/public docs: DOCUMENTATION_STRATEGY.md; public-docs/docs/skills/index.md; update raw/agent exports and relevant skills or record why no skill changes apply.
- [ ] Review changed files, record evidence in STATUS and commit only P3 files.
