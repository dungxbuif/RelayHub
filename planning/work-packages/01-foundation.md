# Foundation, auth và documentation base — Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans task-by-task. All implementation checkboxes start unchecked.

**Goal:** Foundation, auth và documentation base.

**Architecture:** See [engineering contract](../ENGINEERING_DETAILS.md); app-facing protocols remain HTTPS/WSS, broker private.

**Tech Stack:** Go, PostgreSQL, JetStream, Centrifugo; React/TypeScript, Docusaurus where relevant.

**Spec:** [SPEC](../../SPEC.md), [API](../../API_CONTRACT.md), [operations](../../OPERATIONS.md).

**Dependencies:** Không phụ thuộc provider modules; source bootstrap đã có.

## Global constraints

Paths in this plan are repo-root-relative. Commands run from `src/` and are prefixed with `rtk`; pnpm scripts and integration harness are introduced by F1/F4 and the relevant task before invocation. Use real dependencies for contract tests. Update runtime OpenAPI only as endpoints are implemented. All tasks end with scoped commit after tests; do not stage unrelated files.

## F1 — Toolchain và local dependency harness

**Files:** src/deploy/compose.dev.yml; src/scripts/test-integration.sh; planning/DEPENDENCY_LOCK.md; src/internal/testutil/database.go.

**Interfaces:** Pinned PostgreSQL/NATS/Centrifugo, disposable DB schema per test, migrations command. Local ports chỉ bind loopback.

**Implementation decisions:** Readiness checks actual DB/schema; write sanitized capacity report independently of local setup.

**Acceptance cases:** Start isolated DB; insert row, destroy test schema and verify unrelated schema remains. Fail test if a dependency is silently skipped.

- [ ] Write tests named for the acceptance cases; each test must fail because the required behavior is missing. Capture actual versus expected HTTP/state transitions.
- [ ] Run RED: `rtk go test -tags=integration ./tests/integration -run TestHarness -count=1`; verify expected failure, not unavailable test infrastructure.
- [ ] Implement the specified interfaces in the listed files, following ENGINEERING_DETAILS; keep immutable API/project boundaries.
- [ ] Run GREEN: `rtk go test -tags=integration ./tests/integration -run TestHarness -count=1`; confirm acceptance cases and related regression tests pass.
- [ ] Reconcile internal/public docs: OPERATIONS.md; src/README.md; update raw/agent exports and relevant skills or record why no skill changes apply.
- [ ] Review changed files, record evidence in STATUS and commit only F1 files.

## F2 — Project schema, credentials và quotas

**Files:** src/migrations/001_identity.sql; src/migrations/002_resources.sql; src/internal/projects/keys.go; src/internal/projects/store.go; src/internal/httpapi/auth.go.

**Interfaces:** Principal{ProjectID,KeyID,Scopes}; Authenticate(ctx,key), RequireScope(principal,scope); exact contract in API_CONTRACT.

**Implementation decisions:** Use random key secret + verifier; strict ownership FK and unique queue names per project. No shared default keys.

**Acceptance cases:** Project A resource via B→404; missing key→401; wrong scope→403; revoke→401; one-time create returns plaintext key but list never does.

- [ ] Write tests named for the acceptance cases; each test must fail because the required behavior is missing. Capture actual versus expected HTTP/state transitions.
- [ ] Run RED: `rtk go test -tags=integration ./tests/integration -run TestProjectIsolation -count=1`; verify expected failure, not unavailable test infrastructure.
- [ ] Implement the specified interfaces in the listed files, following ENGINEERING_DETAILS; keep immutable API/project boundaries.
- [ ] Run GREEN: `rtk go test -tags=integration ./tests/integration -run TestProjectIsolation -count=1`; confirm acceptance cases and related regression tests pass.
- [ ] Reconcile internal/public docs: API_CONTRACT.md; public-docs/docs/authentication.md; update raw/agent exports and relevant skills or record why no skill changes apply.
- [ ] Review changed files, record evidence in STATUS and commit only F2 files.

## F3 — Admin listener, session và provisioning API

**Files:** src/internal/admin/session.go; src/internal/admin/handlers.go; src/internal/projects/resources.go; src/cmd/relayhub/main.go.

**Interfaces:** Two listeners: public excludes admin router; private includes session/provisioning/query routes in ENGINEERING_DETAILS.

**Implementation decisions:** Admin bootstrap reads password safely; sessions max8h; neither credential hash nor token in logs.

**Acceptance cases:** Public /admin prefix→404 including spoofed X-Forwarded-For; private login→cookie; mutation without CSRF→403; logout invalidates session; bad password rate-limited.

- [ ] Write tests named for the acceptance cases; each test must fail because the required behavior is missing. Capture actual versus expected HTTP/state transitions.
- [ ] Run RED: `rtk go test -tags=integration ./tests/integration -run TestAdminSession -count=1`; verify expected failure, not unavailable test infrastructure.
- [ ] Implement the specified interfaces in the listed files, following ENGINEERING_DETAILS; keep immutable API/project boundaries.
- [ ] Run GREEN: `rtk go test -tags=integration ./tests/integration -run TestAdminSession -count=1`; confirm acceptance cases and related regression tests pass.
- [ ] Reconcile internal/public docs: SYSTEM_DESIGN.md; public-docs/docs/projects.md; update raw/agent exports and relevant skills or record why no skill changes apply.
- [ ] Review changed files, record evidence in STATUS and commit only F3 files.

## F4 — Docusaurus base và current API reference

**Files:** public-docs/package.json; public-docs/docusaurus.config.ts; public-docs/sidebars.ts; public-docs/docs/intro.md; src/api/openapi.json.

**Interfaces:** Site /docs/, Markdown source; tabs Guides/API/SDKs/Skills/Changelog. Existing runtime OpenAPI expands only for implemented endpoints.

**Implementation decisions:** Pin Docusaurus lockfile; test JSX-free Markdown and /docs/ asset paths; empty Skills page says no packages released.

**Acceptance cases:** Build site; browse /docs/ nested page directly; external fetch gets page; target job API is labeled planned until implemented.

- [ ] Write tests named for the acceptance cases; each test must fail because the required behavior is missing. Capture actual versus expected HTTP/state transitions.
- [ ] Run RED: `rtk pnpm --dir ../public-docs build`; verify expected failure, not unavailable test infrastructure.
- [ ] Implement the specified interfaces in the listed files, following ENGINEERING_DETAILS; keep immutable API/project boundaries.
- [ ] Run GREEN: `rtk pnpm --dir ../public-docs build`; confirm acceptance cases and related regression tests pass.
- [ ] Reconcile internal/public docs: DOCUMENTATION_STRATEGY.md; public-docs/README.md; update raw/agent exports and relevant skills or record why no skill changes apply.
- [ ] Review changed files, record evidence in STATUS and commit only F4 files.
