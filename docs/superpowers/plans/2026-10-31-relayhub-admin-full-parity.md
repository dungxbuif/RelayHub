# RelayHub Admin Full-Parity Console Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the RelayHub Admin console so every current OpenAPI v1/v2 capability and existing Admin operation has a safe, tested workflow without exposing application credentials to the browser.

**Architecture:** Keep the existing React/Vite Admin SPA and Admin-session/CSRF boundary. Add explicit server-side Admin facade routes for app-scoped capabilities that currently require app-signed credentials, then build typed React Query pages and shared mutation/permission primitives on top. The backend remains authoritative for authorization, redaction, idempotency, state transitions and audit records.

**Tech Stack:** Go HTTP handlers/services/store interfaces, OpenAPI 3.1 static contract, React 18 + TypeScript + React Router + TanStack Query, Vitest, Playwright, existing Docusaurus/embed and route-manifest checks.

**Spec:** `docs/superpowers/specs/2026-10-31-relayhub-admin-full-parity-design.md`

## Global Constraints

- “Full parity” means the complete current OpenAPI v1/v2 surface plus every existing Admin endpoint with a meaningful console workflow; it excludes deployment and infrastructure mutation.
- The browser must never sign requests with an app API key or HMAC secret.
- Credentials, push tokens, presigned URLs and socket tokens must not enter React Query, local storage, URL query strings, logs or analytics.
- `admin` may perform supported control-plane mutations; `user` is read-only wherever the backend permits the role.
- App credential rotation is atomic and immediately invalidates the old credential pair.
- Event payloads are bounded; credentials, signatures, push tokens, upload URLs, callback secrets and unbounded metadata are redacted or omitted.
- Destructive actions require explicit confirmation and must be idempotent/audited where the backend supports it.
- No new UI framework or state library; reuse existing tables, dialogs, query hooks and visual tokens.
- Every new Admin route must be represented in OpenAPI, route-manifest/auth tests and backend redaction/idempotency tests.

## Review Focus

- A read-only `user` session must never reach an Admin mutation even if the UI route is called directly — covered in Task 1 backend auth tests and Task 9 Playwright permission journey.
- A rotated credential, push token, presigned URL or socket token must not appear in response bodies, React Query cache, URL, logs or error text — covered in Tasks 1, 2, 4 and 9 redaction tests.
- Queue replay/drain/settle/lease operations must preserve backend state-machine and generation/idempotency semantics under duplicate or partial requests — covered in Task 3 contract/service tests and Task 7 UI mutation tests.
- Large/invalid JSON, file downloads and event payloads must be bounded and fail safely without freezing or leaking data — covered in Tasks 2, 4 and 9 boundary tests.
- A dependency outage or stale session must produce an honest degraded/error state with no retry loop or fabricated data — covered in Tasks 1, 5 and 9 component/browser tests.

## File map

- `backend/internal/httpapi/admin_full_control.go`: explicit app-scoped Admin facade handlers and redacted response types.
- `backend/internal/httpapi/admin_users.go`, `backend/internal/httpapi/admin_session.go`, `backend/internal/httpapi/auth.go`: Admin role/session/user lifecycle enforcement.
- `backend/internal/httpapi/router.go`, `backend/internal/httpapi/routes.go`: facade route registration and route manifest categories.
- `backend/internal/service/*`, `backend/internal/store/store.go`, `backend/internal/store/postgres/*`: missing admin-safe service/store operations only where existing service contracts cannot be reused.
- `web/docs/static/openapi.json`, `web/docs/static/asyncapi.yaml`: canonical public contract updates; `backend/web/embed.go` regenerated afterward.
- `web/admin/src/api/adminReads.ts`, `web/admin/src/api/controlPlane.ts`, `web/admin/src/api/types.ts`: typed Admin API adapters and redaction-safe DTOs.
- `web/admin/src/auth/*`, `web/admin/src/layout/*`, `web/admin/src/components/*`: permission context, navigation, shared forms, tables, dialogs and mutation feedback.
- `web/admin/src/pages/*`: completed System, access, Functions, Queue, Realtime resource and existing-page enhancements.
- `web/admin/tests/*`, `web/admin/e2e/*`: unit/component and end-to-end journeys.
- `backend/internal/httpapi/*_test.go`, `backend/internal/service/*_test.go`: auth, facade, state-machine, audit and contract tests.
- `docs/superpowers/specs/2026-10-31-relayhub-admin-full-parity-design.md`: approved design authority.

### Task 1: Harden Admin roles, session and user lifecycle

**Files:**
- Modify: `backend/internal/service/admin_sessions.go`, `backend/internal/httpapi/auth.go`, `backend/internal/httpapi/admin_session.go`
- Modify: `backend/internal/httpapi/admin_users.go`, `backend/internal/store/store.go`, `backend/internal/store/postgres/admin_users.go`
- Modify: `backend/internal/httpapi/router.go`, `backend/internal/httpapi/routes.go`, `web/docs/static/openapi.json`
- Test: `backend/internal/httpapi/admin_session_test.go`, `backend/internal/httpapi/admin_password_session_test.go`, new `backend/internal/httpapi/admin_users_lifecycle_test.go`

**Interfaces:**
- Consumes: existing `AdminUserStore`, `AdminSessionService`, `AdminSessionCookie` and CSRF middleware.
- Produces: role-aware `AdminPrincipal`, explicit read/mutate authorization helpers, and bounded Admin user list/create/update/disable/password-rotation endpoints. Passwords remain write-only; responses contain only `id`, `email`, `role`, `enabled`, timestamps.

- [ ] **Step 1: Write failing tests** for user login as `user`, mutation rejection for `user`, Admin user list/update/disable/password rotation, self-disable protection, CSRF failure and session revocation after password rotation.
- [ ] **Step 2: Run the focused Go tests and verify failure** because the store/service methods and role middleware do not yet exist.
- [ ] **Step 3: Implement the smallest role-aware lifecycle** using the existing local password store; preserve `admin` full access, make `user` read-only, and ensure every mutation checks the principal server-side.
- [ ] **Step 4: Update OpenAPI and route manifest** with exact request/response schemas, role/error behavior and bounded password rules.
- [ ] **Step 5: Run focused tests and `go test ./internal/httpapi ./internal/service ./internal/store/postgres`**; expected: PASS with no secret text in responses/log fixtures.
- [ ] **Step 6: Commit** `feat: complete Admin access lifecycle`.

### Task 2: Add the safe app/function Admin facade

**Files:**
- Create: `backend/internal/httpapi/admin_full_control.go`
- Modify: `backend/internal/httpapi/router.go`, `backend/internal/httpapi/routes.go`, `backend/internal/service/functions.go`, `backend/internal/store/store.go`
- Modify: `web/docs/static/openapi.json`
- Test: new `backend/internal/httpapi/admin_full_control_test.go`, `backend/internal/service/functions_admin_test.go`

**Interfaces:**
- Consumes: Task 1 role checks, existing `AppService`, `FunctionService`, `AdminReadStore`, and Admin audit logging.
- Produces: explicit routes under `/api/v1/admin/apps/{appID}/functions` for list/register/update/delete/invoke and app detail reads; handlers resolve the app server-side and return redacted DTOs.

- [ ] **Step 1: Write failing tests** for admin list/register/update/delete/invoke, unknown/disabled app, read-only role, bounded JSON input, synchronous timeout/error outcomes, idempotency replay and audit records.
- [ ] **Step 2: Run the focused tests and verify failure** because Admin facade routes and service/store update methods are absent.
- [ ] **Step 3: Implement server-side facade handlers** that call existing services directly; never accept app API/HMAC credentials from the request, never return credentials, and preserve the function service’s timeout/deadline semantics.
- [ ] **Step 4: Add contract schemas and route categories** to OpenAPI and route-manifest tests, including 401/403/404/409/413/503/504 behavior.
- [ ] **Step 5: Run focused Go tests plus `go test ./internal/httpapi ./internal/service`**; expected: PASS.
- [ ] **Step 6: Commit** `feat: add safe Admin app and function control`.

### Task 3: Complete queue, schedule and delivery operations

**Files:**
- Create or modify: `backend/internal/httpapi/admin_queue.go`, `backend/internal/service/*queue*.go`, `backend/internal/store/store.go`
- Modify: `backend/internal/httpapi/router.go`, `backend/internal/httpapi/routes.go`, `web/docs/static/openapi.json`
- Test: `backend/internal/httpapi/admin_queue_test.go`, `backend/internal/service/*queue*_test.go`, existing DLQ lifecycle tests

**Interfaces:**
- Consumes: Task 1 authorization, Task 2 facade route conventions, existing queue service/store state machines and Admin audit writer.
- Produces: Admin-scoped subscription CRUD, pause/resume, bounded pull/settle/lease extension, metrics, drain/status, schedule CRUD, callback outcomes and DLQ export/replay/delete with cursor/limit bounds.

- [ ] **Step 1: Write failing tests** for every mutation and read, including duplicate idempotency keys, generation conflicts, partial batch results, disabled app, missing subscription, drain status and destructive confirmation contract.
- [ ] **Step 2: Run focused tests and verify failure** on missing routes/facade methods.
- [ ] **Step 3: Implement facade handlers by delegating to existing queue services**; do not recreate transitions in HTTP code. Return per-item outcomes for batch operations and preserve backend cursors.
- [ ] **Step 4: Add bounded request schemas and audit actions** to OpenAPI/route manifest.
- [ ] **Step 5: Run queue/DLQ focused tests and `go test ./internal/httpapi ./internal/service ./internal/store/postgres`**; expected: PASS.
- [ ] **Step 6: Commit** `feat: expose full queue operations to Admin`.

### Task 4: Complete realtime resource and connection Admin operations

**Files:**
- Create or modify: `backend/internal/httpapi/admin_realtime.go`
- Modify: `backend/internal/httpapi/router.go`, `backend/internal/httpapi/routes.go`, `backend/internal/service/*realtime*.go`, `backend/internal/store/store.go`
- Modify: `web/docs/static/openapi.json`, `web/docs/static/asyncapi.yaml`
- Test: `backend/internal/httpapi/admin_realtime_test.go`, existing realtime history/file/push tests

**Interfaces:**
- Consumes: Task 1 authorization and Task 2 facade DTO/error conventions; existing realtime hub, history, file and push services.
- Produces: bounded Admin history query, structured studio actions, connection detail/disconnect, redacted file metadata and streaming download, and push-device metadata/bind/unbind/notification operations.

- [ ] **Step 1: Write failing tests** for app scoping, connection disconnect, history limit/cursor, file download authorization/size, no presigned URL leakage, no device-token leakage, push unavailable and read-only denial.
- [ ] **Step 2: Run focused tests and verify failure** because the Admin projections are absent.
- [ ] **Step 3: Implement explicit facade routes** using server-side app context; stream file bytes through the authenticated Admin request when allowed and redact all token/URL fields.
- [ ] **Step 4: Update OpenAPI/AsyncAPI descriptions and route tests** with the new Admin contracts and capability limits.
- [ ] **Step 5: Run focused realtime tests and `go test ./internal/httpapi ./internal/realtime ./internal/service`**; expected: PASS.
- [ ] **Step 6: Commit** `feat: add realtime resource Admin controls`.

### Task 5: Build shared Admin permission, query and feedback primitives

**Files:**
- Modify: `web/admin/src/api/client.ts`, `web/admin/src/api/types.ts`, `web/admin/src/auth/AuthProvider.tsx`
- Create: `web/admin/src/auth/PermissionGate.tsx`, `web/admin/src/components/MutationFeedback.tsx`, `web/admin/src/components/JsonEditor.tsx`, `web/admin/src/components/ResourceSelector.tsx`
- Modify: `web/admin/src/components/ConfirmationDialog.tsx`, `web/admin/src/components/DataTable.tsx`, `web/admin/src/hooks/usePausableRefresh.ts`
- Test: `web/admin/tests/auth.test.tsx`, new `web/admin/tests/permission.test.tsx`, `web/admin/tests/components.test.tsx`

**Interfaces:**
- Consumes: Task 1 session payload (`AdminPrincipal` with `role`) and Task 2–4 typed Admin DTOs.
- Produces: `useAdminPrincipal()`, `canMutate()`, `PermissionGate`, `useAdminMutationFeedback()`, bounded JSON editor and safe resource selector used by all later pages.

- [ ] **Step 1: Write failing Vitest tests** for role gating, 401 invalidation, 403 non-retry behavior, redaction helper, JSON/size validation, confirmation focus trap and mutation invalidation feedback.
- [ ] **Step 2: Run focused tests and verify failure** on missing shared primitives.
- [ ] **Step 3: Implement primitives** with existing tokens/components and TanStack Query; keep sensitive values out of query keys/cache and clear transient mutation data after completion.
- [ ] **Step 4: Run focused tests plus `npm run typecheck` in `web/admin`**; expected: PASS.
- [ ] **Step 5: Commit** `feat: add Admin permission and mutation primitives`.

### Task 6: Replace placeholders and complete access/system/functions UI

**Files:**
- Create: `web/admin/src/pages/SystemPage.tsx`, `web/admin/src/pages/AdminUsersPage.tsx`, `web/admin/src/pages/FunctionsPage.tsx`
- Modify: `web/admin/src/app/App.tsx`, `web/admin/src/layout/Navigation.tsx`, `web/admin/src/api/adminReads.ts`, `web/admin/src/api/controlPlane.ts`, `web/admin/src/api/types.ts`
- Modify: `web/admin/src/pages/OverviewPage.tsx`, `web/admin/src/pages/AppsPage.tsx`
- Test: `web/admin/tests/system.test.tsx`, `web/admin/tests/admin-users.test.tsx`, `web/admin/tests/functions.test.tsx`, `web/admin/tests/navigation.test.tsx`

**Interfaces:**
- Consumes: Tasks 1, 2 and 5 APIs/primitives.
- Produces: working `/admin/system`, `/admin/users`, `/admin/functions` and app detail/function workflows with honest states, role-aware actions, bounded payload display and one-shot credentials.

- [ ] **Step 1: Write failing component tests** for system health/degraded state, Admin user lifecycle, function register/invoke/error/timeout and app credential rotation redaction.
- [ ] **Step 2: Run focused Vitest tests and verify failure** because routes/pages are placeholders or absent.
- [ ] **Step 3: Implement pages and navigation** using shared selectors/forms/tables; do not put secrets in component state beyond the one-shot dialog lifecycle.
- [ ] **Step 4: Run page tests, `npm run typecheck`, and `npm test`**; expected: PASS.
- [ ] **Step 5: Commit** `feat: complete Admin access system and functions UI`.

### Task 7: Complete events, delivery and queue UI

**Files:**
- Modify: `web/admin/src/api/adminReads.ts`, `web/admin/src/api/controlPlane.ts`, `web/admin/src/api/types.ts`
- Modify: `web/admin/src/pages/EventsPage.tsx`, `web/admin/src/pages/EventDetailPage.tsx`, `web/admin/src/pages/DeadLettersPage.tsx`, `web/admin/src/pages/DeadLetterDetailPage.tsx`, `web/admin/src/pages/QueuePage.tsx`, `web/admin/src/pages/RoutingRulesPage.tsx`, `web/admin/src/pages/AuditLogsPage.tsx`
- Test: existing page tests plus new `web/admin/tests/queue-lifecycle.test.tsx`, `web/admin/tests/event-boundaries.test.tsx`

**Interfaces:**
- Consumes: Tasks 3 and 5 typed facade methods and shared mutation/permission components.
- Produces: complete queue/subscription/schedule lifecycle UI, bounded event payload/timeline views, DLQ export/replay/delete, routing CRUD feedback and audit filters.

- [ ] **Step 1: Write failing tests** for cursor pagination, empty/error states, queue pause/resume/drain, schedule CRUD, pull/settle/extend JSON validation, partial replay/delete and payload redaction.
- [ ] **Step 2: Run focused tests and verify failure** on missing adapters/pages/actions.
- [ ] **Step 3: Implement the workflows** with exact server state refresh after every mutation and explicit target/count confirmation.
- [ ] **Step 4: Run all affected Vitest tests, typecheck and Admin build**; expected: PASS.
- [ ] **Step 5: Commit** `feat: complete Admin delivery and queue workflows`.

### Task 8: Complete realtime studio and resource UI

**Files:**
- Modify: `web/admin/src/api/controlPlane.ts`, `web/admin/src/api/types.ts`, `web/admin/src/pages/RealtimeStudioPage.tsx`, `web/admin/src/pages/realtimeStudioFrames.ts`
- Create: `web/admin/src/pages/RealtimeResourcesPage.tsx`
- Modify: `web/admin/src/app/App.tsx`, `web/admin/src/layout/Navigation.tsx`
- Test: `web/admin/tests/realtime-studio.test.ts`, new `web/admin/tests/realtime-resources.test.tsx`

**Interfaces:**
- Consumes: Task 4 realtime facade and Task 5 JSON/feedback primitives.
- Produces: structured history/rewind, presence, message actions, file metadata/download, push-device workflows and connection management with token-safe logs.

- [ ] **Step 1: Write failing tests** for each realtime protocol action, malformed JSON, bounded history, disconnect confirmation, file download redaction and push-token redaction.
- [ ] **Step 2: Run focused tests and verify failure** on missing resource adapters/actions.
- [ ] **Step 3: Implement the studio/resource flows** with connection cleanup, hidden-tab polling pause, explicit capability errors and no token logging.
- [ ] **Step 4: Run realtime tests, typecheck and build**; expected: PASS.
- [ ] **Step 5: Commit** `feat: complete Admin realtime controls`.

### Task 9: End-to-end journeys, contract synchronization and release gate

**Files:**
- Modify: `web/admin/e2e/admin-auth.spec.ts`, `web/admin/e2e/admin-dashboard.spec.ts`, `web/admin/e2e/admin-lifecycle.spec.ts`
- Create: `web/admin/e2e/admin-full-parity.spec.ts`
- Modify: `web/admin/playwright.config.ts`, `.github/workflows/ci.yml`, `backend/scripts/check-docs.py`
- Modify: `web/docs/static/openapi.json`, `backend/web/embed.go`, `web/admin/src/app/App.tsx`
- Delete: `web/admin/src/pages/FeatureBoundaryPage.tsx`
- Test: full `web/admin` Vitest/Playwright suite, backend route/contract suite, docs static checker

**Interfaces:**
- Consumes: all prior task routes, DTOs, pages and permissions.
- Produces: a release-blocking browser journey for login, access, apps, events, DLQ, routing, queue, functions, realtime and system degradation; no advertised placeholder route remains.

- [ ] **Step 1: Write failing Playwright journeys** against the existing test server fixtures for each feature group, including a read-only role and a dependency outage.
- [ ] **Step 2: Run Playwright and confirm failures** for each missing/full-parity journey.
- [ ] **Step 3: Implement only test-server fixtures and wiring needed to exercise real APIs**; do not add production-only bypasses.
- [ ] **Step 4: Synchronize OpenAPI, route manifest, generated embed and docs checker**; scan for secret/token leakage in built Admin assets and response fixtures.
- [ ] **Step 5: Run final verification:** `go test ./...` in `backend`; `go test ./...` in `sdks/go`; TypeScript SDK tests/typecheck; `npm run typecheck && npm test && npm run build` in `web/admin`; `npm --prefix web/docs run test:product-docs`; `.venv-docs/bin/python backend/scripts/check-docs.py --static --self-test`; `go -C backend generate ./web` followed by `git diff --exit-code -- backend/web`.
- [ ] **Step 6: Commit** `test: verify full Admin parity and release gates` and push the feature branch; do not merge `main`.

## Execution order and integration notes

Tasks 1–4 are backend contract slices and must land in order because later facade routes consume the role and DTO conventions established earlier. Tasks 5–8 are frontend slices consuming those stable endpoints. Task 9 is the final integration gate. After each task, run its focused tests and commit before moving on; any contract change must regenerate the embedded public/Admin assets before the next task consumes it.
