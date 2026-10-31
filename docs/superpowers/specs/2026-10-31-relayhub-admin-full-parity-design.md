# RelayHub Admin Full-Parity Console — Design

## Intent

Make `/admin` the complete, trustworthy control surface for RelayHub. An operator should be able to understand service health, manage applications and access, inspect delivery outcomes, configure routing and queues, exercise realtime behavior, and perform supported recovery actions without leaving the console or handling application signing secrets in browser code.

The current Admin SPA already has real pages for Overview, Events, Dead Letters, Audit Logs, Apps, Routing Rules, Queue, and Realtime Studio. `System` is still a placeholder, and several API capabilities have no usable Admin workflow. This design completes the product around the existing contracts and adds narrowly scoped Admin projections where an app-signed client API cannot be safely called from a browser.

For this release, “full parity” means the complete current OpenAPI v1/v2 surface plus every existing Admin endpoint that has a meaningful console workflow. It does not include deployment or infrastructure mutation.

## Goals

- Cover every supported operational capability exposed by the current HTTP/message contracts with a discoverable Admin workflow.
- Replace placeholder routes with real data, loading, empty, error and permission states.
- Keep application API keys, HMAC secrets, push tokens, upload URLs and socket tokens out of browser state, logs and URLs unless a one-shot credential dialog explicitly displays a newly issued application credential.
- Make destructive or high-impact actions explicit, idempotent where the backend supports it, and auditable.
- Give the operator a safe end-to-end path for each major capability: provision, observe, diagnose, operate, and verify.
- Keep the console usable on desktop and narrow screens, with keyboard navigation and accessible status/error feedback.

## Non-goals

- Editing deployment files, infrastructure settings, databases or provider credentials from the browser.
- Exposing raw logs, payloads, secrets or unbounded metrics.
- Replacing the public SDKs or making the Admin console a generic API explorer.
- Inventing UI for capabilities that the backend cannot authorize or return safely; those require a documented backend Admin projection first.
- Deploying to production as part of the design work. Production rollout remains a separate release gate after automated and connected-environment verification.

## Anti-goals

- Never make the browser sign requests with an app API key or HMAC secret.
- Never cache credentials, push tokens, presigned URLs or socket tokens in React Query, local storage, URL query strings or analytics.
- Never provide an unlabeled “reset”, “delete all” or “replay all” action.
- Never show fabricated sample metrics when a dependency is unavailable.

## Approaches considered

### 1. Frontend-only completion

Add pages that call existing app APIs from the browser. This is the smallest diff, but it requires handling app credentials in the browser and cannot safely cover admin-only projections or app-signed operations. Rejected for the security boundary.

### 2. Full parity with a server-side Admin facade — selected

Keep the current Admin session and add narrowly scoped `/api/v1/admin/...` read and mutation endpoints for operations that need privileged aggregation or app ownership context. The SPA calls only the Admin session API; the backend performs authorization, redaction, idempotency and audit logging. This matches the existing Admin architecture, keeps secrets server-side, and lets the console cover all supported product capabilities.

### 3. Separate operator console and product console

Split health/recovery from application configuration into two products. This gives a smaller navigation surface per role but duplicates auth, routing, tests and release work. It also makes the requested “full feature” workflow harder to discover. Rejected for this release; role-aware navigation can be added later without splitting the application.

## Release-scope decisions

These decisions resolve the builder check and are the default acceptance criteria for implementation:

- Admin roles use the existing `admin` and `user` values. `admin` may perform all supported control-plane mutations; `user` can authenticate and read only the pages and data explicitly permitted by the backend. The UI hides unavailable mutations, while the backend remains authoritative.
- Admin users use the existing local password store in this release. The console adds list/create/role/enable state and password rotation/reset workflows with session revocation where supported; external identity providers are out of scope.
- App credential rotation is immediate and atomic: the old API key/HMAC secret is invalid as soon as the new pair is committed. The new pair is displayed once; if it is missed, the operator rotates again and the previous pair remains unrecoverable.
- Event details show bounded application payloads to an authorized Admin, but credentials, signatures, push tokens, upload URLs, raw callback secrets and unbounded metadata are always redacted or omitted.
- File parity includes bounded metadata and an authorized streaming download through the Admin session. The browser never receives a reusable presigned URL, and size/content authorization follows the existing service limits.
- Queue replay, drain, delete, lease and schedule behavior follows existing service state machines and generation/idempotency rules; the UI must display partial failures returned by the backend rather than emulate transitions locally.
- Function invocation is treated as a synchronous, bounded request using the existing function timeout/deadline semantics. The UI shows success, handler error, unavailable and timeout distinctly and never retries automatically.
- System health uses the existing dashboard/metrics aggregation plus `healthz`/`readyz` semantics. The UI labels a component degraded only from backend-provided status, never from a frontend guess.

## Chosen product shape

The console uses grouped navigation and consistent page primitives:

### Control and access

- Overview: current health, readiness, degraded components, active connections, durable counts and bounded time-series metrics.
- System: dependency/replica status, health/readiness details, metric window/step controls, degraded-state explanation and safe refresh; no infrastructure mutation.
- Apps: create, edit, disable, rotate credentials, inspect delivery mode and open an app detail workspace. Credentials remain one-shot.
- Admin users: create users, assign supported roles, enable/disable accounts, and show the current session/access boundary. Passwords are write-only and never listed.

### Delivery and recovery

- Events: bounded search/filter, event detail, target/delivery summary and lifecycle timeline.
- Dead Letters: filter, detail, attempt history, bounded multi-select replay, export and any backend-supported delete action behind explicit confirmation.
- Routing Rules: create, edit, enable/disable and delete with app labels instead of opaque IDs wherever possible.
- Audit Logs: cursor-paginated filters with resource links and redacted metadata.

### Durable work

- Queue workspace: select an app and subscription, inspect policy/depth/callbacks, create/edit/delete subscriptions, pause/resume, pull a bounded batch, settle or extend leases with explicit JSON input, drain with confirmation, and inspect/export/replay/delete dead letters.
- Schedules: list, create, edit, enable/disable and delete schedules; show next/last run and validation errors.
- Every queue mutation refreshes affected queries and shows the resulting state, not merely a success toast.

### Realtime

- Realtime Studio: retain v2, legacy realtime and durable stream connections; add structured history/rewind, presence, message actions, file metadata and push exercises where the contract supports them.
- Connections: inspect app-scoped live connections and disconnect a selected connection with a reason/confirmation.
- Realtime resources: browse only bounded, redacted file/push metadata through Admin projections; never display raw upload URLs or device tokens after creation.

### Functions

- Functions: choose an app, list/register/delete owner-scoped functions, configure timeout/enabled state where supported, invoke with bounded JSON and display the invocation result/timeline without leaking credentials.

## Backend/Admin facade contract

Existing Admin endpoints remain the source for dashboard, events, DLQ, audit, studio, connections and queue read views. New endpoints are added only where full parity needs a safe server-side projection:

- Admin user list/update/disable or password-rotation operations, with role and enabled state only.
- App-scoped functions list/register/update/delete/invoke through Admin authority.
- App-scoped subscription CRUD and lifecycle operations, schedules, bounded pull/settle/lease extension and DLQ export/replay/delete.
- Realtime history query, file metadata/download proxy and push-device metadata operations with response redaction.
- Any new mutation must define its idempotency behavior, audit action, authorization rule, bounded input/output and error mapping in the OpenAPI contract before the UI uses it.

The backend must not accept an app credential from the Admin browser as a workaround. Admin handlers resolve the selected app on the server, enforce enabled/ownership rules, invoke the existing service layer, and redact secrets. New Admin routes use the existing Admin session plus CSRF protection and are included in route-manifest/auth tests.

## State, safety and UX rules

- Use React Query for server state with query keys scoped by app, subscription and filters; abort stale requests and pause polling when the tab is hidden.
- Use a shared mutation result pattern: pending state, inline error, audit/idempotency identifier where applicable, then targeted invalidation and a refreshed result.
- Use confirmation dialogs for disable, rotate, drain, delete, replay, disconnect, settle and lease extension. Show exact target/count and consequences.
- Validate JSON, cron, URL, channel, IDs, limits and cursor values before submission; server validation remains authoritative.
- Keep payloads bounded and redacted in tables. Details can show application data only when the authenticated Admin contract explicitly allows it.
- On `401`, clear the session and return to login. On `403`, explain the missing Admin permission without retry loops. On `409`/`503`, preserve user input and present recovery guidance.
- Navigation labels must map to working pages; no `FeatureBoundaryPage` remains for an advertised feature.

## Testing and release gates

- Frontend unit tests for every API adapter, redaction rule, form validation, mutation confirmation and query invalidation path.
- Component tests for loading/empty/error/success states and keyboard/focus behavior on every page.
- Playwright journeys covering login/logout, app provisioning/rotation, event investigation, DLQ replay, routing, queue lifecycle, schedule editing, realtime studio, functions, Admin user management and system degradation states.
- Backend tests for every new Admin route: session/CSRF/auth boundary, app selection/ownership, redaction, idempotency, audit record and bounded input/output.
- Contract tests keep the Admin TypeScript types and OpenAPI in sync; `check-docs.py`, route parity, generated embed and Admin build remain required.
- Run the full backend, Go SDK, TypeScript SDK, Admin and docs suites before a branch release. Connected-environment smoke tests may run only after the local gates pass and must not turn production into an unreviewed test target.

## Open implementation calls

- Use the existing visual language, tables, dialogs and query hooks; do not add a UI framework or state library.
- Prefer adding server-side projections over exposing app credentials.
- Implement in slices ordered by risk: backend facade/auth contract, access/system, durable work/functions, realtime resources, then cross-page polish and Playwright coverage.
