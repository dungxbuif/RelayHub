# RelayHub full migration implementation plan

Date: 2026-10-31

## Objective

Migrate the complete RelayHub product from the legacy Go runtime plus minimal Rust Workspace UI to the Rust runtime plus the full Admin UI, preserving data and behavior. Verify in repeated loops against production-like and live-safe checks, cut public traffic over, then stop the old app without deleting rollback material.

## Constraints and safety

- Do not touch the dirty user worktree at `/home/dungxbuif/workspace/projects/RelayHub`; use an isolated worktree for implementation.
- Do not merge `main` into feature work. Push the migration branch to both remotes and integrate only after gates pass.
- Do not expose or commit passwords, API keys, cookies, session tokens, database URLs or backup contents.
- Take and verify a fresh database backup before every migration rehearsal and before final cutover.
- Keep the Go service and database available as rollback until the post-cutover soak is complete.
- Every migration step must be idempotent, resumable and produce counts/checksums.

## Phase 1 — freeze the contract and inventory

1. Add a machine-readable parity manifest derived from the registered Go routes and the Rust routes. Include method, path, auth mode, request/response fixture and owning feature.
2. Inventory the full Admin UI routes, pages, navigation, API helpers and browser acceptance tests.
3. Inventory Go and Rust schemas, background workers, brokers, object-store references, environment variables and secrets. Record which tables are source-of-truth during each phase.
4. Add a documented migration status endpoint/report that returns build, schema, feature and data-migration versions without secrets.

Verification: route manifest has no unclassified route; every existing Admin page has an owner and a Rust API target; schema report is checked into test artifacts.

## Phase 2 — build the Rust control-plane parity layer

1. Implement Rust admin session compatibility, admin users, CSRF/session expiry and one-time password setup/reset. Support the existing admin email without copying an incompatible password hash.
2. Implement the control-plane APIs used by the full UI: dashboard/metrics, events/timeline, DLQ/replay, audit, connections, apps/credentials, callbacks/subscriptions, functions/invoke, routing rules, queue v2, realtime files/push/studio.
3. Preserve existing Rust identity/workspace/agent routes as compatibility aliases, with shared authorization and audit code.
4. Make all writes idempotent where the old contract is retryable and add transaction/lease fencing for queue and delivery operations.
5. Add unit, integration, contract and failure tests for each route family, including unauthorized, CSRF, replay, pagination, lease expiry and duplicate request cases.

Verification: the parity manifest is green; each route has a real Rust handler and test; no route is backed by a proxy to the old Go service.

## Phase 3 — migrate the full UI

1. Port `web/admin` into the Rust public image and serve it at `/admin`.
2. Preserve the full Admin navigation and pages: Overview, Events, Dead Letters, Queue, Apps, Routing Rules, Realtime Studio, Audit Logs and System.
3. Replace any Go-specific assumptions with the Rust canonical API while retaining response shapes or adding a tested adapter at the Rust boundary.
4. Keep the minimal Workspace UI only as a compatibility route until its capabilities are covered by the full UI; then redirect or remove it in a separately verified change.
5. Run typecheck, build, accessibility/browser acceptance and a page-by-page authenticated smoke test.

Verification: an admin can sign in, see every old page, inspect existing data, and complete every old write workflow in a disposable environment.

## Phase 4 — migrate data safely

1. Write `tools/relayhub-migrate` (or equivalent existing tool) with `plan`, `apply`, `verify` and `rollback-report` modes.
2. Migrate admin users/workspaces/apps/credentials/callbacks/functions/routing rules/queue subscriptions/schedules/realtime resources/audit records with stable-ID maps.
3. Import events, deliveries, attempts, outbox, DLQ and function invocations with explicit state translation. Reject unsafe state transitions instead of guessing.
4. Handle credential/password material through a setup/reset flow; revoke old sessions and record the migration event.
5. Reconcile row counts, per-feature checksums, referential integrity, active secret counts, queue depth and non-terminal work. Emit a signed-off report with source/target database identifiers redacted.
6. Rehearse restore into a disposable database and rerun the entire verification suite.

Verification: two independent rehearsals produce the same report; restore is readable; no data-loss or orphan check fails.

## Phase 5 — production cutover loop

1. Build and tag a release image with immutable commit and schema versions; push to the shared lab registry.
2. Deploy the candidate beside the old stack with a separate namespace and no public traffic.
3. Run the complete acceptance loop: health/readiness, auth/setup, app and credential lifecycle, event idempotency, webhook delivery/retry/replay, queue pull/settle/lease/drain/schedule, functions, routing, realtime websocket/history/files/push, audit/DLQ, docs/SDK artifacts, restart/recovery and browser UI.
4. Run live-safe read-only parity and synthetic writes using disposable IDs; verify cleanup and no unexpected old-writer activity.
5. Freeze old writes, take the final backup, apply the final delta, verify again, switch the public router, and run the soak checklist.
6. Stop the old Go containers/workers only after the soak passes. Keep the old image, database backup, migration report and one-command rollback documented.

Verification: attach command output and timestamps to the release record; any red gate means rollback, not a partial “done”.

## Phase 6 — documentation and release

1. Update product docs, deployment docs, data migration runbook, rollback runbook, admin UI guide and service-specific context in the homelab vault.
2. Document every new environment variable, secret mapping, migration version, backup/restore step and known operational trap without storing secret values.
3. Commit the implementation and docs with focused commits, push the feature branch to GitHub and GitLab, request review, then integrate the verified release.
4. Record the final public image tag, route owner, schema version, old-app stop time and rollback expiry date.

## Review focus

- accidental data loss or duplicated delivery;
- password/credential/session handling;
- auth and CSRF regression;
- queue lease and replay correctness;
- public route accidentally still serving Go;
- UI capability missing behind a placeholder;
- migration scripts that are not restart-safe;
- claims of completion without fresh evidence.
