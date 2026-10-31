# SDD ledger — plan: docs/superpowers/plans/2026-10-31-relayhub-full-migration.md

Base: 14be5fc docs: record RelayHub release deployment

Pre-flight: Phase 1 produces the route/data contract manifest consumed by the Rust parity implementation, UI port, migration tool, and cutover verifier. The UI consumes the canonical API shapes produced by Phase 2. The migration verifier consumes schema/version markers produced by Phase 1 and Phase 4. These interfaces are intentionally ordered; no implementation task may bypass the manifest or verifier.

Ruling: Execute in an isolated worktree branched from the released main commit; preserve the dirty user worktree and keep the Go production stack running until the final soak succeeds.

Task 1: complete (commit pending) — route manifest covers 159 registered Go/Rust routes; migration-status endpoint is admin-only and redaction-tested. Tests: `python3 scripts/check-route-parity.py && PYTHONPATH=scripts python3 scripts/test_route_parity.py` → PASS; `cargo test --manifest-path rust/Cargo.toml --test identity migration_status_is_admin_only_and_reports_versions_without_secrets -- --exact --nocapture` → PASS.

Task 2a: complete — Rust now serves the full Admin session cookie/CSRF contract, admin user list/create/update/disable/password rotation, and migration status; the full `web/admin` artifact has a production Nginx image definition. Tests: `cargo test --manifest-path rust/Cargo.toml --test identity -- --nocapture` → 14 passed; `npm --prefix web/admin typecheck/test/build` → PASS; route parity → 161 routes, PASS.

Task 2b: in_progress — app inventory adapters and migration ledger are implemented; full event/queue/realtime feature mapping remains blocking for cutover. Rust now has durable function/routing schemas and Admin CRUD adapters; credential compatibility, stable IDs, queue/realtime mapping and full data import remain blocking. Tests: Rust targeted control-plane test PASS; Go migration packages pass.

Task 2c: in_progress — mount Rust-owned Admin dashboard/metrics/event detail/timeline/DLQ/audit read projections and replace the old minimal Workspace UI artifact in the production stack. Rust Admin accepts both X-RelayHub-CSRF (existing SPA) and x-csrf-token. Full UI artifact is built but not deployed; Go remains rollback.

Task 2d: in_progress — Rust Admin functions/routing CRUD is mounted on schema versions 8-9. Function invocation is intentionally failed closed until the Rust function worker protocol is migrated. Queue configuration is mounted on schema version 10; pull/settle/lease paths are degraded until the queue worker protocol is migrated. Realtime Studio token/publish and Admin route adapters are mounted; connection index, history, file and push stores remain degraded. Full parity and data migration are still required before cutover.
Ruling: Preserve Go signed-client compatibility before any cutover — Go credentials use hex SHA-256 API-key hashes and `rhsec:v1` AES-GCM HMAC secrets, so Rust now stores the legacy fields, verifies the canonical HMAC request, and rejects Admin mutations from read-only users. This does not yet import production credentials or make the production secret available; Go remains rollback.

Task 2e: in_progress — Rust credential compatibility slice added with migration version 11, signed `/api/v1/events` integration coverage, and Admin read-only mutation authorization coverage. Remaining: import Go credentials/crosswalk, enforce production encryption-key startup, and migrate the function/queue/realtime data planes.

Deployment checkpoint (2026-10-31): full Admin UI is public on `relayhub.dungxbuif.com/admin/`; Rust API image `relayhub-v1:85f0559` is healthy on VM100 with the Go-compatible encryption key mounted as Docker Secret. Local and public OpenAPI health checks return 200. `relayhub-api` and `relayhub-worker` Go services remain running as rollback; no legacy cutover has been declared. GitLab registry push remains blocked by missing Docker registry credentials, so the Rust images were transferred to VM100 directly.

Task 2f: in_progress — schema version 12 adds legacy crosswalks and preserves the full control-plane configuration shape. The Go migration tool now imports active credentials (retaining Go `rhsec:v1` ciphertext), functions, routing rules, queue subscriptions, and schedules into Rust UUID rows and reports those counts during verify. Event/delivery runtime state, audit history, realtime files/push, function invocation history, and queue delivery state remain blocked until their data-plane translations and workers are implemented.
