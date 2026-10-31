# RelayHub full migration design

Date: 2026-10-31

## Problem

The public deployment currently runs the Rust v1 runtime and `web/remake-admin`, while the old Go runtime owns the complete Admin UI and its control-plane API. The Rust database and Go database are separate. The result is a partial migration: the public sign-in screen is real, but the existing Go admin user and the old UI/function set are not available in Rust.

The migration must preserve existing applications, credentials, events, deliveries, queue state, routing rules, functions, realtime resources, audit history, and admin access. The old runtime must remain available as rollback until the new path passes parity and production acceptance.

## Decision

Make the Rust runtime the single production control plane and migrate the full Go Admin surface into it. Reuse the existing full Admin UI contract where possible, but make the Rust service the only API and data owner. Keep the old Go stack isolated and read-only during the migration window; stop it only after cutover verification.

The target has one public origin, one Admin UI, and one authoritative Postgres database. NATS/Redis remain implementation details behind the service boundary. No public route or user-visible capability is removed as part of the migration.

## Compatibility boundary

The public contract to preserve is the registered Go route inventory:

- admin session and user lifecycle;
- dashboard, metrics, events, event timeline, DLQ and replay, audit and connections;
- app CRUD, credential rotation, callbacks and subscriptions;
- functions CRUD and invocation;
- routing rules;
- queue v2: subscriptions, pull/settle, leases, schedules, drain, callbacks and dead letters;
- realtime v1/v2: socket tokens, channel publish/history, files, push devices/notifications, studio;
- docs, SDK downloads, schemas, AsyncAPI and llms files.

Rust-only identity/dashboard routes remain compatible aliases where they are already used by agents. The full Admin UI uses the canonical `/api/v1/admin/*`, `/api/v1/apps/*`, `/api/v1/functions/*`, `/api/v1/routing/*` and `/api/v2/*` contract.

## Data strategy

1. Snapshot the Go database, object store references, runtime configuration, and route/feature counts before any write.
2. Create an explicit, idempotent migration tool with a mapping report. It must never copy password hashes blindly: if the hash format is incompatible, create a one-time setup/reset flow and revoke all old sessions.
3. Map the Go control-plane tables into Rust-owned tables using stable IDs where constraints permit. Preserve timestamps and audit provenance. For incompatible event/delivery/queue/realtime records, use a compatibility schema or a versioned import table and expose them read-only until the Rust writer can safely own them.
4. Reconcile counts, foreign keys, active credentials, queue depth, non-terminal deliveries, schedules, DLQ records, functions, routing rules and audit rows. Any mismatch blocks cutover.
5. Perform one final delta migration while the old writer is quiesced. Do not delete the old database; retain the backup and rollback image until the retention checkpoint.

## UI strategy

Port the existing full Admin UI (`web/admin`) to the Rust public artifact rather than retaining the minimal `web/remake-admin`. Keep the current routes, navigation, pages, error states, CSRF/session behavior, accessibility checks and browser acceptance coverage. The UI must not expose implementation names or require users to self-host internal components.

## Cutover and rollback

- Deploy the candidate side by side with the old Go stack.
- Run contract probes and browser acceptance against every Admin page and destructive workflow in a disposable namespace.
- Run live read-only parity checks and synthetic publish/queue/realtime/DLQ flows against production data.
- Switch the public router to the Rust service only after all gates pass.
- Observe health, error rate, auth, delivery, queue and realtime metrics for a defined soak window.
- Stop the old Go app only after the soak succeeds. Keep its image, database backup and rollback procedure; “tắt app cũ” means stop the runtime, not destroy recovery data.

## Non-goals

- deleting the Go database or old images before the rollback checkpoint;
- changing public credentials or silently rotating application secrets;
- dropping historical events, deliveries, DLQ, audit or realtime metadata to make counts match;
- claiming parity from a build-only or health-only check.
