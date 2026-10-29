# Rust v1 release rehearsal and migration handoff

## Current decision: no production cutover

Task 11 supplies disposable recovery tests, a bounded rehearsal runner, and a
container artifact for contract tests. It does not supply a production API/worker
host. `relayhub version` and `relayhub connect --config <path>` are the existing
CLI commands; there is no server, migration, or drain CLI to invoke.

The existing Go deployment, its Compose files, public routes, SDKs, and admin UI
remain the operational system. Do not point the Rust tests at it, reuse its
database, or rename its streams/volumes. There is no supported automatic import
of legacy work or credentials into the Rust schema. This handoff authorizes no
push, deployment, traffic switch, or deletion of legacy state.

## Rehearsal artifacts and evidence

Run from the repository root on a trusted, disposable local Docker host:

```sh
bash scripts/remake-acceptance.sh
```

This is an operator rehearsal command, not a quick validation command. It runs
the existing Rust unit/CLI, integration and documentation suites, all five
recovery tests, Node SDK checks/build/acceptance, Go race acceptance/vet,
dashboard checks/build/browser tests, and the artifact build/contract tests.
The SDK suites build library adapters; dashboard browser tests use mock APIs.
Neither demonstrates that a deployable production host exists.

Prerequisites: Bash, GNU timeout, setsid, mktemp, Rust matching `rust/Cargo.toml`
(artifact compiler 1.99.1), a C compiler, Node >=22, npm, Go, Docker with a local
Unix-socket daemon, Compose v2 supporting `--wait` and build additional contexts,
and the pinned store images or registry access. Install locked npm dependencies
in `sdks/remake/node` and `web/remake-admin` beforehand. Playwright needs a usable
Chrome/Chromium installation. Cold builds may exceed the runner's bounds; a
timeout is a failure, never a skip or a pass.

Each suite has a 110-second timeout and a five-second forced-kill allowance.
Suites run sequentially, so the complete rehearsal can exceed 120 seconds.
Do not run it during a session restricted to commands under 120 seconds. Each
cleanup command is bounded separately; cleanup of many projects can also take
longer than a single suite. Prerequisite failures exit 2. Suite or cleanup
failures exit nonzero. Even if every suite passes, the missing-host gate exits 2;
there is no bypass flag. A successful library rehearsal is not cutover approval.

Output exposes suite labels and exit codes only. Raw test/adapter/Compose output
is captured inside a private temporary directory and deleted on exit; it may
contain tokens, URLs, or fixture bodies and must not be copied into tickets.
Do not run under shell tracing or supply production secrets. Record the commit,
tool versions, image digests, pass/fail labels, durations, and exit status as
sanitized evidence. Never record credentials, request bodies, or credential URLs.

The script gives its artifact a unique project and records the exact Compose
file/project pairs created by the existing UUID-based test harnesses. EXIT,
INT, TERM, and HUP traps stop the active process group and remove only registered
projects, disposable volumes, and locally built project images. SIGKILL or host
loss cannot run traps. If cleanup fails, the private `projects` registry is
retained and its path is printed, without raw suite output. Inspect that registry
before retrying cleanup; each filename is the project and its content is the
Compose file. Use that exact pair:

```sh
# Substitute only a verified disposable project and its registered file.
timeout --kill-after=5s 45s docker compose --env-file /dev/null \
  -f "$registered_compose_file" -p "$registered_project" \
  down --volumes --remove-orphans --rmi local --timeout 5
```

Never use global Docker prune, production project names, or external volumes.

### Recovery coverage

All five names in `rust/tests/recovery.rs` are real tests, with Docker-backed
state and assertions. Missing services fail rather than silently skipping.

| Test | Evidence when executed successfully | Limit |
| --- | --- | --- |
| `broker_restart_preserves_pending` | File-backed JetStream retains an unacked notice across restart, redelivers its sequence, and the delivery settles once | Library gateway; no host supervision test |
| `redis_restart_reconnects` | Redis restart invalidates old presence; rebuilt wiring reclaims and settles pending work | Explicitly rebuilds the non-reconnecting Redis connection; no automatic host reconnect |
| `postgres_restore_recovers_outbox` | `pg_dump`/`pg_restore` into a fresh database recovers pending outbox/quota state, dispatches once, and settlement releases quota without changing the source | Disposable database restore; no production backup/PITR certification |
| `gateway_kill_fences_old_ack` | A killed subprocess leaves an in-flight delivery; retry gets a new token and rejects the stale ACK with 409 | Library subprocess and accelerated persisted expiry timestamps; no socket-host drain test |
| `old_and_new_namespaces_isolated` | Old broker/presence/database sentinels survive new-namespace activity | Synthetic legacy sentinel; does not contact the live Go deployment |

Compilation and formatting checks do not execute these recovery scenarios.

### Container boundary

`deploy/remake/compose.yaml` is staging-only. Always use a unique `-p`; it has no
external networks/volumes or fixed public ports. Stores bind random loopback
ports. PostgreSQL trust auth and unauthenticated NATS/Redis are only acceptable
for disposable fixtures on a trusted machine, never a shared or public service.
PostgreSQL and JetStream use project volumes so restart/restore tests can retain
state. Redis deliberately loses ephemeral presence on restart.

Services run without root, capabilities, or writable root filesystems, with
explicit writable mounts, CPU/memory/PID/log limits, health checks, SIGTERM, and
a 35-second stop grace. The optional `artifact` profile builds the Dockerfile
using the Rust and frozen-contract contexts. Its harness has no network and runs
contract tests once; process exit is the test gate. Its CLI-version health check
is only a binary smoke check, not API readiness. Neither this image nor the
Compose file is a production host; production resource sizing and graceful drain
remain unverified.

## Host gates required before a traffic rehearsal

The runner's final blocker must remain until approved live assertions replace it.
Implement and verify these in a separately authorized change:

1. Authenticated publish and delivery-status adapters, verified principals and
   tenant boundaries, persisted idempotency/quota behavior, and Response frame
   dispatch tied to the real delivery attempt and settlement transaction.
2. Supervised outbox/expiry workers, durable broker consumers, gateway routing,
   reconnect scans, and Redis connection replacement after failures. Startup
   migrations must finish before readiness or ingress. Test fail-closed readiness
   and restart behavior through actual host sockets.
3. Shutdown that first stops ingress and new claims, then allows active
   responses/settlements to finish within a measured deadline. Unfinished work
   must remain recoverable; stale sessions/tokens must stay fenced. Compose's
   stop grace alone does not implement this protocol.
4. Dashboard workspace/delivery/replay/audit endpoints and scoped realtime-token
   adapters, with session, authorization, expiry, and logout checks against the
   live host. Mock browser acceptance cannot satisfy this gate.
5. Health/readiness, secret delivery/rotation, TLS/proxy configuration, sanitized
   operational metrics/logs, and backup/restore evidence for that exact image and
   environment. Agree on canary duration, latency/error thresholds, recovery
   objectives, and a named rollback owner before a traffic switch.

## Future migration sequence (blocked until host gates pass)

1. **Inventory and back up.** Record legacy routes, credentials by reference,
   endpoint mappings, outstanding work, idempotency windows, consumer positions,
   and rollback owner. Take a protected PostgreSQL backup and required legacy
   broker/storage backups; restore into an isolated environment and reconcile
   counts, IDs, quota totals, and pending outbox state. Preserve retention and
   deduplication windows. A dump file alone is not restore evidence.
2. **Provision separately.** Use a dedicated Rust database/role, broker namespace
   and consumers, Redis prefix, credentials, project volumes, and staging route.
   Keep legacy resources unchanged. Apply the library migrations in order:
   Identity, Events, Deliveries, then Webhooks (including response tables). The
   current recovery harness exercises the first three; response-aware host
   startup must include the fourth before serving webhook traffic. Do not run
   Rust migrations on the legacy database or copy its migration ledger.
3. **Enroll a canary.** Create new scoped application/agent credentials through
   the approved host and map endpoint IDs explicitly. Move only an agreed test
   publisher and connector. Store agent credentials in an owner-only file
   (mode 0600); the existing connector rejects symlinks and insecure ownership
   or permissions. Browser code must never receive backend application/agent
   secrets. Verify binary bodies, duplicate headers, response timing, replay,
   quota rejection, auth failures, and cross-tenant isolation through live routes.
4. **Exercise failure and drain.** Run the five recovery tests on disposable
   stores, then equivalent live-host failure/drain assertions. Observe outbox
   backlog, delivery state/attempt counts, stale ACK rejection, quota accounting,
   active claims, response outcomes, and reconnect recovery. Reconcile every
   accepted canary event to an explicit terminal or recoverable pending state.
5. **Switch deliberately.** With explicit operator approval and recorded gates,
   stop legacy ingress for the selected cohort and let its accepted work drain
   on the legacy stack. Switch publishers/connectors as one recorded cohort;
   do not dual-publish or share idempotency domains implicitly. Keep legacy
   workers/storage available for their outstanding work. Expand only when the
   agreed observation thresholds hold. Retire legacy resources only after all
   accepted work and retention obligations are accounted for and a separate
   deletion decision is approved.

## Rollback and ownership of accepted work

Trigger rollback for missing/duplicate work, incorrect responses, auth leakage,
unbounded backlog, failed restore/drain, or the agreed error/latency thresholds.
Stop expansion and new Rust ingress/claims using the future host's verified
drain procedure. Preserve its database, outbox, streams, and audit records.
Allow active settlements to finish; retain unresolved work for recovery.

Route only new work back to the unchanged legacy deployment and restore its
publisher/connector configuration. Work already accepted by Rust remains owned
by Rust until explicitly reconciled. Do not replay it into legacy blindly:
local side effects can have happened before an ACK, so retries require endpoint
idempotency and an audited per-event decision. Never roll back by restoring an
old database over newly accepted work or by deleting the new project volumes.

Before retrying cutover, reconcile accepted IDs, terminal states, pending outbox,
attempt histories, quotas, and side effects on both sides. Record the failure,
sanitized evidence, data owner, remediation, and renewed approval. Until these
host procedures exist and pass, the only supported action is isolated rehearsal.
