# Rust v1 release rehearsal and migration handoff

## Current decision: Rust v1 promoted to the public edge with legacy rollback held

The branch supplies a disposable Compose host built from the committed
`relayhub-server` binary, with dashboard/scoped-token adapters, supervised
outbox/expiry and broker-consumer workers, live publish/status/ACK/settlement,
and shutdown-drain assertions. The complete local rehearsal is green against
fresh stores, but it does not authorize a production deployment.

The Rust v1 stack is now the public `relayhub.dungxbuif.com` route. The former
Go Compose runtime remains running but has no public route and is the explicit
rollback target. Rust v1 uses its own database, role, namespace, NATS stream and
Redis instance; there is no supported automatic import of legacy work or
credentials, and no legacy state was deleted.

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
The final live gate starts the real `relayhub` Compose service and is the only
host assertion in this rehearsal.

Prerequisites: Bash, GNU timeout, setsid, mktemp, Rust matching `rust/Cargo.toml`
(artifact compiler 1.99.1), a C compiler, Node >=22, npm, Go, Docker with a local
Unix-socket daemon, Compose v2 supporting `--wait` and build additional contexts,
and the pinned store images or registry access. Install locked npm dependencies
in `sdks/remake/node` and `web/remake-admin` beforehand. Playwright needs a usable
Chrome/Chromium installation. Cold builds may exceed the runner's bounds; a
timeout is a failure, never a skip or a pass.

Each suite has a 110-second timeout and a five-second forced-kill allowance;
the 39-case delivery suite has a 180-second timeout.
Suites run sequentially, so the complete rehearsal can exceed 120 seconds.
Do not run it during a session restricted to commands under 120 seconds. Each
cleanup command is bounded separately; cleanup of many projects can also take
longer than a single suite. Prerequisite failures exit 2. Suite or cleanup
failures exit nonzero. The live host gate also fails closed on startup, credential
seed, publish/status, Drain, socket-close, or clean-exit failures. A successful
rehearsal is not cutover approval.

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

### VM100 deployment and public-edge evidence (2026-10-30 — 2026-10-31)

Rust v1 runs as the `relayhub-v1` Swarm stack on `vm100-prod`. It uses a
dedicated Rust database/role, namespace, NATS stream, Redis instance, isolated
overlay networks, and private host port `18080`; the public edge reaches the
service through `edge-http-net`. The deployed image is `relayhub-v1:c61e998`, image ID
`sha256:a728179759b4426a76f8267a9dd78bf5111f988a2dea0e2ec37e10f995c4811c`.
The static React dashboard is served by the separate non-root
`relayhub-v1_admin` service from `relayhub-admin:v1-public-20261031`.

The live host checks passed: public OpenAPI (`200`), public dashboard root/login,
authenticated publish/status, WebSocket delivery and ACK settlement, then a
real Swarm `--force` update while a delivery socket was open. The socket
received a valid `drain` frame (`deadline_ms=30000`), closed, and the service
recovered to `1/1`. The old edge route is preserved at the Pi5 backup
`dynamic.yml.bak-relayhub-v1-pre-rust-20261031` for rollback.

The public contract smoke passed with disposable credentials: identity
setup/login, dashboard endpoint CRUD, dashboard durable publish/status/list/
replay/audit, scoped realtime, webhook ingress authentication, Node SDK
durable/realtime, Go SDK durable/realtime, credential revocation, and session
logout. The public dashboard browser smoke also created an app and endpoint,
sent a durable test, and verified the status message. Temporary workspaces and
their rows were removed after every run; no credentials were logged.

### Production-like restart and restore evidence (2026-10-31)

The v1 data-plane restart probes were run one service at a time on the
staging namespace that is currently serving the public edge. A forced update of
`relayhub-v1_nats` converged to `1/1`, and the public OpenAPI probe stayed
`200 application/json`. A forced update of `relayhub-v1_redis` likewise
converged to `1/1` and the public OpenAPI probe stayed 200. The API, Admin UI,
NATS and Redis services remained `1/1` after both probes.

A custom-format `pg_dump` of `relayhub_v1_staging_20261030` was restored with
`pg_restore --no-owner --no-acl` into the disposable database
`relayhub_v1_restore_20261031`. The restored database reported 17 public
tables and 8,655 kB; the dump was 51 kB. The disposable database and dump were
removed after verification. This proves a logical restore of the deployed
schema/data boundary, but it is not a PITR, off-host backup, or production
database failover rehearsal.

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
contract tests once; process exit is the test gate. The `relayhub` service uses
the same image but starts `/usr/local/bin/relayhub-server` with disposable store
URLs and an HTTP readiness healthcheck. Neither this image nor the Compose file
is a production host; the public Swarm deployment is separate and uses the
pinned Rust image with its dedicated production-like database namespace. Its
backup/restore and long-term soak remain operational gates.

### Live host gate

After the pinned artifact is built, the runner starts only the disposable
`relayhub` service and its PostgreSQL, NATS, and Redis dependencies with a unique
Compose project. It verifies the loopback-only published port through the
service healthcheck (`GET /api/v1/openapi.json`), then seeds one application key,
one agent key, and one endpoint into that fresh PostgreSQL database. The seed is
random, owner-only, and removed with the project volumes.

The live assertion performs these exact checks against `relayhub-server`:

1. `POST /api/v1/events` with the application bearer credential and an
   idempotency key returns `202` with event and delivery IDs.
2. The seed SQL is read back by a PostgreSQL assertion covering the user,
   workspace, app, both credentials, and endpoint rows.
3. `GET /api/v1/events/{event_id}` with the same credential returns `200`; the
   authenticated agent receives the matching delivery, ACK settles it, and the
   persisted event reaches `succeeded`.
4. The agent receives a valid `Drain` frame after SIGTERM reaches the real host;
   the websocket closes and the stopped container exits with code zero.

The runner captures command output in a mode-700 temporary directory and emits
only labels and exit status. It never logs generated keys, SQL, payload, or
remote error text. The live gate has no production URL, credential, volume, or
network.

## Remaining production gates after the public promotion

The public route and all v1 application surfaces are now exercised. The
remaining work is operational hardening and legacy-data migration, not a code
or route cutover:

1. Production sizing, secret delivery/rotation, TLS/proxy configuration,
   sanitized operational metrics/logs, and backup/restore evidence for that exact
   image and environment. Agree on canary duration, latency/error thresholds,
   recovery objectives, and a named rollback owner before a traffic switch.
2. A production failure/drain rehearsal covering active in-flight responses,
   PostgreSQL service restart/failover, reconnect recovery and stale ACK
   fencing. NATS/Redis restart recovery and a logical restore of the deployed
   PostgreSQL boundary are now evidenced above; the local recovery suite and
   the logical restore remain short-lived evidence, not PITR certification.
3. Decide whether to migrate legacy Go work/credentials into Rust v1. No automatic
   migration exists; until that decision, the old Compose runtime remains the
   rollback system but is not a second public writer.

## Future migration sequence (blocked until production gates pass)

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
sanitized evidence, data owner, remediation, and renewed approval. Until the
production gates above exist and pass, the only supported action is isolated
rehearsal.
