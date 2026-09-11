# Deployment stack decisions

Root `compose.yaml` is canonical, with an exact public copy at
`public-docs/deploy/docker-compose.relayhub.yml`. The Go YAML contract test parses
both and enforces three service names, one project network, API-only publication,
required environment examples, hardening, Redis AOF and CI gates. The public copy
uses `--project-directory .` from the root so its build context remains identical.

API and worker use one distroless static image, numeric non-root UID/GID 65532,
read-only roots, no capabilities or writable mounts. Redis 7 uses UID/GID 999,
read-only root plus one writable named volume, required AUTH, AOF and every-second
fsync. All services have healthchecks and graceful stop periods. Go healthchecks
call local readiness directly in the binary, return only success/failure, reject
redirects/nonloopback targets and need neither application credentials nor a shell.
The multi-stage build includes CA roots and builds native Linux amd64 or arm64.

`RELAYHUB_REDIS_PASSWORD` is the only new runtime setting: when nonempty, parse the
Redis URL, retain its username and URL-encode this password into userinfo. It
overrides an existing URL password. Existing URL-only configuration keeps working.
Root Compose fixes listener ports to match routing/probes; direct binary usage
retains configurable addresses. The no-argument binary defaults to API. See the
[complete settings table](../../public-docs/deploy/README.md) for supported defaults.

Request logs are generated in HTTP middleware with server-owned request UUIDs,
method, matched route template, status, latency and bounded outcome. They omit
caller request IDs, raw paths/queries, headers and bodies. Unknown paths/methods
are bounded. The wrapping response writer preserves WebSocket hijacking support.
Function/callback operation logs contain typed IDs and bounded outcomes. Captured log tests
supply sentinel values in auth/signature/query/path/input/result/body fields.
Acceptance additionally scans actual API/worker logs for every generated credential,
signature and sentinel payload. No production delivery or persistence behavior is
changed by logging. This narrow logging addition was explicitly authorized to
satisfy Task 8 acceptance evidence.

The independent acceptance client imports no RelayHub internal code. It signs the
public canonical contract, checks the published golden vector, uses Gorilla RFC
6455, and verifies callback raw bytes/HMAC independently. Its unique project owns
all acceptance resources. HTTP callbacks are enabled only in acceptance and resolve
`e2e.internal` to the host gateway. Required test credentials remain in process
memory and subprocess environment; no secret-bearing config file is emitted.
Docker retains environment metadata as usual; KEEP projects require trusted access.

CI has a required Redis service and explicit `RELAYHUB_TEST_REDIS_URL`, avoiding the
testcontainer unavailable skip path. Gates cover formatting, vet, unit/race,
integration, docs/contracts negative controls, Docker, Compose and acceptance.
External Traefik and Cloudflare are documented operator infrastructure and are not
added to the three-service deployment. External routes never cache API/RPC/WebSocket
traffic. No image publishing or external deployment occurs in this task.

The [operations runbook](../operations/runbook.md) describes health diagnosis,
backup/restore, upgrades, rollback, observability and the exact release gate. The
[root README](../../README.md) provides generated credentials and a complete first
signed publish/lease/ack. Public Markdown plus llms/OpenAPI/Skills form the human
and AI documentation surfaces. `go generate ./web` regenerates canonical artifacts
and embeds them; build and negative controls reject drift.

## Task 8 implementation record (before code)

The root Compose deployment will have exactly API, worker and authenticated Redis 7 on a project-scoped network, with only API port 8080 published. API/worker will share a non-root distroless image with CA roots, read-only filesystem, dropped capabilities and a binary HTTP probe. Redis uses a named AOF volume and a required password; configuration will URL-encode that password separately from the Redis URL.

Acceptance will independently sign HTTP requests, use Gorilla RFC 6455, verify raw callback signatures and bodies, check RPC exclusivity/replay/offline/timeout, compare served documentation bytes, restart each runtime/Redis and scan captured logs for secret/payload leakage. A unique Compose project owns all acceptance resources and bounded cleanup. CI will run unit/race/integration, negative contract controls, image/Compose gates and acceptance without repository secrets. A root quick start, public deployment/security/troubleshooting docs and internal runbook will document actual defaults, TLS/proxy behavior and backup/restore.

Verification sequence: acceptance RED before Compose, probe/topology/config RED then GREEN, complete clean-project acceptance, generated docs parity, then the full Task 8 gate.

## Implementation reconciliation

Implemented the topology, binary probe, separate encoded Redis password, static distroless image, required CI gates and independent acceptance. The first clean-project acceptance passed every scenario. Exact final gate evidence is recorded in the Task 8 implementation report.

Successful authenticated request logs also include the persisted app ID. Application
operation logs record generated event/job IDs for publish/lease/admin transitions,
the validated event ID for acknowledgement, a persisted function ID at registration,
and the persisted invocation ID for a completed call or replay. Worker logs record
persisted target app/event/job IDs, callback attempt and outcome only after the
transition commits. Function names, callback URLs and handler error details remain
excluded. A function replay may change its URL, so its unchecked path is never
logged as the original function ID. Capture tests and acceptance assert these
specific IDs and outcomes while checking every sensitive sentinel remains absent.

The final narrow logging ruling also covers WebSocket authentication: after token
verification, the token-derived app ID is attached to request-log state. The
connection-completion log records that app ID and 101 while excluding the token,
query and client-supplied request-ID header. A real Gorilla capture test verifies
this boundary.

## Task 8 fix round 1 plan (before implementation)

Review identified three operational gaps. The docs runtime checker will use CI's
explicit external Redis URL with a cryptographically random prefix and cleanup
restricted to that prefix; local host Redis remains an explicit fallback. CI
contract tests will reject a runtime docs gate with no supplied Redis dependency.
Backup and restore helpers will run UID/GID 999 with zero capabilities, streaming
the archive through the host's protected file. A disposable-volume rehearsal will
exercise private AOF permissions and exact restored bytes. The acceptance command
runner will create process groups, terminate the entire group on deadline, apply
bounded pipe waits and SIGKILL fallback; an inherited-pipe orphan fixture must show
bounded return, child death and cleanup continuation. Focused tests, docs/runtime
checks, full acceptance and the volume rehearsal will validate these changes.

## Fix round 1 reconciliation

CI now supplies `RELAYHUB_DOCS_TEST_REDIS_URL` to the required docs runtime gate.
Real-service tests hide the host binary and prove unique namespace cleanup on
success and failure while preserving unrelated Redis state. A missing dependency
and a workflow with its explicit URL removed both fail the contract gate.
All Docker/Compose commands use the bounded Unix process-group runner, covered by
TERM-ignoring inherited-pipe fixtures and cleanup-continuation assertions. The
UID/GID 999 backup rehearsal passed exact-byte, ownership, permission and
readability checks using fresh disposable volumes and zero capabilities. Focused
race tests, all 15 negative controls and complete Docker acceptance passed; no
rehearsal resources remain. These changes affect verification and backup
operations, with production topology, API contracts and safe logging preserved.

## Task 8 fix round 2 plan (before implementation)

The docs cleanup client currently selects the URL path database while pinned
go-redis v9.22.0 allows a `db` query override. The checker will validate its
supported test-URL subset before launching the API and use the same effective
database for readiness and namespace cleanup: a single valid nonnegative decimal
`db` query value overrides the path. Duplicate, malformed or unsupported query
options and invalid database paths will fail explicitly without printing the URL.
A network-free wire regression will require `SELECT 1` for `/0?db=1`, and a real
external-Redis regression will create app state in DB 1, verify cleanup on success
and failure, and preserve unrelated sentinels in both databases. Cleanup remains
prefix-only SCAN/DEL. Internal/public deployment docs and generated AI surfaces
will document the supported URL subset; focused docs/CI and negative controls
will verify the correction.

Round 2 reconciliation: the checker now validates the supported URL subset before
build/start and reuses its effective database for every Redis command. The
network-free regression first observed `SELECT 0` instead of the literal expected
`SELECT 1`; the real-service regression first observed three leftover app/index/
credential records in DB 1. Both now pass, as do rejection tests for 19 invalid or
unsupported forms before process/network activity. The external test verifies
cleanup on success and failure while preserving unrelated state in both DBs;
only the checker and its operator documentation changed.
