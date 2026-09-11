# Task 8 implementation report

Status: DONE

Commit message: `build: ship verified three-service RelayHub stack`

## Delivered behavior

- Canonical root Compose and byte-identical public copy have exactly API, worker and Redis 7 on one project-scoped network. Only API publishes a host port; no container names, worker expose/ports or Redis host port exist.
- The shared image is a multi-stage Linux amd64/arm64 distroless static build with trusted CA roots, embedded current docs/Skills/contracts, numeric non-root identity and explicit build-input copies excluding `.env`/backups/arbitrary workspace files. Runtime roots are read-only, all capabilities are dropped and no-new-privileges is enabled. Redis alone writes its named AOF volume.
- Required generated admin/server/Redis credentials have empty example values. Separate `RELAYHUB_REDIS_PASSWORD` is URL-encoded and overrides URL userinfo password without changing existing URL-only direct-binary configuration. Every supported setting is documented and included in `.env.example`.
- Binary `healthcheck` uses bounded loopback HTTP, requires exactly 200, rejects redirects/invalid targets and prints no URL/body. API/worker Compose probes use Redis-backed readiness; worker operations remain internal. Redis has authenticated healthcheck, AOF/everysec fsync, persistent volume and graceful stop.
- Repeatable `./scripts/e2e.sh` builds its client within 120 seconds, then runs a bounded suite with unique project/image, process-local credentials, controlled host callback listener, captured secret-free output and project-only cleanup. Cleanup verifies containers/networks/volumes and the unique image tag are removed; KEEP explicitly retains the diagnostic project.
- CI validates formatting, vet, unit/race, mandatory real Redis integration via explicit service URL, docs/runtime contracts with negative controls, Docker, Compose and acceptance. No repository secrets or skip-on-error gates are used.
- Root quick start includes generated secrets and a complete first independently signed publish/lease/ack. Public deployment/security/troubleshooting, internal deployment/runbook, llms and embedded docs match actual defaults, topology, health, logging, backup/restore, upgrade, external Traefik and Cloudflare routing/cache/timeout behavior.

## Authorized logging scope rulings

The initial ownership limited main changes to probes and excluded HTTP/worker logging. Existing runtime logs lacked the generated IDs required by acceptance. The parent explicitly authorized narrow structured logging in the HTTP boundary, main and worker, then clarified that domain IDs were mandatory. A final explicit authorization included the WebSocket handler's token-derived app-ID assignment.

Implemented only typed safe fields: generated request UUID, authenticated app ID when available, method, matched route template, status, latency, event/job/function/invocation IDs at successful boundaries, persisted callback attempt and bounded outcome. Body/header/query/callback URL/function name/input/result/error detail are never log fields. HTTP replay can change its function URL, so logging does not mistake that unchecked URL parameter for the original persisted function ID. Worker success logs occur only after the durable transition succeeds. WebSocket logs retain RFC 6455 hijacking and report 101 with token-derived app identity after connection completion.

Public API/JSON Schema/OpenAPI shapes were unchanged, so contract schemas and Skill source did not require semantic changes. Generated llms/embed artifacts were reconciled. No Task 1–7 delivery, auth, persistence, function or protocol rule was weakened.

## TDD evidence

All commands were executed through the required RTK wrapper. RED observations preceded implementation:

| Test / check | Observed RED | GREEN evidence |
| --- | --- | --- |
| Initial `scripts/e2e.sh` | `FAIL: root compose.yaml is required for acceptance` | Complete stack acceptance passes |
| `TestHealthcheckCommand` | HTTP 200 probe returned runtime usage error | 200 succeeds; 204/redirect/503/invalid/unreachable targets fail without secrets |
| `TestRedisPasswordIsURLEncoded` | Separate reserved-character password was not encoded into Redis URL | Exact independent expected encoded URL passes |
| `TestDeploymentContract` | Root Compose absent | Parsed exact topology/security/env/public parity passes |
| `TestCIContract` | Workflow absent | Required gates/services/timeouts/no-secrets checks pass |
| Explicit image-copy invariant | `COPY . .` rejected | Build copies only named inputs |
| Request log capture tests | No JSON log output | Generated identity/status/route/outcome and sentinel redaction pass |
| Real WebSocket status log | Reported 200 instead of 101 | 101 assertion passes with standards upgrade preserved |
| Typed operation-ID capture tests | App/event/job/function/invocation fields absent; callback operation missing | Actual publish/ack/register/invoke/replay plus persisted callback fields pass |
| Worker failed persistence capture | Verified no success log on failed transition | Negative branch remains silent |
| Token-derived WebSocket app capture | Verified app identity missing | Final focused race/log/WebSocket suite passes |

The e2e client imports no RelayHub internal packages or helpers. It independently assembles canonical HMAC, verifies the published signing golden vector, verifies exact callback bytes/HMAC, uses Gorilla RFC 6455 and fixed expected results/statuses. Credentials, API keys, signatures, socket tokens and sentinel input/result/event values remain in memory and are checked against captured logs.

## Complete gate evidence

The complete gate was repeated after the domain-ID logging extension. Every command below exited 0:

- `test -z "$(gofmt -l .)"`
- `go vet ./...`
- `go test ./...`
- `go test -race ./...`
- `go test -race -tags=integration ./... -count=1 -timeout=180s` — real Redis/testcontainer package completed in 29.517s; all packages passed.
- `./scripts/build-skill.sh`
- `./scripts/build-llms.sh`
- `python3 scripts/check-docs.py` — real API/Redis artifacts and signed flows passed.
- `./scripts/check-contracts.sh --self-test` — all 14 mutation controls rejected their broken fixtures, including Compose drift, removed hardening, usable example password and missing mandatory Redis gate.
- `docker build -t relayhub:release-candidate .` — linux/arm64 image built with static docs/contract checks.
- `docker compose config --quiet` — generated process-only credentials, no interpolated configuration printed.
- `./scripts/e2e.sh` — all nine stages passed repeatedly; the last full run includes exact persisted domain-ID/outcome assertions and cleanup verification.
- Additional `docker build --platform linux/amd64 -t relayhub:amd64-verification .` — built successfully; no image was pushed.

After the final three-line token-derived WebSocket app-ID assignment, the parent explicitly requested the focused gate rather than repeating the entire stack: `go test -race ./internal/httpapi -run 'Test.*Log|Test.*WebSocket' -count=1`, `git diff --check`, and the formatting check all exited 0. The final source differs from the prior complete gate only in that tested logging assignment and its capture/internal note.

Local tooling: host Go 1.26.3, Docker 29.7.2, Compose v5.5.1, Node v24.0.2; image toolchain Go 1.24. The first local docs command encountered unavailable Python validator imports, then a manually narrowed PATH hid Node. Both were corrected by prepending the existing `/tmp/relayhub-docs-venv/bin` to the normal PATH. No repository code was changed to bypass tooling requirements; final docs gates used the real validators/Node.

## Acceptance assertions observed

1. Exactly three healthy services; only API host port; API and internal worker health/readiness/metrics return success.
2. Admin creates producer, queue/function consumer, separate WebSocket observer, retry callback and terminal callback apps without printing credentials.
3. Signed 202 publication, same-event replay/header, queue lease and repeated ack reach `acked`.
4. Gorilla ready/subscribe/event/job updates with a cross-app ping barrier/quiet window and normal close.
5. Two owner connections receive exactly one dispatch per new online invocation; fixed successful result, 503 offline, 504 timeout, and all replay statuses/bodies with no redispatch.
6. Raw callback body equals the persisted event bytes; independent HMAC/timestamp checks; 503 then 204 yields two attempts/delivered; 400 yields one attempt/dead-letter.
7. Docs HTML/Markdown/OpenAPI/schemas/llms/Skill source/ZIP MIME and bytes match sources; ZIP opens with exactly the normalized expected paths and entry bytes.
8. API/worker restart preserves pending/queryable state; Redis stop makes readiness fail while liveness remains; Redis AOF restart recovers pending state and allows lease/ack.
9. Logs contain exact event/callback job/function/invocation IDs and expected outcomes/attempts while containing none of the generated credentials/signatures/tokens/sentinel payloads.

## Limits and concerns

No unresolved implementation concerns. External Traefik/Cloudflare deployment and image publishing were intentionally not performed. Native Docker end-to-end evidence is arm64; amd64 was cross-built. AOF uses every-second fsync, so API acceptance does not claim disk fsync or zero data loss after a host/volume failure. Acceptance HTTP callback host-gateway access requires a permissive local test firewall; production retains HTTPS-only defaults. These operational limits are documented in the public deployment guide and internal runbook.

## Fix round 1: required review corrections

All three Important findings were reproduced and corrected. The separately ledgered KEEP cleanup-hint Minor remains outside this fix round.

| Finding | Observed RED before fix | Implemented GREEN |
| --- | --- | --- |
| CI docs required a host Redis binary despite its service container | External-service runtime test failed with `redis-server is required for real API smoke`; CI contract failed with `CI misses RELAYHUB_DOCS_TEST_REDIS_URL:` | Checker accepts the mandatory CI service URL, authenticates and checks it, uses a random per-run namespace, and deletes only that namespace on success or failure. A real-service test hides the host binary, creates application data, preserves an unrelated sentinel, and proves two distinct namespaces are cleaned. A missing dependency fails explicitly; a workflow mutation removing the URL is rejected. |
| Cap-dropped root could not archive private Redis AOF files | Disposable-volume rehearsal failed at archive creation using the old root/no-capabilities helper against UID 999 directory mode 700 and files mode 600 | Backup and restore run as numeric `999:999` with no capabilities and no network, streaming the archive through protected host files. Rehearsal verifies independent exact AOF/manifest bytes, ownership, 700/600 permissions, and UID 999 readability after restoration into a fresh volume. |
| CLI cancellation could leave inherited pipes open forever | Both orphan-parent-exit and deadline-parent fixtures exceeded the bounded wait with `command waited indefinitely for inherited pipes` | Every Docker/Compose invocation uses a fresh Unix process group, TERM then 150 ms SIGKILL fallback, and 250 ms `WaitDelay`. Capture tests verify bounded return, death of a TERM-ignoring pipe-holding child, normal input/output, and deferred cleanup continuation. |

The volume rehearsal has a two-minute operation deadline and independent twenty-second cleanup. It creates uniquely named/labeled source and restore volumes plus one temporary helper and removes only those resources. The docs isolation tests used a separately owned disposable Docker Redis service with the host Redis binary hidden; that service was removed after the gates. No production topology, protocol, logging fields, or Task 1–7 behavior changed.

Internal deployment/runbook docs, the root README, public deployment guide, example test configuration, and generated llms/embed surfaces were reconciled. Public API schemas and Skills needed no semantic changes because this round changes verification and operator backup commands only.

### Fix round verification

All commands ran through RTK and exited 0:

- Formatting check, `git diff --check`, `go vet ./...`, and `go test ./...`.
- `go test ./cmd/relayhub -run 'TestCIContract|TestDeploymentContract' -count=1`.
- `go test -race ./scripts/e2e-client.go ./scripts/e2e-client_test.go -count=1 -timeout=20s` repeatedly; final run completed in 2.599 seconds. Explicit-file `go vet` also passed.
- `python3 scripts/test-docs-runtime.py` with explicit Docker service URL and host Redis hidden; both tests passed, final run in 5.314 seconds.
- `go generate ./web`, `python3 scripts/check-docs.py`, and `./scripts/check-contracts.sh --self-test` against the explicit service; all runtime/signed flows and all 15 negative controls passed.
- `./scripts/e2e.sh` rebuilt the image and passed all nine acceptance stages with the new bounded command runner and scoped cleanup.
- `./scripts/e2e.sh --backup-rehearsal` passed repeatedly, including exact restored bytes and permissions. The final Docker volume inventory for the rehearsal label was empty.

The affected package gates, independent runner race tests, mandatory docs Redis gate, complete Docker acceptance, and backup rehearsal cover this round. The prior full real-Redis package race/integration gate remains recorded above; production Go behavior is unchanged in this correction round.

## Fix round 2: Redis URL database parity

The residual Important finding was confirmed against pinned go-redis v9.22.0: a nonempty `db` query parameter overrides the path database. The docs helper previously ignored that parameter, allowing `/0?db=1` to create application state in DB 1 while cleanup scanned DB 0.

RED evidence preceded the implementation:

- Network-free wire regression expected literal `SELECT 1` for `/0?db=1` and observed `SELECT 0`.
- A disposable external Redis regression created an app with `/0?db=1`, then observed three generated app/index/credential records left in DB 1 after the runtime context exited. The test's independent cleanup removed only its own fixture keys even on this RED run.
- Nineteen duplicate, unsupported or malformed query/path/fragment cases reached the process-launch boundary instead of failing URL validation.

The checker now validates a deliberately restricted URL subset before build/API/network activity and selects the resulting database for readiness and every cleanup command. It accepts `redis://`/`rediss://`, optional credentials/port, an optional nonnegative decimal database path, and at most one nonnegative decimal `db` query override; database numbers must fit signed 64-bit. Duplicate, empty, signed, malformed and unsupported query options, invalid paths and fragments fail explicitly without printing the URL. Prefix validation and scoped SCAN/DEL are preserved; no database flush is used.

GREEN and final verification, all executed through RTK with exit 0:

- `python3 scripts/test-docs-runtime.py RedisURLSelection`: the network-free wire and 19 rejection cases pass.
- `python3 scripts/test-docs-runtime.py` against the owned external Redis: all five tests pass, final run in 8.564 seconds. The query override regression verifies generated state appears only in DB 1, is removed after both success and forced failure, and unrelated sentinel keys in DB 0 and DB 1 survive.
- `go generate ./web`, `go test ./cmd/relayhub ./web -count=1`, `python3 scripts/check-docs.py`, and `./scripts/check-contracts.sh --self-test`: docs/API signed flows, affected CI/deployment/embedded docs checks and all 15 negative controls pass.
- `git diff --check` passes. The owned Redis service reported zero keys in both DB 0 and DB 1 after verification and was removed; no test resource remains.

Internal deployment/runbook and public deployment/llms/embed surfaces document the supported test URL grammar. Production configuration, API schemas, Skills, topology, end-to-end runner and logging behavior are unchanged, so they need no semantic updates or repeat of unrelated Docker acceptance. The KEEP Minor remains separately ledgered and untouched. No unresolved concern remains for this finding.
