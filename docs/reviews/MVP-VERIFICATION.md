# RelayHub MVP release verification

**Result: PASS. All 50 requirement rows pass; zero failed or unverified final requirements.**

Verified 2026-09-11 in the independent RelayHub repository on `feat/relayhub-mvp`.
Implementation review range: `12accf0..6d9e797` plus the Task 9 candidate `ff8c191`
(`chore: verify RelayHub MVP release candidate`) and the two scoped review fixes
recorded below as `fix: close release verification review gaps`. The plan's `7f7c3f2` reference
does not exist in this repository; the orchestrator confirmed `12accf0` as the
actual preimplementation baseline. This is a provenance correction, not a product gap.

The requirement matrix was recorded before production edits. Initial gaps were
reproduced, repaired and reconciled below. Review covered the complete current
source/config/contracts/docs tree, all deferred findings, concurrency boundaries,
authentication, credential handling, retry and invocation state, operations and
deployment. No Critical/Important finding remains open.

Environment: macOS arm64; patched Go 1.27.1; Docker Engine 29.7.2, Compose v5.5.1;
Redis 7 Alpine test containers; Python 3.14 with validators from
`/tmp/relayhub-docs-venv/bin`; Node and real Chrome 153.0.8010.12. No external
deployment or image push was performed. The public origin remains
`https://relayhub.dungxbuif.com`.

All raw evidence is in the ignored directory
`.superpowers/sdd/2026-09-11-relayhub-mvp/verification/`, abbreviated **V** below.
Each command ran through `rtk proxy`. Test flags `-count=1` prevent cached results;
`-json` records pass/fail/skip events without changing test behavior.

## Requirement matrix

| Requirement | Evidence verified | Verdict | Audit resolution |
|---|---|---|---|
| Go API/worker, Redis 7, exactly three services, correct origin | Dockerfile, compose.yaml, E2E topology | PASS | Both release images build; E2E topology and safe inspection pass |
| Required secrets, validated config and origin allowlist | config TestLoad*, TestWorkerConfiguration | PASS | Fresh gate passes |
| Liveness independent of Redis; readiness checks Redis | TestHealthDoesNotDependOnRedis, TestReadyReflectsRedisReachability, E2E | PASS | Fresh gate passes |
| Prometheus HTTP/event/delivery/retry/DLQ/socket/function metrics | operations tests, worker binary metrics integration, E2E | PASS | All bounded worker error paths now covered |
| Admin bearer creates/manages/disables applications | apps HTTP and Redis CRUD tests | PASS | Fresh gate passes |
| Credentials returned once; hashed API key; atomic rotation | TestAppCreateReturnsCredentialsOnceAndQueriesRedactThem, TestApplicationPersistenceAndCredentialIndexes | PASS | Fresh gate passes |
| Disabled/unknown/bad-signature/expired app requests return 401 | TestSignedRoutesRejectMalformedAndExpiredAuthentication | PASS | Fresh gate passes |
| HMAC canonical target/body, lowercase SHA256, inclusive 300 seconds | auth signature literal and boundary tests | PASS | Fresh gate passes |
| HTTP body limited to 1 MiB | TestRequestBodyLimitRejectsOversizedRequests, function HTTP tests | PASS | Fresh gate passes |
| Safe callback configuration | App validation tests | PASS | TestAppRejectsEmptyHostnameAndInsecureLinkLocal passes |
| Concurrent app changes preserve disabled state and timestamp order | Redis application integration | PASS | TestApplicationStaleMutationPreservesLatestTimestamp passes across update/disable/rotation |
| Tokens app scoped, tamper resistant, expired rejected, <=15 minutes | auth token tests, TestSocketTokenIssuanceIsSignedScopedAndBounded | PASS | Fresh gate passes |
| Atomic event/jobs/stream; validated targets; idempotent 202 replay | TestEventConcurrentPublication, TestEventExpiryAndAtomicTargetValidation, HTTP event tests | PASS | Fresh gate passes |
| Original event preserved through replay and target disable | TestReplaySurvivesTargetDisableAndReturnsOriginalPayload | PASS | Fresh gate passes |
| Target isolation and 60-second queue leases | TestEventLeaseAckControlsAndIsolation | PASS | TestEventConcurrentAckAndLeaseLeavesTerminalState: 20 raced terminal transitions pass |
| Queue limits 1–100 and wait 0–30 seconds | service/HTTP queue tests | PASS | Exact error envelopes asserted; TestLeaseLongPollBoundsInFlightRedisIO proves cancellation during stalled socket reads |
| Ack idempotency and terminal job TTL not renewed | Redis event integration | PASS | Repeated ack retains independently shortened 10-second TTL |
| Admin requeue/DLQ transitions | event HTTP and Redis tests | PASS | Fresh gate passes |
| Durable state precedes realtime notifications | TestEventNotificationsFollowDurabilityAndFailuresDoNotRollback | PASS | Fresh gate passes |
| Standard RFC6455; signed tokens; origin allowlist | WebSocket HTTP tests and E2E | PASS | Fresh gate passes |
| REST CORS disabled by default; explicit same origin allowlist as WebSocket | New CORS HTTP contract regression | PASS | TestCORSExactOriginPreflightAndAuthenticatedRequests and TestCORSRejectsUnapprovedPreflightCapabilities pass |
| App-isolated events/jobs subscriptions, ping/pong | TestWebSocketFramesAndIsolation, TestPubSubCrossInstanceAndShutdown | PASS | Fresh gate passes |
| Invalid JSON/unknown frames error; invalid UTF-8 closes safely | protocol and WebSocket UTF-8 tests | PASS | Fresh gate passes |
| Bounded outbound buffer; slow clients disconnect; graceful close | hub slow/concurrent/fatal-close tests | PASS | Fresh gate passes |
| Redis Pub/Sub reconnect and shutdown during reconnect | Pub/Sub integration | PASS | Reconnect/dial-failure shutdown passes; trusted TLS/custom dial path retained and fixture names uniquely identify connections |
| Callback exact body/HMAC headers, timeout and bounded drain | delivery callback tests | PASS | Fresh gate passes |
| Callback 2xx success; 408/425/429/5xx/network retry; other 4xx DLQ | TestClassify, callback runtime integration | PASS | Fresh gate passes |
| Five delays 1/5/15/60/300s; sixth failure DLQ; Retry-After <=300s | TestClassify, TestCallbackRedisLifecycle | PASS | Fresh gate passes |
| Callback durable claim, no duplicate dispatch, retry recovery | worker Redis tests incl reclaim/generation/dispatch fencing | PASS | Fresh gate passes |
| Graceful worker cancellation retains unfinished work | TestWorkerShutdownLeavesUnfinishedReclaimable | PASS | Fresh gate passes |
| Worker store errors counted without sensitive labels/logs | worker tests | PASS | TestWorkerObservesEveryStoreFailureWithoutSensitiveLabels passes all four boundaries |
| Function name, owner, timeout 1–30s validation | TestFunctionRegistrationValidationAndOwnerScope | PASS | Fresh gate passes |
| Cross-app invoke; owner-only handler; success/error and idempotency | function service, HTTP roundtrip, two-API integration | PASS | Fresh gate passes |
| Handler errors canonical lowercase code/message | domain RPC validation | PASS | Domain rejection table and TestFunctionFastResultConcurrentReplayAndResponderFencing prove canonical forwarding |
| Offline 503; timeout 504; no redispatch after owner accepted | function integration | PASS | TestFunctionsRedisAcknowledgedOwnerDisconnectNeverRedispatches observes claimed state, closes owner, injects duplicate hint, receives one timeout/replay |
| RPC complete serialized frames <=64 KiB | TestFunctionWireLimitAndCallerKeyNamespace, HTTP boundary tests | PASS | Fresh gate passes |
| Event/job 7-day and idempotency 24-hour retention | config defaults, Redis expiry integration | PASS | Fresh gate passes |
| Configurable prefix default relayhub for all state/streams/PubSub | TestRedisPrefixIsolation, functions/worker prefix tests | PASS | Fresh gate passes |
| Redis AOF with persistent volume and restart recovery | E2E AOF/restart and backup restore | PASS | Final E2E, backup rehearsal and retained-stack inspection pass |
| API only published 8080; private worker :9090; healthchecks; no fixed names | compose.yaml and E2E inspection | PASS | Final E2E, backup rehearsal and retained-stack inspection pass |
| Traefik/Cloudflare whole-origin and cache/upgrade guidance | deployment docs | PASS | Deployment/proxy/cache guidance reconciled against actual topology |
| JSON request/domain logs with IDs and latency; no secrets/payloads | requestlog and worker logging tests, E2E redaction | PASS | Gitleaks history/tree and runtime sentinel scans pass after exact nonsecret exclusions |
| Human docs embedded and route/content types correct | docs operations tests, checker runtime | PASS | Fresh gate passes |
| OpenAPI, event/frame schemas, llms, downloadable Skill aligned | check-contracts --self-test; generated drift checks | PASS | Fresh gate passes |
| Markdown link crawler detects supported syntax or rejects it | check-docs.py | PASS | DocumentLinks rejects reference syntax including nested blockquotes/lists and catches broken embedded anchor/image targets |
| Console desktop/mobile, keyboard focus, copy and fallback | Real browser QA | PASS | Chrome 153 screenshots, focus sequence and copy branches verified |
| E2E KEEP cleanup works after parent/temp config exits | e2e-client tests and retained live stack | PASS | TestKeptProjectCleanupNeedsNoComposeConfigOrSecrets and real retained-stack cleanup pass |
| Docs Redis URL validated consistently before side effects | test-docs-runtime.py | PASS | RedisURLSelection rejects before processes/network; all eight external-runtime tests pass after the review regression was added |
| Whole branch security, concurrency and contract review | git diff 12accf0..candidate; scans and final gate | PASS | Reviewed complete current tree and 12accf0..candidate implementation; no unresolved Critical/Important findings |
| Patched build toolchain and no reachable known vulnerabilities | govulncheck v1.8.0 | PASS | Go 1.27.1 selected in module/CI/Docker; source scans and extracted image binary scans pass |

## Route and authentication coverage

`TestRouteManifestMatchesContractAndAuthentication` and the live docs checker
verify the actual router against OpenAPI and reject a mutated admin/app auth swap.

| Route | Authentication and direct behavior evidence | Verdict |
|---|---|---|
| GET /healthz | Public; liveness does not call Redis | PASS |
| GET /readyz | Public; real Redis access, 503 when unavailable | PASS |
| GET /metrics | Public; Prometheus content and bounded counters | PASS |
| POST /api/v1/apps | Admin bearer; 201 one-time credentials | PASS |
| GET /api/v1/apps | Admin bearer; secret-free collection | PASS |
| GET /api/v1/apps/{appID} | Signed owning app; unrelated records hidden | PASS |
| PATCH /api/v1/apps/{appID} | Signed owning app; merged validation and no re-enable race | PASS |
| DELETE /api/v1/apps/{appID} | Admin bearer; disabled authentication fails | PASS |
| POST /api/v1/apps/{appID}/rotate-secret | Admin bearer; atomic key-index replacement | PASS |
| POST /api/v1/socket/token | Signed app; scope and <=15-minute lifetime | PASS |
| POST /api/v1/events | Signed producer; idempotent 202 and atomic target jobs | PASS |
| GET /api/v1/events/{eventID} | Signed source/target; unrelated reads hidden | PASS |
| GET /api/v1/queue | Signed target; limits, waiting, lease recovery/isolation | PASS |
| POST /api/v1/events/{eventID}/ack | Signed target; idempotent 204 and preserved expiry | PASS |
| GET /api/v1/jobs/{jobID} | Signed source/target; ownership isolation | PASS |
| POST /api/v1/jobs/{jobID}/requeue | Admin bearer; legal state transition/new callback budget | PASS |
| POST /api/v1/jobs/{jobID}/dead-letter | Admin bearer; durable terminal transition | PASS |
| GET /ws | Scoped token and exact browser origin; real RFC6455 frames | PASS |
| POST /api/v1/functions | Signed owner; name uniqueness, 1–30-second deadline | PASS |
| GET /api/v1/functions | Signed owner; own registrations only | PASS |
| DELETE /api/v1/functions/{functionID} | Signed owner; unrelated deletion hidden | PASS |
| POST /api/v1/functions/{functionID}/invoke | Signed caller; success/error/replay/offline/timeout | PASS |
| OPTIONS preflight | Explicit origin/capability allowlist; actual requests still authenticate | PASS |
| /docs and /docs/* | Redirect, embedded artifacts, MIME, attachment, JSON missing/method errors | PASS |

The route-specific HTTP tests in `internal/httpapi/{apps,events,functions,operations,websocket,cors}_test.go`
also verify standard status/error contracts. Redis tests exercise persistence and
concurrency independently of HTTP test doubles; the separate E2E client signs
requests without importing RelayHub signing helpers.

## Regression evidence and deferred findings

Review follow-up plan (recorded before the fixes): reproduce the blockquote
shortcut-reference bypass before making container-nested reference detection robust. Preserve original Redis
transport options/TLS and wrap the configured dialer in the reconnect fixture;
give each fixture a unique Redis client name. Add behavioral regressions for TLS,
custom dialing and independent identities, then rerun docs/contracts and affected
real-Redis integration. These are verification-tooling fixes; public application
routes, data and deployment behavior do not change.

| Finding | RED evidence | Final outcome |
|---|---|---|
| Empty callback hostname and insecure link-local destinations | V/red-validation.log: three unsafe callbacks accepted | Strict validation passes; documented private/loopback/local-name HTTP exception retained |
| Stale application timestamp | V/red-timestamp.log: update replaced later disable timestamp | Atomic max timestamp across update, disable and rotation; fractional and whole-second cases pass |
| Ambiguous handler error keys | V/red-validation.log: wrong-case/duplicate/null overwrite accepted | Exact-key token parsing and non-null strings pass |
| Noncanonical forwarded error | V/red-canonical.log: raw whitespace/order forwarded | Rebuilt validated code/message JSON stored and returned |
| Missing worker store-error observations | V/red-metrics.log: missing count at load/start/finish/XACK | All four failures increment bounded store_error; sensitive details absent |
| Markdown reference/HTML links silently ignored | V/red-links.log: expected checker assertion absent | Unsupported references clearly rejected; HTML links/images crawled |
| KEEP cleanup relies on deleted config/credentials | V/red-cleanup.log: old Compose command fails controlled boundary | Exact printed label-scoped command passes unit and live after-exit test |
| Uppercase docs-test Redis URL accepted too late | V/red-url.log: process started before validation | Rejected before process/network; external-Redis cleanup suite passes |
| Configured REST CORS missing | V/red-cors.log: missing allowed-origin header and rejected preflight contract | Shared explicit origin allowlist; default disabled; no auth bypass |
| Browser implicit favicon error | V/red-browser.log: /favicon.ico 404 console error | Explicit empty data favicon; clean normal browser console |
| Vulnerable build toolchain | V/govulncheck.log: eight reachable Go stdlib findings | Go 1.27.1 module/CI/Docker; zero reachable source or binary findings |
| Config test inherited Redis password | First gate TestLoadUsesDocumentedDefaults failed with injected fixture password | Test reset includes RELAYHUB_REDIS_PASSWORD; scoped Compose credentials; fresh default test/gates pass |
| Quoted shortcut reference bypasses link rejection | V/review-red-links.log: quoted `[Missing reference]` and quoted definition targeting missing-review-target.md yield no assertion; six container cases fail | Repeated blockquote/list markers recognized; seven container variants and a contract negative control pass |
| Reconnect test loses TLS/custom dialer and shares client identity | V/review-red-redis.log: working trusted TLS proxy fails with EOF; two real connections expose one name | Copy initialized options, clone TLS config, wrap existing dialer, create a fresh client-owned push processor and UUID client name; real transport/identity regressions pass |

Coverage concerns required no production change: Redis poll cancellation already
had direct stalled-read evidence; ack TTL and concurrent lease/ack assertions were
strengthened. Pub/Sub reconnection and shutdown during reconnect now have direct
real-Redis coverage. An acknowledged owner disconnect is now tested with another
eligible session and a duplicated notification; it times out once without
redispatch and replays the same terminal outcome.

The SDD ledger is ignored/uncommitted, so its duplicate historical ruling lines
were not rewritten. Every unique ruling is preserved below.

## Fresh full gate

The release gate below ran for `ff8c191`; the later scoped review verification is
recorded in the next section. All final commands exit **0**. V/final-gate.json stores exact argv, durations and
counts; matching V/final-*.log files store output. Counts include named subtests
and parent test pass events; packages with no tests are listed separately.

| Command | Exit | Evidence |
|---|---:|---|
| test -z "$(gofmt -l .)" | 0 | No formatting drift |
| go vet ./... | 0 | Clean |
| go test -json -count=1 ./... | 0 | 231 passed, 0 failed, 0 skipped; 10 tested packages |
| go test -race -json -count=1 ./... | 0 | 231 passed, 0 failed, 0 skipped; 10 tested packages |
| go test -race -json -tags=integration ./... -count=1 -timeout=180s | 0 | 274 passed, 0 failed, 0 skipped; 11 tested packages; 33.05 seconds |
| go test -race -json scripts/e2e-client.go scripts/e2e-client_test.go -count=1 -timeout=20s | 0 | 6 passed, 0 failed, 0 skipped |
| ./scripts/build-skill.sh | 0 | Deterministic Skill ZIP/OpenAPI snapshot |
| ./scripts/build-llms.sh | 0 | Deterministic agent reference |
| python3 scripts/check-docs.py | 0 | Live API/Redis flows, MIME/bytes, schema/link/drift checks |
| ./scripts/check-contracts.sh --self-test | 0 | All 15 deliberate negative mutations rejected |
| docker build --platform linux/arm64 -t relayhub:release-arm64 . | 0 | Patched non-root distroless image; 56.16 seconds |
| docker build --platform linux/amd64 -t relayhub:release-amd64 . | 0 | Patched non-root distroless image; 41.54 seconds |
| docker compose config --quiet | 0 | Generated credentials scoped to this command, never printed |
| ./scripts/e2e.sh | 0 | All nine live stages; 33.24 seconds; isolated cleanup verified |
| ./scripts/e2e.sh --backup-rehearsal | 0 | UID 999, no capabilities, exact AOF/manifest bytes and 700/600 permissions |
| python3 V/browser-runtime.py | 0 | Real embedded API docs, two viewports and copy branches |
| python3 scripts/test-docs-runtime.py | 0 | Seven tests against owned external Redis; namespace cleanup, unrelated DB preservation, early URL rejection and link checks |
| RELAYHUB_E2E_KEEP=1 ./scripts/e2e.sh, then printed cleanup | 0 | Real retained stack inspected after process exit; exact printed command succeeds without config/credentials |

All normal and KEEP acceptance resources were removed. Final container/network/
volume inventory contains no Task 9 or RelayHub acceptance resources. The two
release image tags are retained as local build outputs. No external infrastructure
was changed.

## Scoped review verification

Both approved review Minors are closed. The follow-up changes only verification
scripts, integration fixtures/tests and review records. No production Go source,
public API/configuration, schemas, console behavior or deployment artifacts changed;
the public human/agent docs remain accurate and need no content regeneration.
The live docs/contract checks below still verify their generated artifacts.

The TLS regression forwards a locally trusted TLS endpoint to real Redis and
first confirms the original client can connect; it uses certificate verification,
not an insecure override. The derived client must also connect through the custom
dialer. Two simultaneously live clients must have distinct names in Redis CLIENT
LIST. The existing reconnect test finds and kills only its generated name, then
checks recovered delivery and bounded close during a forced dial outage.

| Command | Exit | Evidence |
|---|---:|---|
| python3 scripts/test-docs-runtime.py DocumentLinks | 0 | V/review-green-links.log: both tests pass, including seven nested container cases |
| go test -race -tags=integration ./internal/store/redisstore -run TestPubSub -count=1 -timeout=60s -v | 0 | V/review-green-redis.log: all four Pub/Sub tests pass, including TLS/custom dialing and independent names |
| go test -race -json -tags=integration ./internal/store/redisstore -count=1 -timeout=90s | 0 | V/review-integration.log: 45 test/subtest passes, zero failures/skips; 27.374 seconds |
| python3 scripts/check-docs.py | 0 | V/review-docs.log: live API/Redis, links, schemas, route parity and generated drift checks pass |
| ./scripts/check-contracts.sh --self-test | 0 | V/review-contracts.log: all 16 negative controls rejected, including quoted shortcut references |
| python3 V/external-docs-tests.py | 0 | V/review-runtime.log: all eight tests pass with external Redis and owned-container cleanup |
| go test -count=1 ./... | 0 | V/review-unit.log: all ten tested packages pass |
| gofmt check, go vet -tags=integration ./internal/store/redisstore, git diff --check | 0 | No formatting, vet or whitespace errors |

No review-test containers remain. The release browser, image and Compose evidence
above is retained for the unchanged application artifacts; it was not rerun for
these verification-only fixes.

## Live Compose and persistence evidence

Normal acceptance and KEEP acceptance both passed creation, real signed
publication/replay, isolated WebSocket delivery, durable queue/ack, one-handler
RPC with success/offline/timeout replay, callback retry then success, permanent
400 dead-letter, exact docs/ZIP bytes, process and Redis restart recovery, and
generated-ID log redaction.

KEEP project `relayhub-e2e-e8ecf4129463` was inspected with `docker compose ps`
after the acceptance parent exited. API, worker and Redis were all running and
healthy. Only API published a host port (`127.0.0.1:60597 -> 8080/tcp`); worker
had no publication, Redis only its internal 6379. Redis owned the named
`relayhub-e2e-e8ecf4129463_relayhub-data` volume at /data. API/worker were read-only,
non-root and on the one project network. The E2E AOF test recreates/restarts Redis
and verifies retained queued state; the separate backup rehearsal checks exact
restored private file bytes and permissions.

V/kept-compose-ps.jsonl, V/kept-safe-inspection.txt, V/kept-e2e.log,
V/kept-cleanup.log and V/kept-live.log record this evidence. Inspection deliberately
omits container environment. The exact printed cleanup ran from an unrelated
directory with all RELAYHUB_/COMPOSE_ environment removed after temporary files
were deleted. Subsequent label/image inspections found no retained resources.

## Security review

`govulncheck v1.8.0` initially found eight reachable standard-library
vulnerabilities in host Go 1.26.3. Go's [official release metadata](https://go.dev/dl/)
identified Go 1.27.1 as stable. Module minimum, CI-selected version and Docker
builder now use 1.27.1. Patched source scans on Linux arm64 and amd64 exit 0 with
zero reachable or imported-package vulnerabilities. One module-only advisory,
GO-2026-5024 in x/sys/windows, is unreachable and outside the Linux/macOS product
targets; it is documented rather than hidden. Both actual image binaries were
extracted, identified as Go 1.27.1, and scanned with `govulncheck -mode=binary`:
**No vulnerabilities found.** V/image-*-metadata.log and V/image-*-govulncheck.log
prove the shipped artifacts.

Gitleaks v8.30.1 scanned all 22 existing commits (2.84 MB) and the full current tree.
Its initial 12 findings are copies of two nonsecret examples: the operation
idempotency value `calculate-order-123`, and the RFC6455 published sample nonce
`dGhlIHNhbXBsZSBub25jZQ==`. Neither authenticates an application. Exact match
exclusions for those two header/value pairs only are in V/gitleaks-reviewed.toml;
no file/path blanket exclusions were added. Reviewed history and current-tree
scans exit 0 with no leaks; raw findings were redacted. Generated embed/llms copies
account for the repeated hits. Required .env.example credentials remain empty;
API credentials/signatures in tests are isolated fixtures or generated in memory.

Manual source inspection plus capture tests verified logs use generated request,
app, event, job, function and invocation IDs with bounded outcomes/latency.
Raw URLs/queries, caller request IDs, headers, tokens, signatures, callback URLs,
body/input/result/error detail are excluded. Live E2E scans every generated
credential and sentinel payload against API/worker logs and verifies positive
ID/outcome evidence. Callback redirects are not followed, origin allowlists do
not use wildcards, body/frame limits remain bounded, and Redis/API/worker
container hardening is enforced by contract tests and live inspection.

## Browser and documentation evidence

Chrome 153.0.8010.12 opened actual API-embedded /docs/. Both 1440x900 and 390x844
had zero document-width overflow and zero normal console errors. Screenshots were
visually inspected: readable desktop two-column/mobile single-column cards,
navigation, code wrapping and copy controls. Exact viewport and full-page captures:
V/docs-1440x900.png, V/docs-390x844.png and corresponding *-full.png files.

Keyboard order was Skip to documentation, brand, User, Developer, API Reference,
Skills, Start integrating. Real clipboard copy succeeded and restored focus.
Permission-denied fallback succeeded; forced manual fallback exposed and selected
the full Skill textarea; a forced fetch failure produced the documented recovery
state and restored the enabled button. The intentional injected 503 is a negative
test, not a normal console error. V/browser-qa.json records assertions and
V/docs-{denied-fallback,manual,fetch-failure}.png captures these states.

Human/internal docs were reconciled for callback policy, monotonic timestamps,
RPC errors, CORS, build minimum, worker metrics and cleanup/tooling.
OpenAPI, client-frame JSON Schema, Skill OpenAPI snapshot/ZIP, llms-full and the
embedded snapshot were regenerated via go generate ./web. Event/server schemas
and the llms index needed no content change: their structures, paths and discovery
links are unchanged; they were still validated. Historical before-code work notes
remain clearly labeled history, with current reconciliation adjacent.

## Preserved decisions and limits

- Independent RelayHub repository/branch provides workspace isolation.
- All Redis state, Streams and Pub/Sub use the configurable prefix, default relayhub.
- Initial callback plus five retries uses 1/5/15/60/300 seconds and DLQs the sixth failure.
- Worker operations stay private on :9090 and are not published by Compose.
- HTTP bodies remain 1 MiB; each complete serialized WebSocket/RPC frame remains 64 KiB.
- Structured logs expose generated IDs/bounded outcomes and exclude sensitive values.
- Documentation scope additions intentionally reconcile already-shipped behavior.
- Stale review-baseline provenance is corrected to 12accf0.
- Patched Go 1.27.1 is the supported build minimum after the demonstrated security gap.

The approved MVP limitations remain: at-least-once delivery, best-effort realtime
notifications with queue recovery, possible recent-write loss with every-second
AOF after a host crash, no exactly-once effects, no offline RPC queue, no Socket.IO,
no multi-node control plane and no arbitrary code execution. Tests cover the
single-node homelab contract; this audit makes no external production-load claim.
These are documented product limits, not incomplete requirements.
