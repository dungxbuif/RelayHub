# Task 9 report — RelayHub MVP release candidate

Status: DONE. All 50 requirement rows and all route rows in
`docs/reviews/MVP-VERIFICATION.md` are PASS. No unresolved Critical/Important
finding, failed requirement or unverified requirement remains.

Release commit: `ff8c191` (`chore: verify RelayHub MVP release candidate`).
Review follow-up subject: `fix: close release verification review gaps`.
Review baseline: `12accf0`, confirmed by the orchestrator because the plan's
`7f7c3f2` is not present in this independent repository. Work stayed on
`feat/relayhub-mvp`; no subagents, external deployment, image push or domain change.

## Implementation

- Reject empty callback hostnames and insecure link-local destinations; preserve
  documented loopback/private/local-name HTTP exceptions.
- Atomically retain the newest application updated_at across update, disable and
  rotation, including RFC3339Nano fractional ordering.
- Parse RPC handler-error fields with exact lowercase names, unique keys and
  non-null strings; store/forward canonical validated JSON.
- Count load/start/finish/XACK store errors with the bounded store_error outcome,
  without sensitive error details or counting expected stale/missing claims.
- Add the missing configured HTTP CORS path using the same explicit origin list
  as WebSocket. Default CORS stays disabled; actual requests still authenticate.
- Reject unsupported Markdown reference links clearly, crawl embedded HTML
  links/images, reject uppercase docs-test Redis URL schemes before side effects.
- Print a self-contained KEEP cleanup command based only on the unique project
  label, independent of secrets, repository path and deleted temporary config.
- Add an explicit empty favicon to eliminate an actual Chrome 404 console error.
- Upgrade module/CI-selected and Docker build toolchain to Go 1.27.1 after the
  vulnerability scan found eight reachable issues in the old standard library.
- Fix config-test environment isolation for the Redis password override.

## Deferred findings

All twelve actionable deferred categories were resolved or verified. Existing
`TestLeaseLongPollBoundsInFlightRedisIO` directly proves cancellation during a
stalled real Redis socket read, so it was cited without duplicating coverage.
Repeated ack now has an independently shortened TTL assertion; concurrent ack
and lease are raced twenty times and checked for terminal state/no redelivery.
HTTP error tests assert exact machine codes. New real-Redis tests prove Pub/Sub
reconnect, close during failed reconnect, and acknowledged owner disconnect with
another eligible session and duplicate notification without redispatch.

The ledger is ignored/uncommitted, so historical duplicate lines were preserved.
Unique rulings retained: isolated repository; configurable default Redis prefix;
initial attempt plus five retries; private :9090 worker listener; 64 KiB complete
socket frames versus 1 MiB HTTP bodies; generated safe logs; intentional broader
documentation reconciliation. New review rulings correct the stale baseline and
select the patched Go build minimum.

## RED → GREEN evidence

The ignored `verification/` directory contains red-validation.log,
red-timestamp.log, red-canonical.log, red-metrics.log, red-links.log,
red-cleanup.log, red-url.log, red-cors.log, red-browser.log and the original
govulncheck.log. They demonstrate the unsafe acceptance, timestamp regression,
raw error forwarding, absent metrics, undetected links, broken old cleanup,
late URL validation, missing CORS, favicon request and vulnerable toolchain.
All focused regressions and the affected suites pass after the narrow fixes.

## Fresh release verification (ff8c191)

- Formatting and go vet: exit 0.
- Unit: 231 passing test/subtest events; race: 231; real Redis race integration:
  274; explicit E2E client race tests: 6. Zero failures and zero skips.
- Skill and llms builders, live docs checker, 15 contract negative controls:
  exit 0. Seven external Redis/docs tests also pass with owned container cleanup.
- Linux arm64 and amd64 release images: build exit 0. Extracted image binaries
  identify Go 1.27.1; both binary vulnerability scans report no vulnerabilities.
- Compose validation, complete nine-stage E2E, UID 999 AOF backup/restore rehearsal:
  exit 0. Runtime tests cover HMAC bytes, queue/ack, RFC6455, RPC,
  callback retry/DLQ, API/worker/Redis restart persistence and log redaction.
- Real Chrome 153 at 1440x900 and 390x844: screenshots visually reviewed; no page
  width overflow or normal console errors. Keyboard sequence, real clipboard,
  denied-copy fallback, manual selection and forced-fetch-failure recovery pass.
- KEEP stack inspection after parent exit: exactly three healthy services, API
  only published, private Redis AOF volume. The exact printed cleanup succeeded
  from an unrelated directory without RELAYHUB_/COMPOSE_ variables or temporary
  config. No owned containers, networks, volumes or acceptance image remain.
- Gitleaks v8.30.1 scans history and current tree with no leaks after exact
  exclusions for two public nonsecret protocol examples. The twelve initial hits
  are documented, including repeated embed/llms copies; no blanket exclusion.
- govulncheck v1.8.0 source scans for Linux arm64/amd64: zero reachable/imported
  package vulnerabilities. One x/sys/windows module-only advisory is unreachable
  and outside these targets. Both actual release binary scans are clean.

Full argv, exit codes, counts, duration, live evidence and limits are in
`docs/reviews/MVP-VERIFICATION.md` and ignored `verification/final-gate.json`.

## Approved review follow-up

Both review Minors are closed with strict RED → GREEN evidence:

- `verification/review-red-links.log`: quoted shortcut references targeting a
  missing Markdown file bypassed rejection (six failing container cases).
  The checker now recognizes repeated blockquote/list markers. Both DocumentLinks
  tests pass, including seven nested container variants; the contract self-test
  adds the exact quoted fixture as its sixteenth negative control.
- `verification/review-red-redis.log`: the original reconnect fixture lost a
  confirmed working, certificate-verified TLS connection (EOF) and exposed the
  same Redis client name on two simultaneous connections. The fixture copies
  initialized Redis options, clones TLS config, wraps the effective custom/default
  dialer, uses a fresh client-owned push processor and generates a UUID name.
  The existing reconnect/shutdown test targets that unique name.

Fresh scoped checks all exit 0: four Pub/Sub race tests; full Redis store race
integration (45 test/subtest passes, zero failures/skips); both DocumentLinks
tests; live docs checker; all 16 contract negative controls; eight external
Redis/docs tests; all ten unit-test packages; gofmt, integration-tagged vet and
diff whitespace checks. Exact commands and `verification/review-*.log` evidence
are listed in the verification document. Owned test containers were removed.

This follow-up changes verification scripts/fixtures, tests and internal review
records. Public human/agent docs, schemas, API behavior and deployment artifacts
need no content change; live checks confirm their generated artifacts remain
aligned. The browser, image/security and Compose evidence belongs to the original
release gate above and was not repeated for these verification-only fixes.

## Documentation and handoff

Internal and public API/auth/function/reliability/deployment/operations/security
docs match the final behavior. OpenAPI, RPC client schema, Skill reference/ZIP,
llms-full and embedded docs were regenerated; drift checks pass. Event/server
schemas and the llms discovery index need no content changes because their
structures and paths are unchanged; all are validated. The approved spec records
the security-driven toolchain minimum. Historical before-code work notes remain
explicitly historical beside current reconciliation.

Only the two local release image tags are retained. The final worktree is clean
after the release commit. Ignored verification artifacts are available locally;
they are not application source or shipped secrets. The usual MVP limits remain
documented: at-least-once effects, best-effort realtime hints, single-node scope,
every-second AOF crash window and no offline function queue.
