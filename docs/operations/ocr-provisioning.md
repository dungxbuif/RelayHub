# OCR provisioning (2026-09-20)

An existing password-authenticated admin created the following production-stage
integration resources through the public API, using session cookie and CSRF:

| Resource | ID |
| --- | --- |
| ocr-proxy | app_BnJ-pUWlyM3AAv_C_YPZhQ |
| ocr-worker-mac | app_eD8zPc4TOgPHm01asPe8LA |
| ocr-jobs | sub_ef0b92cf-8b8e-4644-bc17-6d83fd22ecae |

All native replicas must use the same subscription. Visibility is 120 seconds,
renewable up to 300 seconds at a time, with a one-hour total lease ceiling.
Retention is seven days, batch size one, max in-flight four and max attempts twenty.
These resources are provisioned but do not indicate that OCR has cut over.

Run `backend/scripts/provision-ocr.mjs` with `RELAYHUB_ADMIN_EMAIL` and a password
on stdin. Credentials are saved outside git with mode 600, never printed.
The default operator path is `~/.config/relayhub/ocr.json`. The script refuses to
duplicate an app when its one-time credentials are missing and logs out afterward.

Before provisioning, a read-only DB inventory confirmed zero application, event,
delivery, queue, function and outbox records, and one admin. No delete/reset was
needed. Preserve admin and real OCR data; cleanup authorization covers tests only.

Verification: backend `go test ./...` and admin UI tests (19/19) pass after migrating
obsolete bearer fixtures to password/session/CSRF. Bearer-rejection tests remain.
Integration source lives in mac-ocr; see its `docs/RELAYHUB_INTEGRATION.md` for
cutover gates. Public API behavior is unchanged by provisioning and fixture updates.

## Pre-integration gate

User requested fixes and tests before integration. Freeze OCR cutover and deployment.
Run the PostgreSQL integration suite against disposable local testcontainers only,
with `RELAYHUB_TEST_POSTGRES_URL` unset. The harness truncates tables, so it must
never be pointed at the production database. Cover a single delivery contested by
multiple consumers, a stale receipt after re-lease, duplicate ACK, and heartbeat
limits as well as the existing queue tests. Run race detection and both SDK suites.
Do not count skipped container tests as a successful integration verification.

Found during SDK verification: both SDKs still sent removed admin bearer tokens.
Replace this with an explicit short-lived admin session (cookie and CSRF token),
separate from app HMAC credentials. Management helpers must reject missing session
credentials, block redirects, and never include session values in errors. Document
the migration and regenerate both downloadable SDKs and the agent-readable guide.

Additional gate: queue workers must inspect per-receipt heartbeat/settlement
responses (HTTP 200 can contain `invalid_receipt`). Cancel the handler context on
uncertain/lost lease and do not settle afterward. Node handlers receive an abort
signal; user code must cooperate. Surface settlement failures without an unhandled
promise rejection. This does not claim exactly-once side effects.

Verified before release: PostgreSQL integration suite passed (38 tests in the
JSON-counted run, no skips), including concurrent claim and stale-owner fencing.
Three contention/fencing tests also passed five repetitions with race detection.
Go SDK race tests, TypeScript SDK 16 tests, admin UI 19 tests and documentation
build passed. Regenerate embedded assets before the final backend race suite;
running generation concurrently with its snapshot test produces a transient mismatch.
