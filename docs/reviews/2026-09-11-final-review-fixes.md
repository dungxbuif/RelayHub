# Final branch review fixes

Status: all seven code/documentation findings verified; feature-completeness
handoff passes. Deployment acceptance is outside the requested scope and remains
pending because Docker cannot start new containers. The current gate and deferred
environment checks are recorded in [MVP-VERIFICATION.md](./MVP-VERIFICATION.md).

## Intended behavior and decisions (before implementation)

1. Reject invalid UTF-8 event JSON before idempotency lookup, persistence or
   notifications. Reuse strict JSON-object validation and preserve valid Unicode
   and raw numeric precision. Verify a signed HTTP request with a real WebSocket,
   plus repository/notification absence and valid publication on the same key.
2. Partial application updates must merge against the latest stored fields.
   Add optimistic compare-and-swap over the editable fields, retry read/merge/
   validation on contention, and retain atomic disable/rotation and monotonic
   timestamps. No new public revision field or API change is needed.
3. Give each real-Redis integration fixture a unique prefix and clean only its
   keys. Derived clients must preserve transport and deliberate shared prefixes;
   replace hardcoded default-prefix probes. Reproduce and rerun the exact CI
   shared-Redis environment, and prove unrelated keys survive cleanup.
4. Export bounded HTTP request counts/latency and event publication outcomes.
   Labels use method, registered route template, status and fixed outcomes only;
   never IDs, URL/query values, credentials, types or payloads. Fresh publication
   and idempotent replay count separately. Verify meaningful scrape deltas and
   privacy in HTTP tests and live E2E.
5. Preserve unexpected Redis GET errors from LoadCallback so workers observe
   store_error. Only missing/mismatched/expired claims are ordinary conflicts;
   exercise actual Redis wrong-type and conflict cases.
6. Make each of the twenty ACK/lease races publish with a distinct key, assert
   non-replay, distinct IDs and pending state before racing.
7. Correct frame-size guidance: inbound WebSocket messages and complete RPC
   envelopes are limited to 64 KiB; outbound event notifications follow the
   accepted event size (HTTP body limit 1 MiB) and include envelope overhead.

## Affected documentation and verification

Update internal architecture/reliability/operations and release records; public
event/app/operations/protocol docs and agent references as relevant. Regenerate
Skill, llms and embedded docs after source documentation changes. Preserve route
and schema compatibility while clarifying applicable limits.

Record RED/GREEN logs under the ignored Task 9 verification directory. Run focused
regressions, exact CI shared-Redis integration, unit/race, formatting/vet,
docs/contracts, both architecture images, Compose/live E2E/backup and affected
security scans. Reconcile MVP-VERIFICATION.md and task-9-report.md with actual
results; do not reuse old results as proof for changed application artifacts.

The first broad concurrent gate passed unit/race/docs/contracts but the worker
binary smoke test hit its 3-second shutdown assertion. Give that test an owned,
bounded HTTP client without pooled idle connections and drain responses before
close; this removes shared-client connection state from the shutdown observation.
Recheck the real process repeatedly and rerun integration before claiming success.

Metrics must also distinguish unexpected storage failure from a durable publish:
test a failing repository and a recovered handler panic before finalizing the
instrumentation, so an unwound publication cannot increment `published`.
