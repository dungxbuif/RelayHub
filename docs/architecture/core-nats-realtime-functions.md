# Core NATS realtime and remote functions

RelayHub v1 keeps the public `/ws` frames and function HTTP endpoints stable
while replacing Redis Pub/Sub with private Core NATS subjects. PostgreSQL is the
authority for function catalog, caller idempotency, invocation ownership,
deadlines and terminal replies. Core NATS only transports best-effort observation
frames and live function dispatch signals.

## Observation bridge

Event and job notifications are published as an application-scoped envelope on
`rh.v1.realtime.<app_token>`. Every API gateway subscribes to the wildcard and
validates that the envelope application hashes to the received subject before
calling the local WebSocket hub. A lost notification does not affect the durable
event or queue state. Clients recover durable work through the queue APIs.

## Function request and reply

A WebSocket subscribed to `functions` makes its gateway eligible for that
application. Eligible gateways join one queue group on
`rh.v1.rpc.<app_token>.*`; gateways without a local handler do not join. The
invoking gateway publishes the canonical invocation frame to the exact function
subject and requests an acceptance reply on its instance-scoped
`rh.v1.rpc.reply.<instance_token>` subject.

The selected gateway must reserve the PostgreSQL invocation for one authenticated
application and connection, acknowledge that reservation, and enqueue the frame
before replying `accepted`. A disconnect or full local queue releases an
unacknowledged reservation. The gateway first loads the immutable invocation,
checks the subject application/function and all frame fields against it, then
constructs the delivered frame from that record. Once the invoking gateway
receives a matching acceptance, it never
redispatches it. Ambiguous request replies may be retried only while PostgreSQL
still permits a pending reservation; the database fence prevents a second owner
from receiving an already acknowledged invocation.

The browser result is accepted only when application, connection, invocation,
claimed state and persisted deadline all match. PostgreSQL stores a successful or
handler-error result once. Concurrent and later calls with the same caller and
idempotency key replay the stored outcome for 24 hours without dispatch. Pending
or reserved work becomes `unavailable` at its claim deadline. Claimed work becomes
`timeout` at its function deadline. No responder maps to `503
function_unavailable`; expiry after an accepted handler maps to `504
function_timeout`. Late or mismatched results receive the existing generic
`invalid_rpc_result` response.

Each caller subscribes before dispatch to
`rh.v1.rpc.result.<invocation_token>`, where the token is an opaque SHA-256/base32
derivative. A result hint has no payload. `FunctionService.CompleteResult` commits
through the invocation store before publishing the hint; notification failure
does not reject an accepted result. Every waiter uses a normal subscription, so
concurrent same-key callers on different gateways all wake. Reads occur after
subscription/dispatch, on hints and at the persisted claim/function deadline.
A dropped hint is recovered at that deadline without periodic database polling.
Watches unsubscribe on caller cancellation, completion and bridge shutdown.

`FunctionResultNotifier` separates broker wakeups from PostgreSQL storage.
Without that transport the PostgreSQL watch is a deadline-only fallback; the
legacy Redis store retains its own event-driven watch until process cutover.
Acceptance uses monotonic state separate from its coalescing wakeup channel.
The state update and each request publication share one lock, preventing an
already-received success from being overtaken by a retry or overwritten by a
rejection. Replies for another invocation cannot accept the pending request.

## Verification

### Task 9 review corrections (decision before implementation)

The review of `e78b39b` identified three gaps. Result waiters must subscribe to a
Core NATS invocation-scoped wakeup before dispatch, read PostgreSQL after each
wakeup, and recover a missed wakeup at the persisted claim/function deadline.
Completing a result commits PostgreSQL before publishing the hint; a failed hint
must not undo or reject the stored result. Concurrent callers sharing an
idempotency key each receive the hint. There is no periodic database polling.

An incoming private NATS request is still untrusted transport data. Resolve its
invocation from the function backend, verify the application/function and frame
against immutable persisted fields, and construct the delivered frame from that
record before the existing reservation/acknowledgement fence. Altered name,
input, deadline or function must not claim or reach a handler.

Acceptance becomes monotonic request state: rejection hints may coalesce, but
once a matching success arrives it cannot be dropped or overwritten. Checking
that state and publishing another attempt must share synchronization, so a
success already queued at the bridge prevents another request publication.

Verification will cover non-polling waiters, persist-before-wakeup, concurrent
replays, missed-wakeup deadline recovery, forged NATS frames, acceptance ordering,
and real PostgreSQL plus Core NATS behavior. Public function guidance and its
generated agent-readable surface will describe the same recovery contract.

Focused tests cover application and connection fences, fast replies, stored
terminal replay, no responders, deadline boundaries and the absence of
redispatch after acceptance. Integration tests run two NATS bridge instances and
PostgreSQL to prove one eligible owner receives a request while observation
frames reach each matching local hub with Redis unavailable.

### Review verification — 2026-09-12

RED evidence before the fixes:

- `go test ./internal/service -run 'TestFunctionWaitsForResultWakeupWithoutPeriodicReads|TestFunctionCompletionPublishesOnlyPersistedResults' -count=1`
  failed because the idle call read again after about 25 ms and completion
  published no hint.
- `go test -tags=integration ./internal/realtime -run 'TestNATSAcceptanceSuccessSurvivesRejections|TestCoreNATSRejectsForgedInvocationFramesBeforeClaim' -count=1`
  failed for all four altered fields (function ID, name, input, deadline): each
  reached the handler and changed the reservation to `claimed`. The acceptance
  test also showed a success lost behind the first rejection.

GREEN verification after the fixes:

- `go test -race ./internal/realtime ./internal/service ./internal/httpapi -count=1`
- `go test -race -tags=integration ./internal/realtime ./internal/service -count=1`
- `go test -race -tags=integration ./internal/store/postgres -run 'TestPostgresFunctionInvocation|TestPostgresCoreNATSResultsWakeConcurrentIdempotentWaiters' -count=1 -timeout=180s`
- `go vet ./internal/realtime ./internal/service ./internal/store/postgres ./internal/httpapi`
- Core NATS routing/wakeup/acceptance tests also passed five consecutive runs;
  the acceptance burst publication test passed twenty consecutive race runs.

The PostgreSQL test used a disposable PostgreSQL 17 container and three embedded
Core NATS gateways. Eight simultaneous same-key waiters produced one dispatch,
seven replays and one hint after a committed result; both caller gateways woke
without idle database reads. A deliberately dropped hint preserved success and
returned it on the function-deadline read. Scoped watch cancellation and bridge
shutdown release their NATS subscriptions. Existing PostgreSQL owner/connection,
expiry and terminal-replay fences remain covered.

No public endpoint, JSON schema or configuration field changes in this fix.
The public functions guide is updated. Shared `llms`/Skill/embed regeneration
and the combined documentation gate belong to the concurrently active Task 5
finalization, after this Task 9 source/documentation commit.
