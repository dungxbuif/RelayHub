# Callback delivery on JetStream

Status: implementation note for v1 Task 8.

## Intended behavior

Callback deliveries are accepted in the same PostgreSQL transaction as their
event and outbox record. The private `RH_CALLBACKS` stream wakes a bounded
worker; PostgreSQL remains the authority for eligibility, attempt count,
retry time and terminal state. Broker messages contain an internal delivery ID
and event snapshot, never a public NATS subject or credential.

Immediately before an HTTP request, the worker atomically locks the callback
delivery and re-reads the target application, callback endpoint and current
credential. It records the credential version, callback URL, attempt number,
worker token and lease deadline. A disabled application, changed delivery mode,
missing endpoint or expired event ends that callback sink without dispatch.
A live lease delays another worker; an expired lease may be fenced and resumed
as a new at-least-once attempt.

The existing callback HTTP contract is unchanged: byte-exact request body,
HMAC signature, no redirects, bounded response drain, six total attempts and
the documented retry schedule including capped `Retry-After`. Success,
retry or dead-letter state is persisted before ACK or delayed NACK. Permanent
and exhausted failures are published to `RH_DLQ` with a deterministic message
ID before the callback message is ACKed. A redelivery after a persistence/ACK
crash observes the terminal PostgreSQL state and cannot repeat a completed HTTP
request. Receivers must still deduplicate an ambiguous request that completed
remotely before the worker crashed.

## Schema and worker contract

Migration `007_callback_deliveries.sql` adds callback lease, retry, endpoint
snapshot, credential-version and dead-letter publication fields to the existing
`deliveries` table. `delivery_attempts` records one row per reserved callback
attempt; completion updates that same row idempotently.

The JetStream worker uses explicit ACK, delayed NACK and progress signals with
a bounded `MaxPending` value. Cancellation drains the broker subscription and
stops accepting work. Redis callback code remains available during development
until the v1 cutover task removes the legacy data plane.

## Verification plan

Focused tests cover successful persistence-before-ACK, transient delayed NACK,
`Retry-After`, permanent/exhausted DLQ, crash redelivery, stale lease fencing,
callback and credential changes before dispatch, malformed broker messages and
graceful cancellation. PostgreSQL integration tests cover atomic attempt
reservation, idempotent completion and expired-lease takeover.
