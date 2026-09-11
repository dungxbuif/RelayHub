# ADR 0002: Accept events with PostgreSQL and a transactional outbox

- Status: Accepted for RelayHub v1
- Date: 2026-09-12

## Context

An accepted event can create several independent stream and callback
deliveries. Writing PostgreSQL and publishing to NATS cannot be one atomic
transaction. Publishing first can expose work whose database record is absent;
committing first without recovery can strand accepted work.

## Decision

PostgreSQL is the source of truth for accepted events, idempotency reservations
and product delivery state. RelayHub writes the event, producer-scoped
idempotency result, target delivery rows and outbox rows in one transaction, and
returns `202 Accepted` only after that transaction commits.

An outbox dispatcher claims rows with `FOR UPDATE SKIP LOCKED` and publishes
them to JetStream using a deterministic `Nats-Msg-Id` derived from the outbox
identity. It records dispatch state after publish. Repeating publication after a
crash is safe within the configured JetStream duplicate window, and a
reconciler repairs ambiguous publish/update windows.

Claims use random fencing tokens and expire after a bounded interval. Broker
errors schedule a bounded exponential delay but never discard an accepted row.
Readiness reports an unhealthy dependency when pending lag exceeds the operator
limit; the API can remain live while operators restore the broker.

Stream and callback delivery state is independent. ACK/NACK changes only the
authenticated application's assigned stream delivery. Callback workers record
their own attempts and terminal state.

## Consequences

- A committed `202` is recoverable even while NATS is unavailable.
- Database migrations, outbox lag and stale claims become operational concerns.
- Deterministic message IDs and idempotent state transitions are required; the
  design does not promise exactly-once application side effects.
- Event payloads and secrets stay out of logs, metrics and audit records.
