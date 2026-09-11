# Transactional event outbox

RelayHub v1 accepts an event only after PostgreSQL commits the canonical event,
producer-scoped idempotency result, per-sink delivery rows and matching outbox
rows in one transaction. The transaction locks every target application before
checking that it is enabled, so a concurrent disable cannot produce partially
accepted work.

Each target creates a stream delivery when its policy is `queue`, `websocket` or
`all`. A configured callback target creates a separate callback delivery when
its policy is `callback` or `all`. The HTTP publication response retains the
original per-target job snapshots for v1 compatibility; sink delivery identity
and state remain internal until exposed through the streaming protocol.

Event JSON is stored as PostgreSQL `json`, rather than `jsonb`, to preserve the
accepted UTF-8 object bytes and large JSON numbers. The idempotency row stores a
serialized copy of the original publication result. A retry with the same source
application and key returns that stored result even if the input or target state
has since changed.

## Dispatcher recovery

The dispatcher claims bounded batches with `FOR UPDATE SKIP LOCKED`. A random
claim token fences completion, and a claim becomes eligible again after its
lease expires.

- A crash before publish leaves a stale claim that another dispatcher reclaims.
- A crash after publish but before PostgreSQL completion republishes with the
  same deterministic message ID. JetStream suppresses it inside its duplicate
  window. Outside that window, the durable PostgreSQL delivery-assignment fence
  prevents two physical messages from becoming concurrent or post-ACK SDK
  assignments.
- A crash after PostgreSQL completion leaves no eligible row.

Claiming does not consume an attempt. A publish success or failure increments
exactly that row after the network call returns, so unvisited rows in a claimed
batch retain their attempt count. Publish failures clear the claim and schedule
exponential retry capped at `MaxRetry`. `MaxAttempts` bounds completed publish
calls. Exhaustion stores a terminal
`failed_at` marker, moves the delivery to dead-letter state and fails readiness;
the event, delivery and outbox payload remain available for inspection and
operator requeue. Logs and metrics use only bounded outcomes and counts; they
exclude event data, application IDs, delivery IDs and subjects.

The gateway assigns a stream delivery under a PostgreSQL row lock. The assignment
stores target app, connection, random fence token and expiry. An active duplicate
returns `already_assigned`; an acknowledged delivery returns `already_complete`.
An expired unacknowledged assignment may be delivered again with the same ID,
which is the documented at-least-once behavior.

Readiness fails when the oldest pending row exceeds the configured maximum age.
Metrics expose pending count, oldest age, dispatch outcomes and claim recovery.

## Verification

PostgreSQL integration tests cover atomic rollback, concurrent idempotency,
target-disable serialization, raw-number preservation and `SKIP LOCKED` claims.
Dispatcher tests inject failures at each publish/update boundary and assert that
the same message ID is reused without losing the accepted delivery.
