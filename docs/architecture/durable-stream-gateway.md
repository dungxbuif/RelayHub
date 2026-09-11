# Durable stream gateway implementation contract

The v1 gateway serves `GET /api/v1/stream` on the RelayHub origin. It verifies a
short-lived `stream:connect` token and allowed browser Origin before RFC 6455
upgrade, then requires WebSocket subprotocol `relayhub.stream.v1`. The first
server frame is `ready`; a client starts the server-owned consumer `default`
exactly once.

The HTTP route remains registered whenever token issuance is available. During
staged rollout, a correctly authenticated and negotiated request returns `503
stream_unavailable` until PostgreSQL and the gateway are configured. This keeps
the public route and its pre-upgrade authentication behavior stable.

Each application maps to one deterministic private JetStream filter and durable
consumer. Several RelayHub processes or application replicas bind to the same
durable and share work. Public frames never contain subjects, stream names,
consumer names other than `default`, or broker sequence values.

## Assignment fence

For every physical JetStream message, the gateway decodes the stored outbox
envelope and reserves a PostgreSQL delivery assignment containing application,
connection, random assignment token and expiry. A message is sent to the client
only after this transaction commits.

- `delivery.ack` atomically marks the matching PostgreSQL delivery complete,
  then double-ACKs JetStream.
- `delivery.nack` releases the matching database assignment, then sends a
  bounded delayed NACK.
- `delivery.progress` renews the matching database assignment, then sends a
  JetStream progress acknowledgement.
- Disconnect and shutdown release live assignments and NACK their messages.

Every mutation includes delivery ID, authenticated app ID, current connection ID
and assignment token. Missing, expired, cross-app, stale-session and replaced
assignments fail without revealing which part mismatched. A second physical
message for a completed delivery is ACKed without delivery; an already assigned
duplicate is NACKed for later inspection.

## Bounds and shutdown

The server caps negotiated inflight count at 256, total inflight encoded bytes,
complete frame bytes and the outbound queue. It NACKs before assignment when a
session has no capacity. Malformed broker payloads are never sent to clients.
Malformed client frames receive the stable protocol error; binary and oversized
messages close with the documented codes.

Shutdown stops new assignments, drains the private NATS subscription within the
configured deadline, releases current assignments, NACKs unacknowledged
messages, sends a normal restart close where possible and waits for session
goroutines. This preserves at-least-once redelivery without trusting client
offsets.

## Verification

Focused unit tests cover token-before-upgrade, version negotiation, deterministic
consumer binding, count/byte backpressure, malformed and Unicode frames, all
assignment fences, duplicate physical messages and cleanup. Real RFC 6455 tests
exercise `/api/v1/stream`; NATS/PostgreSQL integration tests exercise shared
durables and redelivery.
