# ADR 0001: Keep NATS as a private data plane

- Status: Accepted for RelayHub v1
- Date: 2026-09-12

## Context

RelayHub needs durable push delivery, redelivery, explicit acknowledgement,
request/reply and shared consumption across replicas. Exposing NATS directly
would make every application manage subjects, credentials, reconnects, consumer
state and broker upgrades. It would also make those broker details part of the
public compatibility contract.

The Redis prototype exposes durable work through `GET /api/v1/queue`. That
requires each application to own a polling loop and duplicates functionality
provided by JetStream consumers.

## Decision

NATS and JetStream are private RelayHub infrastructure. Only the RelayHub
process can access their listeners and credentials. RelayHub derives opaque
subject tokens from authenticated application IDs; no public request or frame
accepts a NATS subject, stream, durable name or sequence.

Applications consume through the same-origin WebSocket endpoint
`/api/v1/stream` using the `relayhub.stream.v1` subprotocol. One server-owned
durable consumer named `default` belongs to each application. Replicas of that
application share delivery through the same consumer. Client frames identify
only RelayHub delivery IDs assigned to the current connection.

The prototype queue polling endpoint is removed before the v1 release. The
existing `/ws` endpoint remains a best-effort observation channel while durable
business processing uses `/api/v1/stream`.

## Consequences

- Official SDKs own reconnect, continuous consumption, bounded concurrency and
  ACK/NACK behavior.
- RelayHub can change NATS topology without changing application code.
- Delivery is at least once. Applications still make side effects idempotent by
  event ID or delivery ID.
- Broker diagnostics are exposed through RelayHub health, metrics and the
  management API rather than through public NATS access.

