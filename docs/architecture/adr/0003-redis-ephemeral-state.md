# ADR 0003: Use Redis only for shared ephemeral state

- Status: Accepted
- Date: 2026-09-20

## Context

RelayHub must run multiple API and worker replicas without sticky sessions.
Admin sessions, cluster-wide quotas, socket ownership and live instance presence
cannot remain process-local, but moving accepted events or durable queue
settlement back into Redis would duplicate PostgreSQL and JetStream guarantees.

## Decision

PostgreSQL remains the durable system of record. NATS JetStream remains durable
work transport and wakeups. Core NATS remains ephemeral replica fan-out and
control. Redis stores shared sessions, fencing records, connection ownership,
distributed rate limits and bounded live state.

Every Redis key uses a validated prefix and explicit Cluster hash tag. TTL-bound
records expire without cleanup scans. Multi-key operations stay in one hash slot;
ownership refresh and release use atomic compare-and-delete scripts. Each process
uses a unique instance identity and random non-zero generation.

Standalone, Sentinel and Cluster deployments are supported. Credentials stay in
separate username/password settings rather than addresses. Production endpoints
use private networking, ACL credentials and TLS; Cluster uses database zero.

Redis loss fails API and worker readiness. New session, quota-sensitive and
realtime-admission operations fail closed with retryable behavior. It does not
delete or invalidate an event already committed to PostgreSQL, and durable
callback/outbox workers continue where their persisted transition does not need a
new Redis policy decision. Recovery is automatic after the same Redis endpoint
returns; application processes do not require restart.

Redis Streams are reserved for future bounded Realtime history, where trimming is
part of the product contract. They are not Queue v2's durable settlement
mechanism. Queue v2 uses PostgreSQL for authoritative state and JetStream for
durable transport, leases and acknowledgement flow.

## Consequences

- API and worker replicas require no load-balancer affinity.
- Redis is required for readiness but is excluded from durable backups in the
  local stack.
- Operators must monitor Redis latency, memory, authentication and failover while
  backing up PostgreSQL and JetStream as a consistent durable pair.
- Password rotation requires updating every replica and recreating affected
  services; addresses and logs never contain credentials.
