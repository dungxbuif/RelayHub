# RelayHub Realtime v2 Plan

**Goal:** Add a version-negotiated, app-isolated realtime protocol with channel ACLs, bidirectional publish, targeting, connection ownership and presence while preserving v1 clients.

**Architecture:** `relayhub.realtime.v2` is negotiated through the WebSocket subprotocol. Signed short-lived claims carry trusted app/client identity and channel actions. The local Hub owns sockets; Redis stores TTL connection/presence metadata and Core NATS routes commands/fan-out across replicas. V1 remains unchanged when no subprotocol is negotiated.

## Task 1: Claims and wire contracts
- Extend token claims with optional client ID and bounded exact-channel capabilities.
- Add v2 client/server frame schemas, ready capabilities, unsubscribe, publish and presence frames.
- Reject identity fields and unauthorized actions; keep v1 fixtures green.

## Task 2: Local v2 session behavior
- Register protocol/client/capabilities on sessions.
- Implement app-isolated subscribe/unsubscribe and publish audiences: all, others, connection and client.
- Generate message ID/time and trusted publisher identity server-side.
- Add strict size, count and rate boundaries.

## Task 3: Multi-replica ownership and commands
- Store TTL connection metadata and channel membership in Redis without token/payload data.
- Route targeted publish/disconnect commands to the owner instance over Core NATS.
- Add bounded Admin connection list/disconnect endpoints.

## Task 4: Presence and occupancy
- Store bounded ephemeral presence with TTL and app/channel isolation.
- Emit join/update/leave/timeout over Core NATS and expose occupancy without member identity.
- Reconcile disconnect loss through expiry; presence never becomes business state.

## Task 5: Studio, docs and verification
- Upgrade Realtime Studio to v2 negotiation, unsubscribe, publish, targeting and presence.
- Update AsyncAPI/JSON schemas/OpenAPI, Go/TS SDK contracts and Skill/docs.
- Run v1 compatibility, ACL, cross-app isolation, multi-replica, browser and release gates.

Advanced history/rewind, wildcards, batch publish, encryption, files/actions and push integrations remain the immediately following Realtime v2 advanced plan.
