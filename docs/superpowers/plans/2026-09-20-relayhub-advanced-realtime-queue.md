# RelayHub Advanced Realtime and Queue Plan

**Status:** Approved and scope-locked  
**Constraint:** No Python SDK/package and no reverse-proxy implementation.

**Goal:** Deliver every accepted advanced capability after the versioned
Realtime v2 and Queue v2 foundations, without weakening app isolation,
generation fencing, bounded resource use or v1 compatibility.

## Realtime A — history, rewind and namespace grants

- Store bounded, TTL Redis Streams history per app/channel; realtime history is
  reconnect continuity, never durable work delivery.
- Add `history` capability, cursor/limit bounds and rewind-on-subscribe.
- Support only issuer-granted colon-segment namespace patterns; reject global or
  ambiguous wildcards and enforce resolved channels against per-token caps.
- Add bounded batch publish with one authorization/rate decision per item and
  stable per-item outcome.

## Realtime B — encrypted channels and message extensions

- Treat end-to-end encrypted payloads as opaque ciphertext; RelayHub never owns
  room keys, indexes plaintext or claims server-side moderation of ciphertext.
- Add message actions/annotations with author/app/channel fencing, bounded action
  types and deterministic delete/tombstone behavior.
- Add file-message metadata referencing operator-configured S3-compatible object
  storage; socket frames never carry unbounded binary payloads.
- Add app-scoped push-device registration and publish controls without exposing
  provider credentials to clients.
- Emit signed, retryable channel lifecycle webhooks with safe metadata only.

## Queue A — scheduler and fairness

- Add PostgreSQL recurring schedules with IANA timezone, minimum frequency,
  deterministic next-run calculation, idempotent occurrence IDs and
  multi-replica `SKIP LOCKED` claiming.
- Preserve absolute/per-message delay already carried by event publications.
- Add starvation control to priority leasing so sustained high priority cannot
  indefinitely suppress older eligible work.
- Add subscription drain: stop new leases, allow bounded in-flight completion,
  then expose a terminal drain result without deleting retained work.

## Queue B — callbacks, audit and DLQ operations

- Add success and terminal-failure callback configuration. Payloads contain only
  app/subscription/delivery/event IDs, generation, outcome, attempt count and
  explicitly supplied safe metadata.
- Reuse SSRF policy, exact-body HMAC signing, retry classification and durable
  callback attempts; callback receipts are generation fenced.
- Audit subscription changes, pause/resume/drain, replay and permanent deletion.
- Add cursor-bounded JSON/NDJSON DLQ export. Replay/delete always requires an
  explicit selection of at most 100 IDs; no implicit “all matching” mutation.

## SDK, Admin and contract completion

- Add Go/TypeScript APIs for realtime history, wildcards, batch publish,
  encryption envelopes, actions, file metadata, push and lifecycle webhook
  verification.
- Add Go/TypeScript schedule, drain, result callback, audit and DLQ export APIs.
- Add Admin views for Queue v2 subscriptions, schedules, depth/fairness, callback
  outcomes and advanced realtime diagnostics.
- Update OpenAPI, AsyncAPI, JSON Schemas, Docusaurus, `llms` artifacts and the
  integration Skill in the same changes as each public contract.

## Verification gates

- Cross-app and cross-namespace denial tests for every realtime operation.
- History retention/cursor, encrypted payload opacity, action ownership and file
  size/metadata bounds.
- Multi-replica scheduler single-execution, clock/timezone/DST and crash reclaim.
- Priority starvation, rate/in-flight limits, callback retries, stale generation,
  audit append-only and destructive DLQ confirmation tests.
- Go race/integration, TypeScript package, Go SDK, Admin browser, Docusaurus,
  generated artifact, container and two-replica termination/recovery gates.
