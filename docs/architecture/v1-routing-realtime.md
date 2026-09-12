# RelayHub v1 Routing Relay and Realtime Channels

RelayHub v1 includes a small routing relay and a channel-based realtime layer.
The routing relay maps accepted business events to targets inside RelayHub, so a
producer can publish an event type without embedding every downstream consumer.
Realtime channels provide online fan-out for browser/mobile dashboards and live
application clients. Realtime delivery is intentionally not durable; durable
business work still uses stream deliveries, callbacks and function invocations.

## Scope

The v1 routing relay supports rules with:

- `source_app_id`: optional exact producer app.
- `event_type`: exact event type, such as `order.created`.
- `target_app_id`: required durable target app.
- `realtime_channel`: optional channel to publish an online-only notification.
- `enabled`: disabled rules are ignored.

When a producer sends `POST /api/v1/events` with `target_app_ids`, RelayHub
keeps the explicit targets and validates them as before. When `target_app_ids`
is omitted or empty, RelayHub resolves targets from enabled rules. The resolved
target list is persisted in the event envelope and idempotency replay returns the
same resolved publication.

The v1 realtime layer supports:

- WebSocket subscribe topics `events`, `jobs`, `functions` as before.
- Dynamic channel subscriptions using `channel:<name>`.
- Channel names matching lowercase letters, digits, `_`, `-`, `.` and `:`,
  with a maximum length of 96 characters.
- Online-only fan-out. Offline clients do not receive missed channel messages.

## API

Routing rules are admin-managed:

- `POST /api/v1/routing/rules`
- `GET /api/v1/routing/rules`
- `PATCH /api/v1/routing/rules/{ruleID}`
- `DELETE /api/v1/routing/rules/{ruleID}`

Realtime publish is signed by an app credential:

- `POST /api/v1/realtime/channels/{channel}/publish`

The request body is a JSON object:

```json
{"data":{"message":"hello"}}
```

RelayHub emits this server frame to subscribed sessions:

```json
{"type":"channel.message","channel":"ops.alerts","publisher_app_id":"app_sender","data":{"message":"hello"}}
```

## Implementation Notes

The routing service is independent from durable persistence. It only resolves
targets and realtime side effects before `EventService` creates the final event
and jobs. PostgreSQL owns routing rule storage in v1. Tests use an in-memory rule
store for service and HTTP behavior and PostgreSQL integration coverage for
migration and persistence.

Realtime channels reuse the existing standard WebSocket endpoint. Clients
connect with the existing `ws:connect` token and subscribe with:

```json
{"type":"subscribe","topics":["channel:ops.alerts"]}
```

NATS remains private. Realtime channel frames move through the NATS
bridge for multi-gateway fan-out; each API instance receives the channel
observation and fans out to its local WebSocket sessions. The public contract
stays WebSocket and HTTP only.

## Verification

- Service tests prove implicit routing resolves durable targets, preserves
  idempotency replay, rejects unresolved events, and emits realtime channel
  notifications only on first acceptance.
- Realtime tests prove channel validation, subscription isolation and frame
  shape.
- HTTP tests prove admin routing-rule CRUD and signed realtime publish behavior.
- PostgreSQL integration tests prove routing rules survive migration and resolve
  with source/event filters.
- NATS integration tests prove a channel publish from one gateway reaches
  subscribers connected to another gateway.
