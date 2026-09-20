# Routing relay and realtime channels

Routing relay lets a producer publish an event type and let RelayHub choose the
targets. Realtime channels let online WebSocket clients subscribe to live,
non-durable messages such as dashboards, notifications and activity feeds.

## Routing rules

An administrator creates routing rules:

```http
POST /api/v1/routing/rules
Cookie: __Host-relayhub_admin=<session-cookie>
X-RelayHub-CSRF: <session-csrf-token>
Content-Type: application/json

{
  "source_app_id": "app_checkout",
  "event_type": "order.created",
  "target_app_id": "app_billing",
  "realtime_channel": "orders.live"
}
```

`source_app_id` is optional. If it is omitted, the rule can match any producer
for the same event type. `target_app_id` must be an enabled app. Disabled rules
are ignored. `realtime_channel` is optional and publishes an online-only channel
message when a new event is accepted.

When `target_app_ids` is present in `POST /api/v1/events`, RelayHub uses that
explicit target list. When it is omitted or empty, RelayHub resolves enabled
routing rules matching the source app and event type. The resolved targets are
stored in the event envelope and idempotency replay returns the original routed
publication without emitting a second realtime message.

```http
POST /api/v1/events
Idempotency-Key: order-123-created
X-RelayHub-Api-Key: <api-key>
X-RelayHub-Timestamp: <unix-seconds>
X-RelayHub-Signature: <signature>
Content-Type: application/json

{"type":"order.created","data":{"order_id":"ord_123"}}
```

Rule management endpoints:

| Endpoint | Auth | Purpose |
| --- | --- | --- |
| `POST /api/v1/routing/rules` | Admin bearer | Create a rule. |
| `GET /api/v1/routing/rules` | Admin bearer | List active rules. |
| `PATCH /api/v1/routing/rules/{ruleID}` | Admin bearer | Change source, event type, target, channel or enabled state. |
| `DELETE /api/v1/routing/rules/{ruleID}` | Admin bearer | Soft-delete a rule. |

## Realtime channels

Realtime channels are standard WebSocket topics named `channel:<name>`. Channel
names are lowercase and may contain letters, digits, `_`, `-`, `.` and `:`,
up to 96 characters.

Browser or app clients connect to `/ws` with a `ws:connect` token, wait for
`ready`, then subscribe:

```json
{"type":"subscribe","topics":["channel:orders.live"]}
```

A trusted backend can publish to the channel with a signed app request:

```http
POST /api/v1/realtime/channels/orders.live/publish
X-RelayHub-Api-Key: <api-key>
X-RelayHub-Timestamp: <unix-seconds>
X-RelayHub-Signature: <signature>
Content-Type: application/json

{"data":{"order_id":"ord_123","status":"paid"}}
```

Subscribed clients receive:

```json
{"type":"channel.message","channel":"orders.live","publisher_app_id":"app_checkout","data":{"order_id":"ord_123","status":"paid"}}
```

Realtime channel messages are online-only. In PostgreSQL/NATS mode RelayHub
forwards channel messages through the private NATS bridge so every API instance
can fan out to its own connected WebSocket sessions. Use durable event
streaming, callbacks, or functions for work that must survive disconnects and
restarts.

## Verification

The integration smoke test `TestFullStackRoutedEventReachesRealtimeAndDurableStream` exercises the management-console use case against real PostgreSQL, real NATS, the HTTP router, standard WebSocket realtime channels, outbox dispatch and `/api/v1/stream` acknowledgement.
