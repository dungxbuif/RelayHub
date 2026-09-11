# RelayHub v1 durable streaming protocol

This document freezes the application-facing wire contract for durable event
consumption and online function handling. NATS and JetStream are internal; an
integration never supplies a subject, stream, durable name, sequence or app ID.

## Connect and negotiate

1. Sign `POST /api/v1/socket/token` and request the `stream:connect` scope.
2. Open `wss://relayhub.dungxbuif.com/api/v1/stream?token=<encoded-token>` with
   WebSocket subprotocol `relayhub.stream.v1`.
3. Require the server to select that exact subprotocol.
4. Read `ready` and require `protocol_version` to equal `1`.
5. Send `consumer.start` for the server-owned consumer `default`.

Tokens are short lived and bound to one application. The gateway authenticates
before upgrade. Browser Origin checks, message limits and token redaction follow
the existing WebSocket security policy. A token or URL containing it must never
be logged.

## Limits

- Text frames only, valid UTF-8, exactly one JSON object, at most 65,536 encoded
  bytes per complete message.
- Duplicate JSON object keys and unknown fields are rejected.
- `max_in_flight` is 1 through 256 and can be reduced by the server.
- `delay_ms` is 0 through 300,000. Progress can extend processing only within a
  server-owned maximum.
- IDs are opaque 1 through 128 byte UTF-8 strings. Error messages are at most
  1,024 UTF-8 bytes. Event types retain the HTTP publish contract: after
  trimming they are nonempty, with no new streaming-specific maximum.
- Event `data` and function `input` are JSON objects. A successful function
  `result` can be any valid JSON value, including `null`, a scalar or an array.
  Function names and handler error codes use the existing function-name grammar:
  `^[A-Za-z_][A-Za-z0-9_.-]{0,63}$`.

The public schemas are [client frames](../../public-docs/schemas/stream-client-frame.schema.json)
and [server frames](../../public-docs/schemas/stream-server-frame.schema.json).
The complete channel contract is [AsyncAPI](../../public-docs/asyncapi.yaml).

## Client frames

```json
{"type":"consumer.start","protocol_version":1,"consumer":"default","topics":["order.created"],"max_in_flight":16}
{"type":"delivery.ack","delivery_id":"dlv_01J..."}
{"type":"delivery.nack","delivery_id":"dlv_01J...","delay_ms":5000}
{"type":"delivery.progress","delivery_id":"dlv_01J..."}
{"type":"function.result","invocation_id":"inv_01J...","ok":true,"result":{"total":42}}
{"type":"ping"}
```

`topics` is optional. Omitting it accepts every event type. An empty list is
invalid. Topic filters compare complete event type strings; they do not become
broker subjects. `delivery.ack`, `delivery.nack` and `delivery.progress` are
valid only while that delivery is assigned to the same authenticated app and
connection. Duplicate, stale, cross-app and unassigned references are rejected.
The assignment is fenced durably in PostgreSQL before the frame reaches the SDK.
JetStream duplicate suppression is time bounded, so two physical broker messages
can exist for one delivery ID. Only one can hold an active assignment, and a
physical duplicate arriving after durable ACK is consumed without invoking the
handler. An unacknowledged assignment can be delivered again after its lease
expires, always with the same delivery ID.
Function results follow the same ownership boundary: `invocation_id` must be
assigned to the authenticated application and current connection. Missing and
cross-application IDs return `function_not_assigned`; a prior connection cannot
complete an invocation after reconnect.

The gateway commits each assignment in PostgreSQL before sending
`event.delivery`. ACK commits completion before acknowledging JetStream. NACK and
disconnect release the fenced assignment before requesting redelivery; progress
renews both the database lease and broker acknowledgement timer. This ordering
lets a repeated physical broker message be suppressed by delivery identity.

## Server frames

```json
{"type":"ready","protocol_version":1,"app_id":"app_01J...","connection_id":"conn_01J...","heartbeat_interval_ms":25000,"max_in_flight_limit":256}
{"type":"consumer.started","consumer":"default","max_in_flight":16}
{"type":"event.delivery","delivery_id":"dlv_01J...","attempt":1,"event":{"id":"evt_01J...","type":"order.created","source_app_id":"app_01J...","target_app_ids":["app_01J..."],"data":{"order_id":42},"created_at":"2026-09-12T10:00:00Z"}}
{"type":"delivery.accepted","delivery_id":"dlv_01J...","state":"acked"}
{"type":"function.invoke","invocation_id":"inv_01J...","function":"calculate","input":{"a":20,"b":22},"deadline":"2026-09-12T10:00:05Z"}
{"type":"error","code":"delivery_not_assigned","message":"Delivery is not assigned to this connection.","retryable":false}
{"type":"pong"}
```

`delivery.accepted.state` is `acked`, `retrying`, or `progress`. Receipt means
RelayHub accepted the control action; it does not make application side effects
exactly once. A connection drop leaves unacknowledged work for server-side
redelivery.

## Stable errors

| Code | Meaning | Retry |
| --- | --- | --- |
| `invalid_utf8` | Message is not valid UTF-8 | Fix frame |
| `frame_too_large` | Complete message exceeds 65,536 bytes | Fix frame |
| `invalid_json` | Message is not exactly one JSON object | Fix frame |
| `duplicate_key` | Any JSON object contains a repeated key | Fix frame |
| `invalid_frame` | Fields or values do not match the selected frame | Fix frame |
| `unknown_type` | Frame type is unknown | Upgrade or fix client |
| `unsupported_version` | Version/subprotocol is unsupported | Upgrade client |
| `delivery_not_assigned` | Delivery is absent or belongs to another app/session | Do not retry that ID |
| `stale_delivery` | Assignment is no longer current | Await redelivery |
| `consumer_already_started` | This session already started consumption | Fix client |
| `consumer_unavailable` | Durable consumer is temporarily unavailable | Reconnect with backoff |
| `backpressure` | Session exceeded a server bound | Reconnect with lower concurrency |
| `function_not_assigned` | Invocation does not belong to this session | Do not retry that ID |
| `internal_error` | Unexpected server failure | Reconnect with backoff |

Messages are diagnostic and can change. Integrations branch on `code` and the
documented `retryable` flag.

## Stable close codes

| Code | Meaning | Client action |
| ---: | --- | --- |
| 1000 | Normal drain | Reconnect only if still running |
| 1001 | Server shutdown | Reconnect with jitter |
| 1003 | Binary message unsupported | Send JSON text |
| 1009 | Message too large | Fix message size |
| 4400 | Protocol violation | Fix client frame |
| 4401 | Token missing, invalid or expired | Mint a new token |
| 4403 | Origin or scope forbidden | Fix credentials/configuration |
| 4406 | Subprotocol/version unsupported | Upgrade client |
| 4408 | Heartbeat or processing timeout | Reconnect; expect redelivery |
| 4429 | Backpressure limit exceeded | Reconnect with lower concurrency |
| 4503 | Messaging dependency unavailable | Reconnect with backoff |

## Delivery and function semantics

The SDK ACKs only after the handler completes successfully. Handler failure sends
NACK with bounded backoff. During long work it can send progress, but the server
limits total extension. Delivery is at least once, so handlers commit
idempotently using `event.id` or `delivery_id`.

Functions are online request/reply. A handler returns one `function.result`; a
disconnect or missed deadline fails the invocation rather than placing it in the
durable event stream.
