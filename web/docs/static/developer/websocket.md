# Standard WebSocket delivery

RelayHub supports RFC 6455 clients: browser `WebSocket`, Node `ws`, Go Gorilla, OkHttp, and equivalent libraries. **Socket.IO is unsupported** because its framing and handshake are a different application protocol.

## Realtime v2 channels

For new room/channel integrations, request a capability token with `protocol: "realtime.v2"`, a trusted `client_id`, and exact per-channel actions:

```json
{"protocol":"realtime.v2","client_id":"user_42","channels":{"support.room_42":["subscribe","publish","presence"]},"ttl_seconds":600}
```

Connect using subprotocol `relayhub.realtime.v2`. Omitting the subprotocol preserves the legacy v1 contract documented below.

```js
const socket = new WebSocket(url, "relayhub.realtime.v2");
socket.onmessage = ({data}) => {
  const frame = JSON.parse(data);
  if (frame.type === "ready") socket.send(JSON.stringify({type:"subscribe", channels:["support.room_42"]}));
};
socket.send(JSON.stringify({type:"channel.publish", channel:"support.room_42", audience:{type:"others"}, data:{text:"hello"}}));
socket.send(JSON.stringify({type:"presence.update", channel:"support.room_42", data:{status:"online"}}));
```

V2 supports `subscribe`, `unsubscribe`, bidirectional `channel.publish`, audiences `all`, `others`, `connection`, and `client`, plus ephemeral presence/occupancy. App, publisher, message ID, and timestamp fields come only from the server. Channels and targets never cross the token's application boundary. Connection metadata and presence use Redis TTL; tokens and message payloads are not stored in the connection registry. Admins can list app-scoped connections and route disconnect commands to the owning gateway.

Schemas: [client v2](../schemas/client-frame-v2.schema.json) and [server v2](../schemas/server-frame-v2.schema.json). Realtime has no replay guarantee; use the durable stream or callbacks for reliable processing.

## Connect and subscribe

1. Register an application and securely store its credentials using the [registration flow](./registration-flow.md).
2. From your backend, sign `POST /api/v1/socket/token` with `{"scopes":["ws:connect"],"ttl_seconds":600}`. Use the returned `token` within its lifetime (maximum 900 seconds). See [signing examples](./auth.md).
3. Connect to `wss://relayhub.dungxbuif.com/ws?token=<URL-encoded-token>`.
4. Wait for `ready`, then send a `subscribe` frame for `events`, `jobs`, `functions`, `channel:<name>`, or any combination.

The verified token fixes the connection's application identity. Client `app_id` fields are rejected. `ws:connect` grants these application-scoped subscriptions; additional `ws:read` or `ws:subscribe` scopes are not required. Missing, invalid, or expired tokens fail **before upgrade** with HTTP 401 and `{"error":{"code":"unauthorized","message":"Authentication failed."}}`. A valid token missing `ws:connect` receives HTTP 403 `forbidden`. Token expiry is checked at the handshake; an established connection is not terminated when its token expires. Mint a fresh token for reconnect.

Browser `Origin` must exactly match an entry in `RELAYHUB_ALLOWED_ORIGINS`, for example `https://orders.example.com`. No wildcard is accepted. With an empty allowlist, browser origins fail with HTTP 403 `forbidden`. An absent or empty Origin is allowed for native/server clients. Origin checking supplements token authentication.

## JSON text frames

Client frames currently accepted:

```json
{"type":"subscribe","topics":["events","jobs","channel:orders.live"]}
{"type":"ping"}
```

Subscriptions add topics to the connection; repeated requests are safe, but duplicate topics within one request are invalid. Each request must contain at least one supported topic. Realtime channel topics use `channel:<name>`, where names are lowercase and may contain letters, digits, `_`, `-`, `.` and `:` up to 96 characters. There is no unsubscribe frame. Only addressed target applications receive events and job updates, and only after subscribing to the matching topic. Channel messages go to every connected session subscribed to that exact channel. Producers do not receive event notifications merely because they created the event. All subscribed sessions for the target receive the same event/job notification. Function invocations select exactly one owner session through a persisted claim. The application's `delivery_mode` does not prevent an explicitly subscribed session from observing its target notifications.

Server frames:

```json
{"type":"ready","app_id":"app_123","connection_id":"conn_123"}
{"type":"subscribed","topics":["events","jobs"]}
{"type":"event","event":{"id":"evt_123","type":"order.created","source_app_id":"app_source","target_app_ids":["app_123"],"data":{"order_id":42},"created_at":"2026-09-11T10:00:00Z"}}
{"type":"job.updated","job":{"id":"job_123","event_id":"evt_123","source_app_id":"app_source","target_app_id":"app_123","status":"pending","attempts":0,"created_at":"2026-09-11T10:00:00Z","updated_at":"2026-09-11T10:00:00Z"}}
{"type":"channel.message","channel":"orders.live","publisher_app_id":"app_source","data":{"order_id":42}}
{"type":"pong"}
{"type":"error","code":"invalid_topics","message":"Supply events, jobs, functions or channel:<name> topics without duplicates."}
```

Event and job objects use the same fields as the [HTTP API](./api-overview.md), including optional job `lease_until`. A WebSocket event is a notification, not a queue lease or acknowledgement. `channel.message` is online-only and has no replay or acknowledgement. Job notifications follow successful publish, stream delivery/ack and callback outcome operations. Retention cleanup that marks expired work terminal does not currently emit a notification. Concurrent operations can produce duplicate or out-of-order hints; use signed HTTP reads for authoritative current state.

Function handlers subscribe to `functions` and receive `rpc.invoke`:

```json
{"type":"rpc.invoke","invocation_id":"inv_123","function":"calculate","input":{},"deadline":"2026-09-11T10:00:05Z"}
{"type":"rpc.result","invocation_id":"inv_123","ok":true,"result":{"value":42}}
{"type":"rpc.result","invocation_id":"inv_123","ok":false,"error":{"code":"failed","message":"Calculation failed."}}
```

Only the selected owner connection may return a result, before the persisted deadline. Accepted results produce no acknowledgement; malformed, mismatched, unknown, expired or duplicate results receive `invalid_rpc_result`. See [remote functions](./functions.md) for registration, signed invocation, idempotency, timeout and handler examples. Each complete serialized RPC message fits the existing 64 KiB bound.

| Error code | Meaning |
| --- | --- |
| `invalid_json` | Input is malformed JSON or contains multiple values. |
| `invalid_frame` | Missing type, unknown fields (including `app_id`), invalid field types, or a binary frame. |
| `unknown_type` | Unrecognized client frame type. |
| `invalid_topics` | Empty/missing, duplicate, or unknown topics. |
| `invalid_rpc_result` | Invalid result shape, owner/connection mismatch, unknown, expired or terminal invocation. |
| `rpc_unavailable` | RPC backend is not configured (custom embedded/test routers); the standard API configures it. |
| `connection_closed` | A session is no longer registered. |
| `frame_too_large` | Parser limit exceeded; the network transport closes oversized messages with RFC 6455 code 1009 instead of sending a JSON error. |

Nonfatal errors keep the connection usable. Inbound messages are limited to **64 KiB**, including fragmented messages. Fatal read/size failures close the connection. Text messages must be valid UTF-8; invalid text closes the connection with RFC 6455 code **1007** before JSON decoding. Validation occurs after complete message reassembly, so valid multibyte characters may span fragments. Each connection has one reader, one writer, and a **64-frame** outbound application queue; an additional frame arriving at a full queue disconnects the slow client. Control responses also use a bounded queue. Close requests reserve a separate one-slot queue and take writer priority after any active write finishes, so queued Pong traffic cannot discard or starve fatal close 1007. Waiting for a close write remains bounded by the write timeout and server shutdown. The writer sends protocol Ping every 25 seconds, requires protocol Pong within 60 seconds, and limits writes to 10 seconds. Standards clients normally answer protocol Ping automatically; application `{"type":"ping"}` gets JSON `pong` and does not replace protocol Pong. Shutdown closes all sessions; reconnect after server restarts.

Outbound `event` notifications follow the accepted event size; they are **not**
capped at 64 KiB. The complete publication HTTP body is capped at 1 MiB, and the
outbound message adds the stored event envelope and WebSocket notification wrapper.
Consumers must allow that envelope overhead. Inbound messages and complete
`rpc.invoke`/`rpc.result` envelopes remain capped at 64 KiB. The 64-message outbound
queue is a count bound, not a 64 KiB aggregate memory bound.

## Browser example

Your backend should authenticate the browser user and provide only the short-lived token for the correct application. Never place the application's API key or HMAC secret in browser code.

```js
// token comes from your authenticated backend's token endpoint.
const socket = new WebSocket(
  `wss://relayhub.dungxbuif.com/ws?token=${encodeURIComponent(token)}`
);
socket.onmessage = ({ data }) => {
  const frame = JSON.parse(data);
  if (frame.type === "ready") {
    socket.send(JSON.stringify({ type: "subscribe", topics: ["events", "jobs"] }));
  }
  if (frame.type === "event") {
    // Ask your backend to recover/process the durable queue, deduplicating by frame.event.id.
  }
  if (frame.type === "error") console.error(frame.code);
};
```

## Node.js example

Install `ws` (`npm install ws`); obtain `token` with the Node signing example in [Auth & Signature](./auth.md).

```js
import WebSocket from "ws";
const socket = new WebSocket(
  `wss://relayhub.dungxbuif.com/ws?token=${encodeURIComponent(token)}`
);
socket.on("message", (data) => {
  const frame = JSON.parse(data.toString());
  if (frame.type === "ready") {
    socket.send(JSON.stringify({ type: "subscribe", topics: ["events"] }));
  }
  if (frame.type === "event") {
    // Wake your durable queue consumer; deduplicate side effects by frame.event.id.
  }
});
socket.on("error", () => console.error("WebSocket connection failed"));
```

## Go example

Install `github.com/gorilla/websocket`; pass the short-lived token from the Go signing example to this function:

```go
func listen(token string) error {
    endpoint := "wss://relayhub.dungxbuif.com/ws?token=" + url.QueryEscape(token)
    conn, _, err := websocket.DefaultDialer.Dial(endpoint, nil)
    if err != nil { return errors.New("WebSocket connection failed") }
    defer conn.Close()
    for {
        var frame struct {
            Type string `json:"type"`
            Event json.RawMessage `json:"event"`
        }
        if err := conn.ReadJSON(&frame); err != nil { return err }
        if frame.Type == "ready" {
            if err := conn.WriteJSON(map[string]any{"type":"subscribe", "topics":[]string{"events"}}); err != nil { return err }
        }
        if frame.Type == "event" {
            // Wake your durable queue consumer; deduplicate by event ID.
        }
    }
}
// Imports: encoding/json, errors, net/url, github.com/gorilla/websocket.
```

## Reconnect and recover

Reconnect with exponential backoff and jitter, mint a fresh token, wait for `ready`, and re-subscribe. For durable work, resume `/api/v1/stream` or rely on callbacks and process every event idempotently. Realtime channel messages are online-only hints. See [reliability](./reliability.md). Browser clients should keep durable processing on a trusted backend.

Realtime notifications have no replay. A missed notification, full queue, network partition, or broker reconnect does not remove durable work. Publication retries with the same idempotency key do not emit fresh notifications. Use durable stream delivery or callbacks for reliable recovery; WebSocket delivery alone is best effort. Queue consumers share app-level leases, so overlapping observers must deduplicate their own side effects. Do not log tokens, full connection URLs, HMAC signatures, secrets, or event payloads; reverse-proxy access logs should omit the `/ws` query string.

## Runtime design and namespace

Each API instance establishes a private NATS realtime bridge and fans out locally to connected WebSocket sessions. Event, job, channel and function notifications are hints; PostgreSQL remains authoritative; NATS carries cross-instance fan-out and stream delivery.

Internal channels use `<prefix>:pubsub:<hex-encoded-app-id>`. Every durable app, credential, event, job, queue, stream, acknowledgement, and idempotency key also uses this prefix. Channel names never appear in client-facing frames. Each application notification retains the original event payload but routes only to its channel's target, preventing duplicate fan-out across target channels.

A successful durable operation remains accepted when its notification fails. `relayhub_notification_failures_total` and a payload-free warning expose these failures. `relayhub_websocket_connections` shows active local sessions; `relayhub_websocket_slow_clients_total` tracks full outbound queues. NATS reconnect handles transient broker interruptions in PostgreSQL/NATS mode; missed realtime notifications remain best-effort and durable work remains recoverable through stream delivery/callback state. API startup fails if the initial realtime bridge cannot be established; shutdown closes sessions before waiting for HTTP shutdown.
