# Standard WebSocket delivery

RelayHub supports RFC 6455 clients: browser `WebSocket`, Node `ws`, Go Gorilla, OkHttp, and equivalent libraries. **Socket.IO is unsupported** because its framing and handshake are a different application protocol.

## Connect and subscribe

1. Register an application and securely store its credentials using the [registration flow](./registration-flow.md).
2. From your backend, sign `POST /api/v1/socket/token` with `{"scopes":["ws:connect"],"ttl_seconds":600}`. Use the returned `token` within its lifetime (maximum 900 seconds). See [signing examples](./auth.md).
3. Connect to `wss://relayhub.dungxbuif.com/ws?token=<URL-encoded-token>`.
4. Wait for `ready`, then send a `subscribe` frame for `events`, `jobs`, or both.

The verified token fixes the connection's application identity. Client `app_id` fields are rejected. `ws:connect` currently grants both supported subscriptions; additional `ws:read` or `ws:subscribe` scopes are not required. Missing, invalid, or expired tokens fail **before upgrade** with HTTP 401 and `{"error":{"code":"unauthorized","message":"Authentication failed."}}`. A valid token missing `ws:connect` receives HTTP 403 `forbidden`. Token expiry is checked at the handshake; an established connection is not terminated when its token expires. Mint a fresh token for reconnect.

Browser `Origin` must exactly match an entry in `RELAYHUB_ALLOWED_ORIGINS`, for example `https://orders.example.com`. No wildcard is accepted. With an empty allowlist, browser origins fail with HTTP 403 `forbidden`. An absent or empty Origin is allowed for native/server clients. Origin checking supplements token authentication.

## JSON text frames

Client frames currently accepted:

```json
{"type":"subscribe","topics":["events","jobs"]}
{"type":"ping"}
```

Subscriptions add topics to the connection; repeated requests are safe, but duplicate topics within one request are invalid. Each request must contain at least one supported topic. There is no unsubscribe frame. Only addressed target applications receive events and job updates, and only after subscribing to the matching topic. Producers do not receive notifications merely because they created the event. All subscribed sessions for the target receive the same notification. The application's `delivery_mode` does not prevent an explicitly subscribed session from observing its target notifications.

Server frames:

```json
{"type":"ready","app_id":"app_123","connection_id":"conn_123"}
{"type":"subscribed","topics":["events","jobs"]}
{"type":"event","event":{"id":"evt_123","type":"order.created","source_app_id":"app_source","target_app_ids":["app_123"],"data":{"order_id":42},"created_at":"2026-09-11T10:00:00Z"}}
{"type":"job.updated","job":{"id":"job_123","event_id":"evt_123","source_app_id":"app_source","target_app_id":"app_123","status":"pending","attempts":0,"created_at":"2026-09-11T10:00:00Z","updated_at":"2026-09-11T10:00:00Z"}}
{"type":"pong"}
{"type":"error","code":"invalid_topics","message":"Supply events or jobs topics without duplicates."}
```

Event and job objects use the same fields as the [HTTP API](./api-overview.md), including optional job `lease_until`. A WebSocket event is a notification, not a queue lease or acknowledgement. Job notifications follow successful publish, queue lease, ack, admin requeue, and admin dead-letter operations. Queue housekeeping that marks expired-event jobs dead-letter does not currently emit a notification. Concurrent operations can produce duplicate or out-of-order hints; use signed HTTP reads for authoritative current state.

Reserved RPC syntax is parsed but cannot invoke or complete functions in this release:

```json
{"type":"rpc.result","invocation_id":"inv_123","ok":true,"result":{"value":42}}
{"type":"rpc.result","invocation_id":"inv_123","ok":false,"error":"calculation failed"}
```

A result requires an invocation ID, a boolean `ok`, and either `result` for success or a nonempty `error` string for failure. Valid reserved results receive `rpc_unavailable`. A `functions` subscription receives `unauthorized_topic`. No `rpc.invoke` frame is sent.

| Error code | Meaning |
| --- | --- |
| `invalid_json` | Input is malformed JSON or contains multiple values. |
| `invalid_frame` | Missing type, unknown fields (including `app_id`), invalid field types, or a binary frame. |
| `unknown_type` | Unrecognized client frame type. |
| `invalid_topics` | Empty/missing, duplicate, or unknown topics. |
| `unauthorized_topic` | Reserved `functions` topic is disabled. |
| `invalid_rpc_result` | Incomplete or inconsistent reserved result. |
| `rpc_unavailable` | RPC result routing is disabled. |
| `connection_closed` | A session is no longer registered. |
| `frame_too_large` | Parser limit exceeded; the network transport closes oversized messages with RFC 6455 code 1009 instead of sending a JSON error. |

Nonfatal errors keep the connection usable. Inbound messages are limited to **64 KiB**, including fragmented messages. Fatal read/size failures close the connection. Text messages must be valid UTF-8; invalid text closes the connection with RFC 6455 code **1007** before JSON decoding. Validation occurs after complete message reassembly, so valid multibyte characters may span fragments. Each connection has one reader, one writer, and a **64-frame** outbound application queue; an additional frame arriving at a full queue disconnects the slow client. Control responses also use a bounded queue. The writer sends protocol Ping every 25 seconds, requires protocol Pong within 60 seconds, and limits writes to 10 seconds. Standards clients normally answer protocol Ping automatically; application `{"type":"ping"}` gets JSON `pong` and does not replace protocol Pong. Shutdown closes all sessions; reconnect after server restarts.

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

Reconnect with exponential backoff and jitter, mint a fresh token, wait for `ready`, and re-subscribe. Drain `GET /api/v1/queue` after subscribing and keep polling as a fallback. Process leased work idempotently and acknowledge it only after your side effects commit. See [queue reliability](./reliability.md). Browser clients should ask their backend to perform signed queue and acknowledgement calls.

Redis Pub/Sub has no replay. A missed notification, full queue, network partition, or subscription reconnect does not remove durable work. Publication retries with the same idempotency key do not emit fresh notifications. Polling the durable queue is required for reliable recovery; WebSocket delivery alone is best effort. Queue consumers share app-level leases, so overlapping observers must deduplicate their own side effects. Do not log tokens, full connection URLs, HMAC signatures, secrets, or event payloads; reverse-proxy access logs should omit the `/ws` query string.

## Runtime design and namespace

Each API instance establishes one Redis pattern subscription at startup and fans out locally. All instances sharing one application/event namespace must use the same `RELAYHUB_REDIS_KEY_PREFIX`. Its default is exactly `relayhub`; explicit values must be 1–64 ASCII letters, digits, underscores, or hyphens. Empty values and Redis glob characters are rejected. The default preserves all existing `relayhub:*` data. Changing the prefix selects a separate, initially empty namespace; it does not migrate stored records. Use the same prefix for every process serving the same deployment.

Internal channels use `<prefix>:pubsub:<hex-encoded-app-id>`. Every durable app, credential, event, job, queue, stream, acknowledgement, and idempotency key also uses this prefix. Channel names never appear in client-facing frames. Each application notification retains the original event payload but routes only to its channel's target, preventing duplicate fan-out across target channels.

A successful durable operation remains accepted when its notification fails. `relayhub_notification_failures_total` and a payload-free warning expose these failures. `relayhub_websocket_connections` shows active local sessions; `relayhub_websocket_slow_clients_total` tracks full outbound queues. Redis subscriptions reconnect through go-redis; missed notifications remain recoverable by queue polling. API startup fails if the initial subscription cannot be established; shutdown cancels the subscription and closes sessions before waiting for HTTP shutdown.

Implementation record: protocol and hub tests were written and run RED before implementation, followed by HTTP/client, notifier ordering, prefix validation, real Redis 7 cross-instance and namespace tests. The typed `InvokeFunction`/result hooks are reserved for the later remote-function task.
