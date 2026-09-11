# Remote functions over WebSocket

RelayHub routes a request to a handler running in your application and returns the handler's response. It never executes uploaded source code, installs packages, or creates a sandbox. Use functions for short calls to an online application; use [events and the durable queue](./reliability.md) for work that must survive an offline consumer.

## Register and discover a function

Sign every HTTP request using the [application HMAC rules](./auth.md). The owner signs:

```http
POST /api/v1/functions
Content-Type: application/json

{"name":"calculate","timeout_seconds":5}
```

The response is `201 Created`:

```json
{"id":"fn_...","app_id":"app_owner","name":"calculate","timeout_seconds":5,"enabled":true,"created_at":"2026-09-11T10:00:00Z","updated_at":"2026-09-11T10:00:00Z"}
```

Names match `[A-Za-z_][A-Za-z0-9_.-]{0,63}` and are unique within one application. Names are case-sensitive, stable labels for handler selection. Timeout must be an integer from 1 to 30 seconds; it is required. `enabled` is optional and defaults to `true`. An explicitly disabled registration cannot be invoked. Unknown fields, including a client-supplied `app_id`, are rejected.

`GET /api/v1/functions` returns only the signed owner's registrations, sorted by name; an empty list is `[]`. `DELETE /api/v1/functions/{functionID}` returns `204` for the owner and removes both registration and name reservation. Deleting an absent or another app's function returns the same `404 not_found`. A duplicate name returns `409 conflict`. To change registration settings, delete and register again; the new registration gets a new opaque `fn_...` ID. Share the function ID with callers through your own application configuration. There is no global function directory or update route.

Registration persists until owner deletion. Deletion prevents new calls but leaves accepted invocations and their idempotent replay results intact. Disabled owner applications cannot receive new invocation claims.

## Connect the handler

Obtain an app-scoped short-lived token with signed `POST /api/v1/socket/token`, requesting `{"scopes":["ws:connect"],"ttl_seconds":600}`. Connect to `/ws?token=<encoded-token>`, wait for `ready`, and send:

```json
{"type":"subscribe","topics":["functions"]}
```

The server acknowledges `{"type":"subscribed","topics":["functions"]}`. You may also subscribe to `events` and `jobs` on the same connection. Only one eligible owner connection receives each invocation, even when the owner has multiple connections on different API instances:

```json
{"type":"rpc.invoke","invocation_id":"inv_...","function":"calculate","input":{"a":20,"b":22},"deadline":"2026-09-11T10:00:05Z"}
```

Reply on that same connection with one of:

```json
{"type":"rpc.result","invocation_id":"inv_...","ok":true,"result":{"value":42}}
{"type":"rpc.result","invocation_id":"inv_...","ok":false,"error":{"code":"invalid_numbers","message":"Both inputs must be finite numbers."}}
```

`result` may contain any valid JSON value, including `null`. Input, result and error bytes must be valid UTF-8; malformed bytes are never forwarded as WebSocket text. Failure `error` must be an object containing only `code` and `message`; code uses the function-name grammar and message must be nonempty, at most 1024 UTF-8 bytes. Both fields are caller-visible: return a deliberate safe error, not an exception object, stack trace, access token, or secret. Do not include `result` on failure or `error` on success. Accepted results produce no WebSocket acknowledgement.

The verified token supplies the owner identity, and the server assigns the connection ID. A client cannot select either identity in a result frame. Missing, mismatched, unknown, expired, duplicate, or already-terminal replies receive a stable `error` frame with `code: "invalid_rpc_result"`. They cannot complete another call. A successful completion stays successful when a later duplicate arrives.

## Invoke and retry

Another registered app signs:

```http
POST /api/v1/functions/fn_.../invoke
Idempotency-Key: calculate-order-123
Content-Type: application/json

{"input":{"a":20,"b":22}}
```

`input` is required and must be a JSON object encoded as valid UTF-8; `{}` is valid. Malformed UTF-8 is rejected with `400 invalid_request` before storing an invocation, reserving its idempotency key, or sending a handler frame. The server uses the function's registered timeout. Supplying your own timeout or target app field is invalid. IDs are opaque, and timestamps use RFC3339 UTC with optional fractional seconds.

Success returns HTTP `200`:

```json
{"invocation_id":"inv_...","ok":true,"result":{"value":42}}
```

A handler failure is also a completed invocation and returns HTTP `200`:

```json
{"invocation_id":"inv_...","ok":false,"error":{"code":"invalid_numbers","message":"Both inputs must be finite numbers."}}
```

Check `ok` even after an HTTP success. RelayHub does not interpret or execute handler output.

An idempotency key is required, must be nonblank, and may contain at most 256 bytes. The namespace is **caller app plus key**, independent of function ID. Use a new key for a new logical call. Concurrent calls with the same key converge on one invocation and terminal response. Within 24 hours of initial acceptance, retrying the same key returns the original HTTP status and body with `Idempotent-Replayed: true`, including handler errors, unavailable results, and timeouts. Changed function IDs or object input do not change that original call. A replay continues to work after the registration is deleted. After expiry, the same key may create a new invocation.

Cancellation or a dropped caller connection stops that HTTP waiter without revoking already claimed handler work. Retry with the same key to inspect its eventual response. A handler may have committed a side effect before RelayHub timed out or lost its result: use the invocation ID for application-level deduplication, and do not assume a timeout means no work occurred.

## Limits and failures

| HTTP status / code | Meaning |
| --- | --- |
| `400 invalid_request` | Invalid JSON/fields, missing idempotency key, invalid name/timeout/input, or oversized outbound invocation frame. |
| `401 unauthorized` | Missing, disabled, or invalid signing credentials. |
| `404 not_found` | Missing/disabled function, disabled owner, or owner-scoped deletion of an unrelated function. |
| `409 conflict` | Name is already registered for that owner. |
| `413 request_too_large` | Complete HTTP request body exceeds 1 MiB. |
| `503 function_unavailable` | No eligible owner connection claimed within the 250 ms acceptance window. |
| `504 function_timeout` | A claimed handler did not complete before the persisted function deadline. |
| `500 internal_error` | Unexpected storage or service failure; retry the same key if acceptance is uncertain. |

Errors use the standard `{"error":{"code":"...","message":"..."}}` envelope. The unavailable and timeout responses are persisted terminal outcomes; retries do not redispatch them. To make a deliberate new attempt after a handler returns online, use a new key.

Handler `rpc.result` errors require exactly lowercase, non-null string fields
`code` and `message`, each appearing once. Duplicate, unknown and wrong-case keys
are rejected with `invalid_rpc_result`. RelayHub reconstructs canonical JSON from
the validated strings before storing and forwarding the error.

The complete HTTP body limit remains **1 MiB**. The existing complete WebSocket message limit remains **64 KiB (65,536 bytes)**, including fragmented messages. Each serialized `rpc.invoke` frame, including its ID, function name, input and deadline, must fit 64 KiB; validation includes JSON escaping and rejects excess before dispatch. An input below 1 MiB can therefore still be too large for RPC. The complete serialized `rpc.result` frame, including its result/error, must also fit 64 KiB. Transport oversize closes with code 1009; malformed result envelopes within the bound receive `invalid_rpc_result`. No payload appears in logs or metric labels.

The registered timeout starts at invocation creation and includes routing time. Keep API and Redis clocks synchronized. Only calls acknowledged by an eligible connection can time out with 504; calls that fail to claim return 503. RelayHub cannot stop user code when a deadline expires, but expired results cannot overwrite terminal state.

## Browser handler

Provide `token` from your authenticated backend. Never expose the application API key or HMAC secret in browser JavaScript. Browser Origin must be in the RelayHub allowlist.

```js
const socket = new WebSocket(`${wsBase}/ws?token=${encodeURIComponent(token)}`);
socket.onmessage = ({ data }) => {
  const frame = JSON.parse(data);
  if (frame.type === "ready") {
    socket.send(JSON.stringify({ type: "subscribe", topics: ["functions"] }));
    return;
  }
  if (frame.type !== "rpc.invoke") return;
  if (Date.now() >= Date.parse(frame.deadline)) return;
  const { a, b } = frame.input;
  const valid = frame.function === "calculate" &&
    Number.isFinite(a) && Number.isFinite(b) && Number.isFinite(a + b);
  socket.send(JSON.stringify(valid
    ? { type: "rpc.result", invocation_id: frame.invocation_id, ok: true,
        result: { value: a + b } }
    : { type: "rpc.result", invocation_id: frame.invocation_id, ok: false,
        error: { code: "invalid_numbers", message: "Both inputs must be finite numbers." } }));
};
```

## Node handler and signed caller

Install `ws` with `npm install ws`. This backend helper signs the exact serialized body and request target:

```js
import { createHash, createHmac } from "node:crypto";
import WebSocket from "ws";

async function signed(base, credentials, method, path, value, key) {
  const body = value === undefined ? "" : JSON.stringify(value);
  const timestamp = String(Math.floor(Date.now() / 1000));
  const canonical = [timestamp, method, path,
    createHash("sha256").update(body).digest("hex")].join("\n");
  const response = await fetch(base + path, {
    method, body: method === "GET" ? undefined : body,
    headers: {
      "Content-Type": "application/json",
      "X-RelayHub-Api-Key": credentials.api_key,
      "X-RelayHub-Timestamp": timestamp,
      "X-RelayHub-Signature": createHmac("sha256", credentials.hmac_secret)
        .update(canonical).digest("hex"),
      ...(key ? { "Idempotency-Key": key } : {})
    }
  });
  const data = response.status === 204 ? null : await response.json();
  if (!response.ok) throw new Error(data.error.code);
  return data;
}

// Supply base, ownerCredentials and callerCredentials from backend configuration.
const fn = await signed(base, ownerCredentials, "POST", "/api/v1/functions",
  { name: "calculate", timeout_seconds: 5 });
const { token } = await signed(base, ownerCredentials, "POST", "/api/v1/socket/token",
  { scopes: ["ws:connect"], ttl_seconds: 600 });
const socket = new WebSocket(`${base.replace(/^http/, "ws")}/ws?token=${encodeURIComponent(token)}`);
const subscribed = new Promise((resolve, reject) => {
  socket.once("error", () => reject(new Error("WebSocket connection failed")));
  socket.on("message", raw => {
    const frame = JSON.parse(raw.toString());
    if (frame.type === "ready") socket.send(JSON.stringify({ type: "subscribe", topics: ["functions"] }));
    if (frame.type === "subscribed") resolve();
    if (frame.type !== "rpc.invoke" || Date.now() >= Date.parse(frame.deadline)) return;
    const { a, b } = frame.input;
    const valid = frame.function === "calculate" && Number.isFinite(a) &&
      Number.isFinite(b) && Number.isFinite(a + b);
    socket.send(JSON.stringify(valid
      ? { type: "rpc.result", invocation_id: frame.invocation_id, ok: true, result: { value: a + b } }
      : { type: "rpc.result", invocation_id: frame.invocation_id, ok: false,
          error: { code: "invalid_numbers", message: "Both inputs must be finite numbers." } }));
  });
});
await subscribed;
const response = await signed(base, callerCredentials, "POST",
  `/api/v1/functions/${fn.id}/invoke`, { input: { a: 20, b: 22 } }, "calculation-123");
// Inspect response.ok and response.result or response.error in your application.
socket.close();
```

Register once during deployment; on restart, list existing registrations and reuse the stable name's ID rather than creating duplicates. For production reconnects, mint a fresh token, reconnect with exponential backoff and jitter, wait for `ready`, then re-subscribe. The example intentionally completes one call and closes. Standard Node `ws` answers protocol pings automatically.

## Go handler

Use Gorilla WebSocket and the token from your backend's signing code. This loop keeps socket writes on one goroutine. Extend it with a bounded worker pool for concurrent slow handlers and a single writer; retain each invocation deadline.

```go
// Imports: encoding/json, errors, math, net/url, time,
// github.com/gorilla/websocket.
func serveFunctions(wsBase, token string) error {
    conn, _, err := websocket.DefaultDialer.Dial(wsBase+"/ws?token="+url.QueryEscape(token), nil)
    if err != nil { return errors.New("WebSocket connection failed") }
    defer conn.Close()
    for {
        var frame struct {
            Type string `json:"type"`
            InvocationID string `json:"invocation_id"`
            Function string `json:"function"`
            Input json.RawMessage `json:"input"`
            Deadline time.Time `json:"deadline"`
        }
        if err := conn.ReadJSON(&frame); err != nil { return err }
        if frame.Type == "ready" {
            if err := conn.WriteJSON(map[string]any{"type":"subscribe", "topics":[]string{"functions"}}); err != nil { return err }
        }
        if frame.Type != "rpc.invoke" || !time.Now().Before(frame.Deadline) { continue }
        var input struct { A *float64 `json:"a"`; B *float64 `json:"b"` }
        reply := map[string]any{"type":"rpc.result", "invocation_id":frame.InvocationID, "ok":false,
            "error":map[string]string{"code":"invalid_numbers", "message":"Both inputs must be finite numbers."}}
        if json.Unmarshal(frame.Input, &input) == nil && frame.Function == "calculate" && input.A != nil && input.B != nil {
            sum := *input.A + *input.B
            if !math.IsInf(sum, 0) && !math.IsNaN(sum) {
                reply["ok"] = true
                delete(reply, "error")
                reply["result"] = map[string]float64{"value":sum}
            }
        }
        if err := conn.WriteJSON(reply); err != nil { return err }
    }
}
```

## Scaling, retention and operations

All API instances must use the same Redis URL and `RELAYHUB_REDIS_KEY_PREFIX` for one deployment. The prefix applies to function registrations/name indexes, invocation hashes, hashed caller/key indexes, claims, replies and Pub/Sub channels. Registrations persist until deletion. Invocation, claim and reply metadata share a single expiring hash, and the idempotency pointer expires at the same 24-hour deadline. There are no persistent presence entries.

Redis atomically reserves one connection and acknowledges its dispatch before the frame enters its bounded local outbound queue. Failed enqueue or a connection that closes during reservation releases the claim and notifies other instances; once delivered, an invocation is not redispatched. A dropped connection after dispatch can therefore produce 504. RPC has no durable offline queue and cannot be recovered by event queue polling.

Each caller subscribes to its invocation's Redis wakeup channel before initial publication, then reads persisted state after notifications and every 25 ms as a fallback. Fast results remain readable even when the notification arrives before the HTTP waiter resumes. Expiry is evaluated atomically against the stored deadline on read/claim/result transitions; no detached timer or unbounded background presence cleanup is required. Interrupted publishers become unavailable at the claim deadline when next inspected. Redis operations and claim windows are bounded to 250 ms. Subscription setup applies that startup budget to the complete synchronous connection initialization/write and subscription acknowledgement; an earlier caller deadline also bounds setup. State and payload retention still depend on your Redis persistence policy; changing the key prefix does not migrate records.

`relayhub_function_outcomes_total{outcome="registered|invoked|success|handler_error|unavailable|timeout"}` uses fixed labels. `relayhub_function_duration_seconds` measures the initial caller's terminal latency. Replays are excluded. A cancelled initial caller may leave no observed terminal latency/outcome even if its handler later completes; stored invocation state remains authoritative. Logs contain outcomes and latency, never input, result, tokens, or secrets. Existing WebSocket connection/slow-client metrics remain available.

**Socket.IO does not work with `/ws`.** Use an RFC 6455 client. Avoid logging token-bearing socket URLs, and keep application credentials in backend secret storage.
