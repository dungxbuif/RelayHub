# Realtime v2

Realtime v2 is RelayHub's app-isolated, bidirectional RFC 6455 protocol for rooms/channels, targeted publish, bounded reconnect history and ephemeral presence. It is not a work queue; use durable stream, Queue v2 or callbacks for work that must survive disconnects.

## Token and connection

From a trusted backend, sign `POST /api/v1/socket/token`:

```json
{
  "protocol": "realtime.v2",
  "client_id": "user_42",
  "channels": {
    "project:42:*": ["subscribe", "publish", "presence", "history"]
  },
  "ttl_seconds": 600
}
```

Channel permissions are exact or use one issuer-granted terminal colon segment such as `project:42:*`. Global `*`, middle wildcards and multi-segment expansion are rejected. A grant matches one resolved segment only. Connect to `/ws?token=...` with WebSocket subprotocol `relayhub.realtime.v2`. A connection without that subprotocol uses the legacy v1 contract.

```ts
const socket = new WebSocket(url, "relayhub.realtime.v2");
socket.addEventListener("message", ({data}) => {
  const frame = JSON.parse(String(data));
  if (frame.type === "ready") {
    socket.send(JSON.stringify({type: "subscribe", channels: ["support.room_42"]}));
  }
});
```

The `ready` frame contains trusted `app_id`, `client_id`, `connection_id`, protocol and supported capabilities.

## Publish, targeting and presence

```json
{"type":"channel.publish","channel":"support.room_42","audience":{"type":"others"},"data":{"text":"hello"}}
{"type":"channel.publish","channel":"support.room_42","audience":{"type":"client","client_id":"user_99"},"data":{"text":"private hint"}}
{"type":"presence.update","channel":"support.room_42","data":{"status":"online"}}
{"type":"unsubscribe","channels":["support.room_42"]}
```

Audience types are `all` (default), `others`, `connection`, and `client`. RelayHub ignores client-supplied identity: outbound messages receive server-generated `message_id`, `published_at`, `publisher_client_id`, and `publisher_connection_id`. Every route is scoped to the authenticated app.

Presence is ephemeral coordination state, not business state. Redis TTL removes stale members; graceful disconnect emits `presence.leave`, while reconciliation emits `presence.timeout` after ownership disappears. Occupancy is a count and does not enumerate other member identities.

Inbound frames are limited to 64 KiB. Slow consumers are disconnected when their bounded outbound queue fills. Reconnect with backoff, mint a new token, and resubscribe.

## History, rewind and batch publish

History retention is opt-in: the publisher token must include `history` for the resolved channel. RelayHub retains only `audience=all` broadcasts; targeted and `others` messages stay live-only so later readers cannot bypass their original audience. Each app/channel Redis Stream is capped at 1,000 messages and a 24-hour TTL. Redis loss can erase it.

```json
{"type":"history.get","channel":"project:42:orders","limit":50,"cursor":"MTIzNC0w"}
{"type":"subscribe","channels":["project:42:orders"],"rewind":{"limit":50}}
```

Both operations require `history`. Pages contain chronological `channel.message` items with an opaque `cursor`, optional `next_cursor`, and a `continuity_cursor`. One rewind frame resolves at most 10 channels. Rewind installs a per-connection barrier before reading Redis, emits `history.result`, then `subscribed`, deduplicates by server message ID (including delayed cross-replica frames) and finally releases buffered live frames. The equivalent HTTP endpoint is `GET /api/v2/realtime/channels/{channel}/history?limit=50&cursor=...` with the Realtime token in `Authorization: Bearer`. History has no ACK, lease, consumer ownership or redelivery.

Batch publish accepts 1–50 items with unique IDs:

```json
{"type":"channel.publish.batch","items":[{"id":"one","channel":"project:42:orders","data":{"n":1}}]}
```

Structure and authorization are validated for the complete batch before dispatch. A rejected preflight dispatches nothing. After preflight, each item receives an independent `accepted`, `message_id` or error `code` outcome.

Redis-backed publish quotas are enforced once per message at three app-scoped dimensions: 10,000/app/minute, 600/connection/minute and 1,200/channel/minute. A dependency failure fails closed with `realtime_unavailable`; exhausted quota returns `rate_limited`.

## End-to-end encrypted private channels

Only `private:*` channels accept encrypted messages. The sender uses AES-256-GCM with a 32-byte application-owned key, a fresh 12-byte nonce, and authenticated data containing the channel plus key ID. RelayHub validates bounds, routes the opaque envelope, and stores that same ciphertext in opt-in history; it never receives keys or plaintext.

```json
{"type":"channel.publish","channel":"private:case-42","encryption":{"algorithm":"aes-256-gcm","key_id":"case-42-v3","nonce":"base64url-12-bytes","ciphertext":"base64url-ciphertext-and-tag"}}
```

Channel names, client identity, timing and payload size remain visible. Server-side payload inspection and moderation are unavailable for ciphertext. Mixed encrypted/plaintext batches are rejected. Applications own key distribution, rotation and revocation.

## Message actions

Tokens with `annotate` may add `reaction` or `annotation` actions to an existing message, list its actions, and remove actions created by the same trusted `client_id`. Every put requires an idempotency key. RelayHub derives actor/app identity from the token, caps actions at 100 per message, and retains removal tombstones for the message-state TTL.

```json
{"type":"message.action.put","channel":"support.room_42","message_id":"msg_...","action_type":"reaction","idempotency_key":"user42-like-v1","data":{"emoji":"👍"}}
{"type":"message.actions.get","channel":"support.room_42","message_id":"msg_..."}
{"type":"message.action.remove","channel":"support.room_42","message_id":"msg_...","action_id":"action_..."}
```

Updates fan out as `message.action.updated` or `message.action.removed`; list replies use `message.actions.result`. Never use an action as durable business processing proof.

## File messages

File bytes move directly between the client and operator-configured S3-compatible storage. RelayHub stores only app/channel-fenced metadata in PostgreSQL and sends only that metadata over WebSocket/NATS.

1. Sign `POST /api/v2/realtime/files` with channel, filename, allowed MIME type, byte length (maximum 25 MiB), and lowercase SHA-256.
2. Upload directly to `upload_url` using every returned `required_headers`. The signed checksum header makes object storage verify the bytes.
3. Call `POST /api/v2/realtime/files/{fileID}/complete`; RelayHub verifies size/checksum and invokes the optional scanning hook before marking metadata `ready`.
4. Send `{"type":"file.publish","channel":"...","file_id":"file_..."}` with a token carrying `file.publish`.
5. A trusted backend may request a short-lived URL at `GET /api/v2/realtime/files/{fileID}/download`.

The API returns `503 file_messaging_disabled` when object storage is not configured. Presigned URLs expire after 15 minutes; metadata/object retention defaults to 24 hours. Object keys and provider credentials are never exposed in socket frames.

## Mobile push notifications

Trusted app backends can register APNs or FCM device tokens and bind each device to app-scoped channels. Tokens are encrypted at rest and are never returned by the API, placed in event data, or sent over WebSocket. Do not call these endpoints directly from an untrusted browser.

1. `POST /api/v2/realtime/push/devices` with `{"provider":"fcm","token":"..."}`.
2. `PUT /api/v2/realtime/push/channels/{channel}/devices/{deviceID}` to bind it; use `DELETE` on the same path to unbind.
3. `POST /api/v2/realtime/push/channels/{channel}/notifications` with a title (100 characters), body (500 characters), and JSON data (4 KiB).
4. Delete an obsolete token with `DELETE /api/v2/realtime/push/devices/{deviceID}`.

A publish fans out to at most 100 bound devices and returns a persisted per-device `delivered` or `failed` outcome. Notification data rejects secret-like field names recursively; only identifiers and display-safe data belong there. A missing provider configuration produces `provider_unavailable` outcomes instead of pretending delivery succeeded. APNs/FCM credentials remain server-side and provider responses are reduced to bounded status and message IDs.

Operators enable providers independently with `RELAYHUB_APNS_ENDPOINT`, `RELAYHUB_APNS_AUTHORIZATION`, `RELAYHUB_APNS_TOPIC` or `RELAYHUB_FCM_ENDPOINT`, `RELAYHUB_FCM_AUTHORIZATION`, `RELAYHUB_FCM_PROJECT`. Each provider group is optional but must be complete and HTTPS-only when present. Authorization values are operator-managed and should be rotated before expiry.

## Durable lifecycle callbacks

Apps configured with a callback URL and delivery mode `callback` or `all` receive these normal durable events through RelayHub's existing signed callback, retry and DLQ path:

- `relayhub.realtime.presence.join`, `.update`, `.leave`, `.timeout`
- `relayhub.realtime.client.publish`
- `relayhub.realtime.delivery.failure`

The bounded payload contains only applicable `channel`, `message_id`, `client_id`, `connection_id`, `occupancy`, and `occurred_at` fields. It never contains message data, encrypted plaintext, device tokens, provider credentials, or file bytes. Delivery is at-least-once, so deduplicate by the enclosing event ID.

Verify `X-RelayHub-Signature` against the exact callback bytes, timestamp and request target before parsing the body. The official Go `VerifyCallbackSignature` and Node `verifyCallbackSignature` helpers also enforce a caller-selected replay window. Lifecycle callbacks are disabled for websocket-only, queue-only, disabled, or callback-less apps.

See the [client schema](/schemas/client-frame-v2.schema.json), [server schema](/schemas/server-frame-v2.schema.json), and SDK guides for typed integration.

## Official SDKs

TypeScript browser and Node entry points export `RelayHubRealtimeClient`. Supply a backend token provider and a standard socket factory:

```ts
import {RelayHubRealtimeClient} from "@relayhub/sdk/browser";

const realtime = new RelayHubRealtimeClient({
  baseUrl: "https://relayhub.example",
  clientId: "user_42",
  channels: {"support.room_42": ["subscribe", "publish", "presence"]},
  tokenProvider: request => fetch("/my/realtime-token", {method: "POST", body: JSON.stringify(request)}).then(r => r.json()).then(r => r.token),
  socketFactory: (url, protocols) => new WebSocket(url, protocols),
  encryptionKeyProvider: {
    encryptionKey: async channel => ({keyId: "case-42-v3", key: await load32ByteKey(channel)}),
    decryptionKey: async (channel, keyId) => load32ByteKey(channel, keyId),
  },
  onMessage: message => console.log(message.data),
  onHistory: page => console.log(page.items),
  onBatchResult: result => console.log(result.outcomes),
});
await realtime.connect();
realtime.subscribe(["support.room_42"]);
realtime.publish("support.room_42", {text: "hello"}, {type: "others"});
realtime.history("support.room_42", {limit: 50});
await realtime.publishEncrypted("private:case-42", {text: "secret"});
```

The Go SDK exposes `DialRealtime`, typed frames, subscribe/unsubscribe, publish, presence and serialized writes:

```go
conn, ready, err := client.DialRealtime(ctx, relayhub.RealtimeTokenRequest{
    ClientID: "worker_42",
    Channels: map[string][]string{"support.room_42": {"subscribe", "publish"}},
})
if err != nil { return err }
defer conn.Close()
if err := conn.Subscribe("support.room_42"); err != nil { return err }
if err := conn.PublishEncrypted(ctx, keyProvider, "private:case-42", map[string]any{"text": "secret"}, relayhub.RealtimeAudience{Type: "all"}); err != nil { return err }
_ = ready
```
