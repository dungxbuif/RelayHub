# Realtime v2

Realtime v2 is RelayHub's app-isolated, bidirectional RFC 6455 protocol for rooms/channels, targeted publish and ephemeral presence. It is online-only; use durable stream or callbacks for work that must survive disconnects.

## Token and connection

From a trusted backend, sign `POST /api/v1/socket/token`:

```json
{
  "protocol": "realtime.v2",
  "client_id": "user_42",
  "channels": {
    "support.room_42": ["subscribe", "publish", "presence"]
  },
  "ttl_seconds": 600
}
```

Channel permissions are exact; wildcard capabilities are not accepted. Connect to `/ws?token=...` with WebSocket subprotocol `relayhub.realtime.v2`. A connection without that subprotocol uses the legacy v1 contract.

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

Inbound frames are limited to 64 KiB. Slow consumers are disconnected when their bounded outbound queue fills. Reconnect with backoff, mint a new token, and resubscribe; realtime messages are not replayed.

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
  onMessage: message => console.log(message.data),
});
await realtime.connect();
realtime.subscribe(["support.room_42"]);
realtime.publish("support.room_42", {text: "hello"}, {type: "others"});
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
_ = ready
```
