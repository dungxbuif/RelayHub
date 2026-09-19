# TypeScript SDK

## Realtime v2

`@relayhub/sdk/browser` and `@relayhub/sdk/node` export `RelayHubRealtimeClient`.
It negotiates `relayhub.realtime.v2` and provides typed subscribe, unsubscribe,
targeted publish, presence and server-frame callbacks. The browser token provider
must call your trusted backend; never expose app signing credentials to browser code.

```ts
const realtime = new RelayHubRealtimeClient({
  baseUrl,
  clientId: "user_42",
  channels: {"support.room_42": ["subscribe", "publish", "presence"]},
  tokenProvider,
  socketFactory: (url, protocols) => new WebSocket(url, protocols),
});
await realtime.connect();
realtime.subscribe(["support.room_42"]);
realtime.publish("support.room_42", {text: "hello"}, {type: "others"});
```

Install the Node.js 20+ SDK:

```bash
npm install @relayhub/sdk
```

Application credentials belong only in trusted Node.js services:

```ts
import { RelayHubClient } from "@relayhub/sdk/node";

const relayhub = new RelayHubClient({
  baseUrl: "https://relayhub.dungxbuif.com",
  apiKey: process.env.RELAYHUB_API_KEY!,
  hmacSecret: process.env.RELAYHUB_HMAC_SECRET!,
  adminToken: process.env.RELAYHUB_ADMIN_TOKEN, // needed only for app/routing management
});

const credentials = await relayhub.apps.create({ name: "orders", delivery_mode: "websocket" });
const rules = await relayhub.routing.listRules();

await relayhub.events.publish({
  type: "order.created",
  data: { order_id: 42 }, // target_app_ids is optional when routing rules match
}, { idempotencyKey: "order-42-created" });

await relayhub.realtime.publish("orders", { order_id: 42 });
const channel = relayhub.realtime.subscribe("orders", async (message) => {
  console.log(message.channel, message.publisherAppId, message.data);
});

const consumer = relayhub.events.consume(async (event, context) => {
  await saveOrderOnce(event, context.deliveryId);
}, { concurrency: 16 });

await shutdownSignal;
await consumer.drain();
await relayhub.close({ drain: true });
```

The SDK obtains a fresh short-lived token on every connection, reconnects with
jitter, and sends ACK only after the handler succeeds. Throw `RetryDelivery`
with an optional bounded delay to request NACK/redelivery. `consume` has no topic
filter in v1: all replicas share the application consumer `default` safely.

Browser code imports `@relayhub/sdk/browser` and supplies a backend token
callback. That entry point has no signer, API key option or HMAC secret:

```ts
import { RelayHubStreamClient } from "@relayhub/sdk/browser";

const stream = new RelayHubStreamClient({
  baseUrl: "https://relayhub.dungxbuif.com",
  tokenProvider: async () => fetch("/relayhub-token").then(r => r.text()),
});
```

Use `events.observe` for best-effort event notifications and `realtime.subscribe` for online channel messages; neither replaces durable consumers. Use `functions.handle(name, handler)` in an online owner and
`functions.invoke(id, input, {idempotencyKey})` in a caller. Errors are
`RelayHubError` values with stable `code`, HTTP `status`, `retryable` and optional
request ID fields.

Durable consumption connects to `/api/v1/stream`. Observation, realtime channels and function handling connect separately to `/ws`; the SDK does not claim functions over the durable event connection.


SDK intentionally types JSON payload values as `unknown` to keep TypeScript compilation fast and stable. RelayHub validates that event `data`, realtime channel `data`, function input and callback bodies are JSON objects at the HTTP/API boundary.
