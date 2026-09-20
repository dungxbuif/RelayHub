# @relayhub/sdk

Official RelayHub TypeScript SDK for Node.js and browsers. It includes signed HTTP clients, Queue v2 pull workers, durable stream consumers, legacy realtime compatibility, remote functions, and Realtime v2.

```bash
npm install @relayhub/sdk
```

Use `@relayhub/sdk/node` on trusted backends with an API key and HMAC secret. Use `@relayhub/sdk/browser` with a backend-provided short-lived token; never ship application credentials to a browser.

```ts
import {RelayHubRealtimeClient} from "@relayhub/sdk/browser";

const realtime = new RelayHubRealtimeClient({
  baseUrl: "https://relayhub.example",
  clientId: "user_42",
  channels: {"support.room_42": ["subscribe", "publish", "presence"]},
  tokenProvider: request => issueTokenFromYourBackend(request),
  socketFactory: (url, protocols) => new WebSocket(url, protocols),
});

await realtime.connect();
realtime.subscribe(["support.room_42"]);
realtime.publish("support.room_42", {text: "hello"}, {type: "others"});
```

Realtime v2 uses standard RFC 6455 with subprotocol `relayhub.realtime.v2`. The client supports bounded terminal namespace grants, history/rewind and 50-item batch publish with per-item outcomes. History is TTL/count-bounded reconnect continuity, not reliable work delivery; use Queue v2, durable stream or callbacks for that.

Configure `encryptionKeyProvider` and call `publishEncrypted` for AES-256-GCM `private:*` channels. The provider returns 32-byte keys by key ID; key distribution and rotation stay entirely in the integrating application. Incoming ciphertext is never passed to `onMessage` when no provider is configured.

Use `putMessageAction`, `listMessageActions`, and `removeMessageAction` for bounded reactions/annotations. `onAction` receives updates/tombstones and `onActions` receives list results; actor identity is always token-derived.

Trusted Node clients use `client.realtime.createFile`, upload bytes directly with the returned URL/headers, then call `completeFile` and publish the ready ID with `RelayHubRealtimeClient.publishFile`. `fileDownload` returns a short-lived download URL.

Trusted Node.js services can run a Queue v2 worker with bounded concurrency,
automatic lease heartbeat and graceful drain:

```ts
import {RelayHubClient, RetryDelivery} from "@relayhub/sdk/node";

const client = new RelayHubClient({baseUrl, apiKey, hmacSecret});
const worker = client.queue.work("sub_orders", async delivery => {
  try { await processIdempotently(delivery.event.id, delivery.event.data); }
  catch { throw new RetryDelivery("dependency_busy", {delayMs: 5000}); }
}, {concurrency: 16, visibilitySeconds: 60, heartbeatSeconds: 20});

await worker.drain({timeoutMs: 30_000});
```

Queue v2 is at-least-once. Persist business effects idempotently before the SDK
ACKs. Throw `DeadLetterDelivery` for poison input.

Release tags named `sdk-typescript-v<package-version>` publish through npm trusted publishing with provenance. Configure the GitHub repository/environment as an npm trusted publisher before creating a tag; no long-lived npm token is required.

See the official RelayHub docs for authentication, frame schemas, retries and reliability rules.
