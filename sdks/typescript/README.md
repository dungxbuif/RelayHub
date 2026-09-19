# @relayhub/sdk

Official RelayHub TypeScript SDK for Node.js and browsers. It includes signed HTTP clients, durable stream consumers, legacy realtime compatibility, remote functions, and Realtime v2.

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

Realtime v2 uses standard RFC 6455 with subprotocol `relayhub.realtime.v2`. It is online-only; use the durable stream or callbacks for reliable work.

Release tags named `sdk-typescript-v<package-version>` publish through npm trusted publishing with provenance. Configure the GitHub repository/environment as an npm trusted publisher before creating a tag; no long-lived npm token is required.

See the official RelayHub docs for authentication, frame schemas, retries and reliability rules.
