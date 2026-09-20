---
title: Realtime channels
description: Build live dashboards, connected rooms and presence.
---

# Realtime channels

Channels connect the people and processes that need to see an update now. Use them for dashboard changes, room messages, presence and progress updates.

Channels are isolated by application. A token identifies a client and grants explicit channel permissions.

## Issue a token

Your authenticated backend signs `POST /api/v1/socket/token`:

```json
{
  "protocol": "realtime.v2",
  "client_id": "user_42",
  "channels": {"orders:42": ["subscribe", "publish", "presence", "history"]},
  "ttl_seconds": 600
}
```

Derive identity and permissions from your own authenticated user. Do not grant channels simply because a browser requested them.

## Connect a browser

The [TypeScript SDK](/developer/skills-tab) accepts a token provider implemented by your backend:

```ts
import {RelayHubRealtimeClient} from "@relayhub/sdk/browser";

const realtime = new RelayHubRealtimeClient({
  baseUrl: "https://relayhub.dungxbuif.com",
  clientId: "user_42",
  channels: {"orders:42": ["subscribe", "publish", "presence", "history"]},
  tokenProvider: async request => {
    const response = await fetch("/my/realtime-token", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify(request),
    });
    if (!response.ok) throw new Error("Could not obtain a token");
    return (await response.json()).token;
  },
  socketFactory: (url, protocols) => new WebSocket(url, protocols),
  onMessage: message => console.log(message.data),
});

await realtime.connect();
realtime.subscribe(["orders:42"]);
realtime.publish("orders:42", {status: "processing"});
```

You implement `/my/realtime-token` in your application. It keeps the app secret out of the browser.

## Target messages and show presence

Broadcast, exclude the sender, or target a client or connection. Presence represents online state; it is not a permanent user record.

A raw WebSocket client connects to `/ws` with the `relayhub.realtime.v2` subprotocol. Send protocol identifiers exactly as specified.

## Recover after disconnecting

History permission allows retrieving or rewinding recent broadcasts. Retention is limited to 1,000 messages per app/channel with a 24-hour TTL. Targeted messages and messages to “others” are live-only. Redis loss may erase history.

Reconnect with backoff and recover authoritative business state from your application when needed. Use queues or durable streams for acknowledged processing.

## Go further

Reactions and annotations add interactions to messages. Private channels support application-managed encryption. File messages require operator-configured S3-compatible storage, and mobile push requires configured APNs/FCM providers.

Your application owns encryption keys and rotation. Channel names and message metadata remain visible.

See [AsyncAPI](/asyncapi.yaml) and the [client schema](/schemas/client-frame-v2.schema.json) for exact message formats.
