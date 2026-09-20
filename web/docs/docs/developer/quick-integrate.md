---
title: Backend integration
description: Connect a trusted service with app credentials and the TypeScript SDK.
---

# Backend integration

Your backend publishes events, creates subscriptions and issues restricted tokens for clients. Give each independently managed application its own credentials.

## Publish with the SDK

[Download and install the TypeScript SDK](/developer/skills-tab). Its Node entry point signs application requests for you.

```ts
import {RelayHubClient} from "@relayhub/sdk/node";

const client = new RelayHubClient({
  baseUrl: process.env.RELAYHUB_URL!,
  apiKey: process.env.RELAYHUB_API_KEY!,
  hmacSecret: process.env.RELAYHUB_HMAC_SECRET!,
});

const result = await client.events.publish({
  type: "order.created",
  target_app_ids: [process.env.RELAYHUB_TARGET_APP_ID!],
  data: {order_id: "order-42"},
}, {idempotencyKey: "order-42-created"});
```

Reuse the key when retrying the same publication. Use a new key for a different operation. Keep app credentials on trusted servers.

## Receive data

| Integration | Guide |
| --- | --- |
| Receive signed HTTP callbacks | [Webhooks](/developer/webhooks) |
| Pull background work | [Queues](/developer/queue) |
| Connect browser or mobile clients | [Realtime](/developer/realtime) |
| Process events over WebSocket | [Durable streams](/developer/streaming) |
| Register or call a handler | [Remote functions](/developer/functions) |

## Use HTTP directly

Sign the exact HTTP method, path including query string, and body bytes. See [authentication](/api/signature-and-streaming) for the signing format and [OpenAPI](/openapi.json) for request and response fields.

Administration uses a login session. Use the Control Panel to manage apps and routing; older SDK admin-token helpers do not authenticate against the current session-based admin API.

## Make retries safe

A response can be lost after acceptance. Use idempotency keys where supported, check recorded outcomes, and make downstream business operations idempotent. Event acceptance, delivery and business completion are separate steps.
