---
title: Node.js integration
description: Send events and receive live or durable work from a trusted service.
---

# Node.js integration

Use the Node.js SDK in a trusted service. Keep application credentials in that service and pass only short-lived scoped tokens to browsers.

```bash
npm install @relayhub/sdk
```

Create a client with the instance URL and application credentials supplied by the operator. Publish with a stable idempotency key, inspect delivery state, and handle duplicate events by ID.

```js
import {RelayHubClient} from "@relayhub/sdk";

const client = new RelayHubClient({
  baseUrl: process.env.RELAYHUB_URL,
  apiKey: process.env.RELAYHUB_API_KEY,
  signingSecret: process.env.RELAYHUB_SIGNING_SECRET,
});

const accepted = await client.events.publish({
  type: "order.created",
  targetAppIds: [process.env.RELAYHUB_TARGET_APP_ID],
  data: {orderId: "order-001"},
  idempotencyKey: "order-001-created",
});

console.log(accepted.eventId, accepted.deliveryId);
```

For a browser, use the browser entry point and a token provider from your application. Never put the application signing secret in browser code.
