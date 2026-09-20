---
title: Send your first event
description: Connect two applications and verify an event end to end.
---

# Send your first event

Connect an order service to a receiving application. Publish `order.created`, then check that it reached the intended destination.

## Before you start

You need a RelayHub instance and application credentials issued by its administrator. A Control Panel account and an application's API key are different credentials.

Ask your administrator to create two enabled apps: a producer and a receiver. For a webhook receiver, configure its HTTPS callback URL and callback delivery mode. Keep the producer's API key and HMAC secret in backend secret storage.

## Send an event

This Node.js example uses built-in modules so you can try the API before installing an SDK. Set `RELAYHUB_URL`, `RELAYHUB_API_KEY`, `RELAYHUB_HMAC_SECRET` and `RELAYHUB_TARGET_APP_ID` in your process environment.

```js
import {createHash, createHmac} from "node:crypto";

const path = "/api/v1/events";
const body = JSON.stringify({
  type: "order.created",
  target_app_ids: [process.env.RELAYHUB_TARGET_APP_ID],
  data: {order_id: "order-demo-001", total: 49},
});
const timestamp = String(Math.floor(Date.now() / 1000));
const digest = createHash("sha256").update(body).digest("hex");
const signature = createHmac("sha256", process.env.RELAYHUB_HMAC_SECRET)
  .update([timestamp, "POST", path, digest].join("\n"))
  .digest("hex");

const response = await fetch(new URL(path, process.env.RELAYHUB_URL), {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    "X-RelayHub-Api-Key": process.env.RELAYHUB_API_KEY,
    "X-RelayHub-Timestamp": timestamp,
    "X-RelayHub-Signature": signature,
    "Idempotency-Key": "order-demo-001-created",
  },
  body,
});
const result = await response.json();
if (!response.ok) throw new Error(JSON.stringify(result));
console.log(result);
```

An accepted publication means RelayHub accepted the event. Check delivery separately before treating the receiver's work as complete. Reuse the same idempotency key when retrying this logical publication.

## Check the result

Open **Events** in the [Control Panel](https://relayhub.dungxbuif.com/admin/) and locate the event. Inspect its destination and delivery timeline. Confirm that your callback receiver verified the signature and handled the event successfully.

If authentication fails, check the key, secret, clock and exact signed bytes. If publication succeeds but processing does not, check the destination app, its delivery settings and the timeline.

## Connect your application

Replace the sample order with your own event type and data. Deduplicate events in the receiver using a stable identifier. Once this flow works, use [routing rules](/control-panel/overview) to configure destinations centrally, or add a [queue worker](/developer/queue).
