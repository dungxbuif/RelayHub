---
title: RelayHub
description: Connect your apps with webhooks, background jobs, live channels and remote functions.
slug: /
---

# RelayHub

Connect your applications, deliver background work, and bring live updates to your users.

RelayHub is a self-hosted communication hub for applications. It gives your services a shared place to send events, deliver webhooks, distribute work to workers, exchange realtime messages, and call functions running in another application.

A checkout service can announce an order once. A fulfillment worker processes it, a billing service receives a webhook, and your application can publish progress to a live dashboard. Each integration uses the delivery style that fits its job.

## What can you build?

| You want to... | Use RelayHub for... |
| --- | --- |
| Notify another service when something changes | Signed webhook delivery with retries and failure tracking |
| Process documents, reports or background tasks | Pull queues with leases, acknowledgements and retry control |
| Update a dashboard or collaborate in a room | Realtime channels with targeted messages and presence |
| Keep a worker connected to incoming events | Durable event streaming over WebSocket |
| Ask an online service or agent to perform a short action | Remote functions with request and response |

## One hub, different kinds of communication

**Events describe something that happened.** Publish an event such as `order.created` and choose its destination apps, or let an administrator configure routing rules. Receiving apps process it through callbacks, queues or a durable stream.

**Realtime messages keep connected clients up to date.** Publish to an app-scoped channel, send to a specific client, track presence, or recover recent broadcasts after reconnecting. Realtime history is bounded; use durable delivery for work you cannot afford to lose.

**Remote functions return an answer.** Register a handler in an application and invoke it through RelayHub. Your application executes the function. The handler must be online.

## Built for integration

Application credentials authenticate server-to-server calls. Short-lived tokens let browsers and other clients connect with limited channel permissions. A Control Panel helps administrators manage apps, routing, deliveries and failed work.

TypeScript and Go SDKs cover common integration flows. OpenAPI, message schemas and an installable agent Skill support custom clients and AI-assisted development.

## Start building

1. [Send your first event](/user/get-started) to learn the core workflow.
2. [Choose a delivery style](/user/first-event) for your application.
3. [Integrate your backend](/developer/quick-integrate) or [connect a live client](/developer/realtime).
4. [Download SDKs and agent tools](/developer/skills-tab).

Already integrating? Go to the [API reference](/api/open-api-overview).
