---
title: RelayHub
description: Connect your apps with webhooks, background jobs, live channels and remote functions.
slug: /
---

# RelayHub

Connect applications, deliver work and keep users up to date through one managed integration.

RelayHub receives events from your applications and providers, sends them to the right destination, and shows you what happened. You keep your business logic and data in your applications. The RelayHub operator maintains the connection and delivery service.

A checkout service can announce an order once. A local worker can receive it after reconnecting. A billing application can receive a signed callback, while a dashboard receives live progress.

## What can you build?

| You want to... | Use RelayHub for... |
| --- | --- |
| Notify another service when something changes | Signed webhook delivery with retries and failure tracking |
| Process documents, reports or background tasks | Pull queues with leases, acknowledgements and retry control |
| Update a dashboard or collaborate in a room | Realtime channels with targeted messages and presence |
| Keep a worker connected to incoming events | Durable event streaming over WebSocket |
| Connect an online service to another application | Signed callbacks or durable work |

## One hub, different kinds of communication

**Events describe something that happened.** Publish an event such as `order.created` and choose its destination apps, or let an administrator configure routing rules. Receiving apps process it through callbacks, queues or a durable stream.

**Realtime messages keep connected clients up to date.** Publish to an app-scoped channel, send to a specific client, track presence, or recover recent broadcasts after reconnecting. Realtime history is bounded; use durable delivery for work you cannot afford to lose.

**Remote functions return an answer.** Register a handler in an application and invoke it through RelayHub. Your application executes the function. The handler must be online.

## Built for integration

Application credentials authenticate trusted calls. Short-lived tokens let browsers connect with limited permissions. The Control Panel helps operators manage applications, routes, deliveries and failed work.

Node.js and Go SDKs cover common integration flows. Public contracts, examples and an installable integration Skill support custom clients and AI-assisted development.

## Start building

1. [Send your first event](/user/get-started) to learn the core workflow.
2. [Choose a delivery style](/user/first-event) for your application.
3. [Integrate a trusted service](/integrate/node) or [connect a live client](/integrate/browser).
4. [Download SDKs and the integration Skill](/integrate/skills).

Already integrating? Go to the [v1 API reference](/api/v1).
