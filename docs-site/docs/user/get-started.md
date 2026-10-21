---
title: User Onboarding
description: Choose a delivery workflow and send your first RelayHub event.
---

# User Onboarding

## A quick first workflow

1. Open **Control Panel** and create a producer and consumer app.
2. Choose a delivery mode and add a callback URL when durable HTTP delivery is
   the best fit.
3. Add a routing rule for the event type.
4. Publish a test event and watch the delivery or realtime channel.

## Quick checks

- `POST /api/v1/apps` creates an application.
- `POST /api/v1/routing/rules` connects an event type to a target app.
- `POST /api/v1/events` publishes a signed event.
- The [API reference](https://relayhub.dungxbuif.com/docs/api.md) contains the
  exact request and response contracts.

## When to use stream instead of WebSocket

Use WebSocket for online notifications and realtime channels. Use callbacks or
the durable stream when the consumer must recover work after a disconnect.
