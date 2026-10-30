---
title: RelayHub concepts
description: Product-level concepts for events, agents, delivery and live updates.
---

# RelayHub concepts

## Event

An event describes something your application wants another application or agent to know. It has a stable ID, a type, data and an idempotency key.

## Delivery

A delivery is RelayHub's record of sending one event to one receiver. It has its own state, attempt history and outcome. A single event can have several deliveries.

## Agent

An agent is your outbound connection to a local application. Because it connects outward, the local application can receive requests without a public inbound address.

## Live update

A live update is intended for connected clients such as dashboards and screens. It may be missed during a disconnect. Keep the source of truth in your application and use durable delivery for required work.

## Workspace and application

A workspace separates one team's data and credentials. Applications and agents belong to a workspace. RelayHub derives that identity from credentials rather than trusting a workspace value supplied by a client.
