---
title: Durable event streams
description: Receive and acknowledge work over a persistent WebSocket connection.
---

# Durable event streams

A durable stream keeps a worker connected while RelayHub delivers events over WebSocket. Choose it when a long-lived connection fits your application better than HTTP polling.

## Connect

From a trusted backend, request a socket token with the `stream:connect` scope. Connect to `/api/v1/stream?token=...` with the `relayhub.stream.v1` subprotocol.

After the ready frame, start consumption:

```json
{"type":"consumer.start","protocol_version":1,"consumer":"default","max_in_flight":16}
```

Each app owns a durable consumer named `default`. Multiple worker connections share its work.

## Process and acknowledge

After receiving an `event.delivery` frame, save the result and acknowledge its delivery ID:

```json
{"type":"delivery.ack","delivery_id":"delivery_from_received_frame"}
```

To request a retry:

```json
{"type":"delivery.nack","delivery_id":"delivery_from_received_frame","delay_ms":5000}
```

Make processing idempotent. Disconnects and interrupted acknowledgements can cause redelivery. Bound in-flight work to your worker's capacity.

## Choose the right consumer

Streams provide ongoing delivery over a connection. [Queues](/developer/queue) provide named subscriptions, batch pulling and subscription policies. [Realtime](/developer/realtime) supports live interactions with different recovery guarantees.

Use the [client schema](/schemas/stream-client-frame.schema.json), [server schema](/schemas/stream-server-frame.schema.json) and [protocol reference](https://relayhub.dungxbuif.com/docs/developer/streaming-protocol.md) when implementing a client.
