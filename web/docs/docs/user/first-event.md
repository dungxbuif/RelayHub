---
title: Choose a delivery style
description: Choose between webhooks, queues, realtime, durable streams and remote functions.
---

# Choose a delivery style

Start with what the receiver needs: a notification, a job, a live message or an immediate answer.

## Webhooks: call an existing endpoint

Use webhooks when your service accepts HTTPS requests. RelayHub sends a signed callback and tracks delivery attempts. An order event could trigger an invoice service.

The receiver verifies the signature, deduplicates the event and responds successfully after safely accepting it. [Build a webhook receiver](/developer/webhooks).

## Queues: take work when ready

Use queues for document processing, imports and background jobs. Workers pull batches, receive temporary leases and acknowledge successful work. Failed or abandoned work can be retried.

A worker on a private network makes outbound requests without exposing its own public endpoint. [Build a queue worker](/developer/queue).

## Realtime: update connected clients

Use channels for live dashboards, room messages and presence. Target one client or broadcast to a room.

Recent broadcast history can help clients catch up after reconnecting. It is limited retention, not a permanent record or an acknowledged job queue. [Connect realtime clients](/developer/realtime).

## Durable streams: process work over one connection

Use a persistent WebSocket connection to receive events and acknowledge them after processing. Unfinished work can be redelivered. [Consume a durable stream](/developer/streaming).

## Remote functions: request an answer

Use functions for a short operation handled by an online application, such as checking a device or running a calculation. Use a queue for lengthy work or work that must wait for an offline worker. [Call a remote function](/developer/functions).

## Combine them deliberately

For an OCR workflow, send a document reference as a durable job. Let a worker save the extracted text, then publish a realtime progress update. Store the final result in your application's database so a disconnected user can retrieve it later.
