# Choose a delivery style

Start with what the receiver needs: a notification, a job, a live message or an immediate answer.

## Webhooks: call an existing endpoint

Use webhooks when your service accepts HTTPS requests. RelayHub sends a signed callback and tracks delivery attempts. An order event could trigger an invoice service.

The receiver verifies the signature, deduplicates the event and responds successfully after safely accepting it. [Build a webhook integration](/integrate/webhooks).

## Queues: take work when ready

Use queues for document processing, imports and background jobs. Workers pull batches, receive temporary leases and acknowledge successful work. Failed or abandoned work can be retried.

A worker on a private network makes outbound requests without exposing its own public endpoint. [Build durable work](/concepts/delivery).

## Realtime: update connected clients

Use channels for live dashboards, room messages and presence. Target one client or broadcast to a room.

Recent live updates can help clients catch up after reconnecting. They are limited retention, not a permanent record or an acknowledged job queue. [Connect a browser](/integrate/browser).

## Durable streams: process work over one connection

Use a persistent connection to receive events and acknowledge them after processing. Unfinished work can be redelivered. [Read delivery behavior](/concepts/delivery).

## Combine them deliberately

For an OCR workflow, send a document reference as a durable job. Let a worker save the extracted text, then publish a realtime progress update. Store the final result in your application's database so a disconnected user can retrieve it later.
