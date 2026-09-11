# API Reference

Base URL: `https://relayhub.dungxbuif.com`. The JSON API is under `/api/v1`;
durable streaming uses `/api/v1/stream`, and best-effort WebSocket uses `/ws`.
Operations use `/healthz`, `/readyz`, `/metrics`.
All HTTP routes, auth categories, schemas, headers, statuses and examples are in
[OpenAPI 3.1](openapi.json). See the [readable API guide](developer/api-overview.md).

## Contracts

- [Event envelope schema](schemas/event-envelope.schema.json).
- [Client frame schema](schemas/client-frame.schema.json).
- [Server frame schema](schemas/server-frame.schema.json).
- [Durable stream protocol](developer/streaming-protocol.md),
  [AsyncAPI](asyncapi.yaml), [client schema](schemas/stream-client-frame.schema.json)
  and [server schema](schemas/stream-server-frame.schema.json).
- [Signing and credentials](developer/auth.md).
- [Functions](developer/functions.md) and [WebSocket handshake](developer/websocket.md).

Schemas use JSON Schema 2020-12. Byte limits, ownership, expiry, state transitions
and signature checks are runtime constraints described alongside the schemas.
JSON Schema character limits do not replace UTF-8 byte limits. HTTP bodies are
limited to 1 MiB; complete RPC and inbound WebSocket messages to 64 KiB.

## Errors and retries

Errors are JSON `{"error":{"code":"invalid_request","message":"The request is invalid."}}`.
Interpret status and code, not message wording. Unknown routes use 404 `not_found`;
unsupported methods use 405 `method_not_allowed`. Ack/delete-function success is
204 with no body. Function handler errors use HTTP 200 with `ok:false`.

Use stable idempotency keys for event publish and function invoke when retrying
uncertain responses. Retention and terminal replay rules differ; read
[reliability](developer/reliability.md) and [functions](developer/functions.md).

## Stable agent resources

Use [llms.txt](llms.txt) for discovery, [llms-full.txt](llms-full.txt) for the
concatenated Markdown reference, and [Skills](skills.md) for the downloadable pack.
The API embeds these exact assets; deployment needs no separate docs server.
