---
title: API reference
description: HTTP and messaging contracts for RelayHub integrations.
---

# API reference

Use the guides to choose a workflow and downloadable contracts to implement exact requests and responses.

## HTTP

[Download OpenAPI](/openapi.json) or import it into your API tooling.

| Task | Endpoint |
| --- | --- |
| Publish an event | `POST /api/v1/events` |
| Inspect an event | `GET /api/v1/events/{eventID}` |
| Issue a client token | `POST /api/v1/socket/token` |
| Create a subscription | `POST /api/v2/subscriptions` |
| Pull work | `POST /api/v2/subscriptions/{subscriptionID}/pull` |
| Settle work | `POST /api/v2/subscriptions/{subscriptionID}/settle` |
| Register a function | `POST /api/v1/functions` |
| Invoke a function | `POST /api/v1/functions/{functionID}/invoke` |

Paths are literal API identifiers. Preserve their prefixes when implementing clients.

## Messaging

[AsyncAPI](/asyncapi.yaml) describes messaging contracts. Schemas define [realtime client frames](/schemas/client-frame-v2.schema.json), [server frames](/schemas/server-frame-v2.schema.json), and [durable stream frames](/schemas/stream-server-frame.schema.json).

## Authentication and tooling

App calls use [HMAC signing](/api/signature-and-streaming). Sockets use short-lived tokens. Administration requires a login session and CSRF protection for mutations.

Get [SDK source and agent tools](/developer/skills-tab), or fetch the [plain-text reference](/llms-full.txt).
