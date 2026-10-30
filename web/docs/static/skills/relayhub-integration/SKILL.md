---
name: relayhub-integration
description: Use when an agent needs to integrate an application with RelayHub's public events, webhooks, durable work or live updates.
---

# RelayHub integration skill

Use RelayHub as a managed service for application events, webhook delivery, durable work and live updates. Choose the smallest public capability that fits the user's goal; do not ask the user to install or operate RelayHub components.

## Public references

- Product overview: `https://relayhub.dungxbuif.com/`
- v1 integration guide: `https://relayhub.dungxbuif.com/api/v1`
- Authentication: `https://relayhub.dungxbuif.com/security/auth`
- Exact HTTP contract: `https://relayhub.dungxbuif.com/openapi.json`
- Exact message contract: `https://relayhub.dungxbuif.com/asyncapi.yaml`
- Complete text reference: `https://relayhub.dungxbuif.com/llms-full.txt`

## Choose a delivery style

1. Use webhook delivery when a trusted service should receive an HTTP callback.
2. Use durable work when processing may be delayed or the receiver may reconnect later.
3. Use live updates for connected screens where a missed update can be recovered from application state.

Do not claim that an accepted event means the receiver has completed its work. Read delivery status or use the receiver's idempotent event handling.

## Authentication

Keep application keys and signing secrets on trusted servers. Browsers receive only short-lived, narrowly scoped tokens. Never put an application secret in browser code, a URL, a log, an issue or an example committed to source control.

For signed requests, sign the exact timestamp, HTTP method, path and body bytes described in the [authentication reference](https://relayhub.dungxbuif.com/skills/relayhub-integration/references/authentication.md). Reject stale timestamps and compare signatures in constant time.

## Integration procedure

1. Ask the user for the RelayHub instance URL and the credential already approved for the application.
2. Confirm the requested capability and version before writing code.
3. Use the matching Node.js or Go client package, or a standard HTTP/WebSocket client with the public contract.
4. Send one test event with a stable idempotency key.
5. Verify the response, event identity, destination and receiver outcome.
6. Add retries with the same idempotency key and deduplicate receiver side effects by event ID.

If the user asks for an unavailable or planned capability, say it is not shipped and do not invent an endpoint or SDK method.

## Safety boundaries

The user owns application logic and data. RelayHub's operator owns the managed service and its operational recovery. Do not provide deployment instructions, internal component names, private routes or operator credentials as part of an application integration.

## Output rule

When generating code, include the API version, required environment variable names, a redacted example, error handling, idempotency behavior and a verification command. Keep secrets as placeholders and tell the user where their approved secret store is expected to provide them.
