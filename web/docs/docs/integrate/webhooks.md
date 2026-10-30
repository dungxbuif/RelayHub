---
title: Webhook integration
description: Receive external requests locally or send signed callbacks to an application.
---

# Webhook integration

RelayHub supports two useful directions: receive an external provider request through an outbound agent, or send a signed callback to an HTTPS receiver.

## Receive into a local app

Create an endpoint in the Control Panel, connect an agent from the same private network as your application, and map the endpoint to a fixed local URL. RelayHub preserves the request method, path, safe headers and raw body. Your app returns the response through the same request when the endpoint uses synchronous handling.

The agent mapping is fixed by its configuration. A request cannot choose an arbitrary local destination. Keep the agent credential in the agent's private environment.

## Send a callback

Configure a receiver URL for the target application. RelayHub sends a signed POST, records every attempt and retries temporary failures. Verify the signature over the exact received bytes before parsing them, then deduplicate by event and delivery ID.

## Recover failures

Use the Control Panel to inspect attempts and dead-letter deliveries. Fix the receiver first, then replay only the selected failed delivery. A replay is a new delivery attempt; it does not change the original event.

See [authentication](/security/auth), [delivery behavior](/concepts/delivery) and the [API reference](/api/v1) for exact fields and limits.
