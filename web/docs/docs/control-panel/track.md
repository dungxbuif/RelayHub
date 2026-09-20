---
title: Follow an event
description: Understand where a delivery stopped and what to check before retrying.
---

# Follow an event

Track one event from publication to processing before investigating aggregate metrics.

## Was it accepted?

Check the API response. Authentication failures require checking credentials, timestamps and exact signed bytes. For an uncertain network failure, retry with the same idempotency key.

## Did it reach the right app?

Find the event and inspect its destination and timeline. Check the app is enabled and its delivery settings match the integration.

For queues, confirm that the subscription existed and was enabled at publication and that its event-type filter matches.

## Is it being processed?

The event detail page shows each delivery's target, transport, status, generation and attempt count. The attempt table shows recorded start times, elapsed time between recorded timestamps, outcome and reason. The page refreshes every five seconds while open.

For HTTP troubleshooting, keep the server-generated `X-Request-ID` response header. An operator can match it to the structured request log, which includes route, status, response size and duration. Queue lease logs identify the event and delivery; callback logs include the HTTP status and retry classification. Request bodies, tokens and queue receipts are excluded from these logs.

For callbacks, inspect attempts and receiver logs. For queues, check available and in-flight work, connectivity and leases. For streams, check the consumer connection and acknowledgements.

An accepted publication or open socket does not prove business processing completed.

## Can you replay it?

Fix the cause first. Select specific dead-letter deliveries for replay. Keep duplicate detection active: a previous attempt may have performed part or all of the action.

Realtime history recovers recent broadcasts; it does not replace durable delivery recovery.
