---
title: Webhook delivery
description: Receive signed callbacks and handle retries safely.
---

# Webhook delivery

Connect a service that already accepts HTTPS requests. RelayHub calls its configured endpoint when an event is delivered to the application.

## Configure the receiver

Ask an administrator to set the app's callback URL and enable callback delivery. Publish an event targeting that app or configure a matching routing rule.

## Verify before processing

Preserve raw request bytes. Verify the RelayHub signature against the exact body, timestamp and request target with the app secret. The TypeScript SDK exports `verifyCallbackSignature`; Go provides `VerifyCallbackSignature`.

Enforce a timestamp replay window and reject invalid signatures before parsing JSON.

## Acknowledge safely

Record the event ID and apply changes idempotently. Return success after processing or after durably accepting the event into your own processing system.

Timeouts and retries can cause duplicate delivery. A timed-out request may still have performed its business action.

## Recover failures

Inspect the event timeline in the Control Panel. Fix the receiver before replaying a dead-letter delivery. Keep duplicate detection active during recovery.

Workers without a public endpoint can use [queues](/developer/queue). See [authentication](/api/signature-and-streaming) and [OpenAPI](/openapi.json) for the request contract.
