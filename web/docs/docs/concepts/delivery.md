---
title: Delivery behavior
description: Choose sync, async, durable and live delivery with clear expectations.
---

# Delivery behavior

RelayHub reports acceptance separately from receiver completion. A successful acceptance means the event was recorded for delivery. Inspect the delivery state before treating a downstream action as complete.

## Synchronous handling

Use synchronous handling when the sender needs the receiver's HTTP response. The response has a bounded timeout. If the receiver is offline, the endpoint policy decides whether RelayHub queues the request or rejects it.

An expired HTTP request does not prove that the receiver stopped. Make the receiver's effect idempotent and inspect the event timeline.

## Asynchronous handling

Use asynchronous handling when the sender can continue after acceptance. RelayHub records attempts, retries temporary failures and moves exhausted work to a dead-letter state.

## Repeated delivery

Delivery is at least once. The same logical event can be observed more than once after a timeout, reconnect or lost acknowledgement. Store the event ID before applying a business effect.

## Live-only messages

Live messages are useful hints for connected clients. They do not replace a durable event or your application's database. Reconnect clients and refresh authoritative state when a gap is detected.
