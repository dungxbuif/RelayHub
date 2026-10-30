---
title: What you can build
description: Connect applications, agents and users without operating the delivery service yourself.
---

# What you can build

RelayHub gives your applications a shared way to receive events, deliver work and show live progress. You bring the application logic. The RelayHub operator provides the managed connection, delivery history and access controls.

## Receive a provider webhook in a local application

Point an external provider at an endpoint in RelayHub. An outbound agent keeps the connection open from your private network and forwards the request to your local application. Your application can return a response even when it has no public URL.

Use this for payment notifications, repository events, form submissions and internal tools. Choose synchronous handling when the sender needs an immediate response. Choose queued handling when the application may be offline or the work may take longer.

## Continue work after a laptop or agent reconnects

Submit a job and let an agent receive it when it is available. RelayHub keeps the event, tracks attempts and exposes the outcome. Your worker acknowledges after it has safely stored or applied the work.

This fits document processing, imports, notifications, OCR and scheduled tasks. Duplicate delivery is possible, so the application uses the event ID to make its effect safe to repeat.

## Connect services without point-to-point plumbing

Publish an event once and route it to the applications that need it. Each receiver can use its own delivery mode and inspect its own progress. The sender does not need to know how every receiver is connected.

## Keep a dashboard current

Use live updates for progress, online state and room messages. A disconnected screen can request the authoritative state from your application when it returns. Use durable delivery for work that must be processed.

## What you maintain

You maintain your applications, credentials and business data. The RelayHub operator maintains the connection service, delivery state, retries, access control and operational recovery. You do not install or maintain the service's internal components to follow these integration guides.
