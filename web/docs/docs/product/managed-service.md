---
title: Managed integration model
description: Understand the boundary between your application and RelayHub.
---

# Managed integration model

You use RelayHub through an instance and workspace provided by its operator. The operator gives you the endpoint, application identity and credentials required for your integration.

Your application sends events, receives callbacks, runs an agent or requests short-lived browser access. RelayHub handles routing, delivery state, retry decisions and the dashboard view of those operations.

The service boundary is deliberately simple:

| You provide | RelayHub provides |
| --- | --- |
| Application logic and business data | Authenticated connection and routing |
| A receiver or outbound agent | Delivery attempts and status |
| Idempotent handling of repeated events | Retry and dead-letter visibility |
| User permissions for browser clients | Short-lived scoped access |

You do not need to deploy a database, message broker, proxy or realtime server to use the product. If you operate a RelayHub instance yourself, the internal operator runbook is a separate document from this integration guide.

## Start with a use case

Choose [webhook delivery](/integrate/webhooks) for HTTP callbacks, [durable work](/concepts/delivery) for jobs that must wait, or [live updates](/integrate/browser) for connected screens. The [first event guide](/user/first-event) shows how the choices differ.
