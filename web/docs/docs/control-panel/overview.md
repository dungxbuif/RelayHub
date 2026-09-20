---
title: Manage applications
description: Set up applications and routing in the Control Panel.
---

# Manage applications

The [Control Panel](https://relayhub.dungxbuif.com/admin/) is where administrators connect applications and check communication.

## Sign in

Use the email and password issued by your administrator. Application API keys belong to integrations and are not login credentials. Public documentation requires no login.

Administrators create user accounts. Ask an administrator for access rather than looking for public self-registration.

## Register apps

Create a separate app for each service that needs its own credentials and delivery settings. Configure an HTTPS callback URL and callback delivery mode for webhook receivers.

Store newly issued credentials in your secret manager when shown. Rotate secrets when replacing credentials and update the corresponding application.

## Configure routing

Rules connect event types and source apps to destinations. For example, route `order.created` from checkout to fulfillment.

Explicit target app IDs are useful for initial tests. Routing rules let administrators manage destinations centrally as integrations grow.

## Check delivery

Find publications in Events and inspect their timelines. Use queue views for subscription state and dead-letter views for failed work. Read [delivery troubleshooting](/control-panel/track) before replaying an action.
