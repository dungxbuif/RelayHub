# What RelayHub does

RelayHub gives your applications a shared way to publish, route, and receive
events. A service publishes an event once; RelayHub sends it to the applications
that should see it and keeps the delivery workflow visible while the receiving
service processes it.

It is useful when several services need to communicate without each pair of
services carrying its own delivery, retry, and real-time integration code.

## The capabilities

### Durable event delivery

Publish an event with a type, data, and one or more targets. Each target gets its
own delivery state, so a consumer can process work at its own pace and recover
after a temporary outage. Use callbacks or the durable stream when the work must
survive a disconnect.

### Routing rules

Describe which application receives a class of event. Routing can use the source
application and event type, and can also fan a single publication out to several
targets. The publisher stays focused on the event; the route decides who should
receive it.

### Real-time updates

Use standard WebSocket connections and named realtime channels for live product
updates, presence signals, dashboards, and other online experiences. Realtime
messages are best for immediate awareness; pair them with durable delivery when
the underlying work cannot be missed.

### Signed callbacks

Deliver events directly to an HTTPS endpoint owned by a receiving application.
RelayHub signs the request so the receiver can verify its origin, retries
temporary failures, and records the final delivery outcome.

### Remote functions

Register a function on an online application and invoke it from another trusted
application. RelayHub selects the connected handler, waits for its result, and
returns a normal request/response outcome with a bounded deadline.

### A control surface for teams and agents

The management console helps operators create applications, configure routes,
publish test events, and inspect realtime connections. Developers can use the
HTTP API, WebSocket protocol, OpenAPI schemas, or the downloadable integration
Skill. The same references are available as Markdown and `llms.txt` for agents.

## Choose the right path

| You need to… | Use |
| --- | --- |
| Process work even when a consumer reconnects later | Callbacks or durable stream |
| Show a live update to connected clients | WebSocket realtime channel |
| Send a domain event to selected services | Routed event |
| Ask an online service for a bounded result | Remote function |
| Explore the API before writing code | Management console and OpenAPI |

## A typical integration

1. Create an application for each producer or consumer.
2. Add a route for the event types that should move between them.
3. Publish a signed event from the producer.
4. Receive it through a callback, durable stream, or WebSocket subscription.
5. Process and acknowledge the work in the consumer, using the event ID for
   deduplication when a retry is possible.

Start with the [first event walkthrough](user/getting-started.md), then choose a
[developer integration path](developer.md). When you need exact request and
frame contracts, use the [API reference](api.md).

## What this documentation covers

- **Product and user guides** explain the workflows and delivery choices.
- **Developer references** cover authentication, event contracts, callbacks,
  WebSocket connections, streams, and functions.
- **Deployment and operations** cover installation, security, persistence,
  backups, upgrades, and troubleshooting for teams running RelayHub themselves.
- **Agent references** provide `llms.txt`, the complete Markdown bundle, OpenAPI,
  schemas, and an integration Skill.
