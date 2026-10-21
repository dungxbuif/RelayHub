# Getting started

The quickest path is a producer application, a consumer application, one route,
and one test event. If RelayHub is not running yet, follow the
[deployment guide](../deploy/README.md) first. Once the service is ready, use
the [Admin console](/admin/) or the API to create the two applications.

## Provision applications

An operator calls admin-authenticated `POST /api/v1/apps` with
`{"name":"orders","delivery_mode":"queue"}`. Repeat for the consumer. The response
is 201 with `app_id`, `api_key`, and `hmac_secret`, returned once. Store them in an
approved secret manager; do not paste them into chat, browser code, logs or source.
Use [the registration flow](../developer/registration-flow.md) for exact requests,
rotation and disable behavior. Operators list apps using admin `GET /api/v1/apps`.

## Deliver your first event

Use [the signed Python publish example](../developer/api-overview.md#copyable-signed-publish).
Publish a synthetic event addressed to the consumer. A `202` response means
RelayHub accepted the event and created delivery work. As consumer, use callbacks
or `/api/v1/stream`, deduplicate by event ID, then commit processing.

## Choose how the consumer wakes up

- `websocket`: subscribe to event hints or realtime channels; use callbacks/stream for reliable recovery.
- `callback`: set a reachable HTTPS callback URL and run the callback worker.
- `all`: combine callback eligibility with realtime observation and stream recovery.

Set a URL through signed PATCH of the consumer's app; `callback_url:null` removes
it. WebSocket subscription remains explicit in all modes. Callback receivers must
verify signatures and deduplicate events. Read [reliability](../developer/reliability.md)
before production: acceptance is not a guarantee of exactly-once processing.
