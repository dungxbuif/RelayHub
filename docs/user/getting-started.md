# Getting started

You need a running Redis, the RelayHub API, and two applications: a producer and a
consumer. Use [deployment instructions](../../public-docs/deploy/README.md) for the API/worker
stack. Set separate strong `RELAYHUB_ADMIN_TOKEN` and `RELAYHUB_SIGNING_SECRET`
through your deployment secret mechanism. Check `/healthz` and `/readyz` first.
There is no tenant setup or admin web UI in this release.

## Provision applications

An operator calls admin-authenticated `POST /api/v1/apps` with
`{"name":"orders","delivery_mode":"queue"}`. Repeat for the consumer. The response
is 201 with `app_id`, `api_key`, and `hmac_secret`, returned once. Store them in an
approved secret manager; do not paste them into chat, browser code, logs or source.
Use [the registration flow](../developer/registration-flow.md) for exact requests,
rotation and disable behavior. Operators list apps using admin `GET /api/v1/apps`.

## Deliver your first event

Use [the signed Python publish/queue example](../developer/api-overview.md#copyable-signed-publish-and-queue-loop).
Publish a synthetic event addressed to the consumer; 202 means Redis accepted it.
As consumer, poll the queue, deduplicate event ID, commit processing, then ack.
An empty queue returns 200 `[]`; successful ack is 204 without a body.

## Choose how the consumer wakes up

- `queue`: poll signed HTTP; leases last 60 seconds.
- `websocket`: subscribe to event hints, and keep polling for reliable recovery.
- `callback`: set a reachable HTTPS callback URL and run the callback worker.
- `all`: combine callback eligibility with realtime observation and queue recovery.

Set a URL through signed PATCH of the consumer's app; `callback_url:null` removes
it. WebSocket subscription remains explicit in all modes. Callback receivers must
verify signatures and deduplicate events. Read [reliability](../developer/reliability.md)
before production: acceptance is not a guarantee of exactly-once processing.
