# Getting started

You need a running five-service RelayHub stack (API, worker, PostgreSQL, NATS and Redis) and two applications: a producer and a
consumer. Use [deployment instructions](../deploy/README.md) for the API/worker
stack. Set a strong `RELAYHUB_SIGNING_SECRET`, then seed the first Admin account
with the one-shot `relayhub admin seed-user` command from the deployment guide.
Check `/healthz` and `/readyz` first.

## Provision applications

An operator calls admin-authenticated `POST /api/v1/apps` with
`{"name":"orders","delivery_mode":"queue"}`. Repeat for the consumer. The response
is 201 with `app_id`, `api_key`, and `hmac_secret`, returned once. Store them in an
approved secret manager; do not paste them into chat, browser code, logs or source.
Use [the registration flow](../developer/registration-flow.md) for exact requests,
rotation and disable behavior. Operators list apps using admin `GET /api/v1/apps`.

## Deliver your first event

Use [the signed Python publish example](../developer/api-overview.md#copyable-signed-publish).
Publish a synthetic event addressed to the consumer; 202 means PostgreSQL accepted it and the outbox can recover broker delivery. As consumer, use callbacks or `/api/v1/stream`, deduplicate event ID, then commit processing.

## Choose how the consumer wakes up

- `websocket`: subscribe to event hints or realtime channels; use callbacks/stream for reliable recovery.
- `callback`: set a reachable HTTPS callback URL and run the callback worker.
- `all`: combine callback eligibility with realtime observation and stream recovery.

Set a URL through signed PATCH of the consumer's app; `callback_url:null` removes
it. WebSocket subscription remains explicit in all modes. Callback receivers must
verify signatures and deduplicate events. Read [reliability](../developer/reliability.md)
before production: acceptance is not a guarantee of exactly-once processing.
