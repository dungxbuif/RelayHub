# Register an application and connect

1. An administrator calls `POST /api/v1/apps` with `Authorization: Bearer <admin-token>` and a body such as `{"name":"orders","delivery_mode":"queue"}`.
2. Save the one-time response fields `app_id`, `api_key`, and `hmac_secret` in your backend secret store. Read the exact [application response](./api-overview.md). Credentials are never returned by later reads.
3. Sign `POST /api/v1/socket/token` as that app using the [HMAC signing rules](./auth.md). Request `{"scopes":["ws:connect"],"ttl_seconds":600}`.
4. Pass the returned short-lived `token` to a standard RFC 6455 client at `/ws?token=<encoded-token>`. Browser clients receive only this token from their backend.
5. Wait for `ready`, then send `{"type":"subscribe","topics":["events","jobs"]}`. Receive `subscribed`, `event`, and `job.updated` frames using the [WebSocket examples](./websocket.md). Browser Origin must match the operator's allowlist. Socket.IO is unsupported.
6. A producer signs `POST /api/v1/events` with an `Idempotency-Key`, an event `type`, `target_app_ids`, and object `data`.
7. Your backend polls the signed target queue, processes each leased event idempotently, then signs `POST /api/v1/events/{eventID}/ack`. WebSocket notifications wake consumers but do not lease or acknowledge work.
8. After reconnect, mint a new token, re-subscribe, and drain the durable queue. Continue polling so missed best-effort notifications cannot strand work. See [reliability](./reliability.md).

The verified token supplies application identity; client frames cannot change it. Only explicitly addressed targets receive notifications. Deduplicate committed side effects by event `id`; repeated delivery is expected. Function subscriptions/results and callback delivery workers are reserved for later tasks.
