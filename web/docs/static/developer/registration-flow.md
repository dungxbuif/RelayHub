# Register an application and connect

1. An administrator calls `POST /api/v1/apps` using a username/password admin session and its CSRF token and a body such as `{"name":"orders","delivery_mode":"queue"}`.
2. Save the one-time response fields `app_id`, `api_key`, and `hmac_secret` in your backend secret store. Read the exact [application response](./api-overview.md). Credentials are never returned by later reads.
3. Sign `POST /api/v1/socket/token` as that app using the [HMAC signing rules](./auth.md). Request `{"scopes":["ws:connect"],"ttl_seconds":600}`.
4. Pass the returned short-lived `token` to a standard RFC 6455 client at `/ws?token=<encoded-token>`. Browser clients receive only this token from their backend.
5. Wait for `ready`, then send `{"type":"subscribe","topics":["events","jobs"]}`. Receive `subscribed`, `event`, and `job.updated` frames using the [WebSocket examples](./websocket.md). Browser Origin must match the operator's allowlist. Socket.IO is unsupported.
6. A producer signs `POST /api/v1/events` with an `Idempotency-Key`, an event `type`, `target_app_ids`, and object `data`.
7. Your backend receives durable work through signed callbacks or the standard `/api/v1/stream` WebSocket protocol and deduplicates by event ID before committing side effects.
8. After reconnect, mint a new token, re-subscribe, and resume durable stream/callback recovery. Realtime channel messages are online-only hints. See [reliability](./reliability.md).

The verified token supplies application identity; client frames cannot change it. Only explicitly addressed targets receive notifications. Deduplicate committed side effects by event `id`; repeated delivery is expected. HTTP callback workers are described in [reliability](./reliability.md). For remote functions, the owner signs `POST /api/v1/functions`, subscribes to `functions` on its standard WebSocket, and responds to `rpc.invoke` on the selected connection. A signed caller invokes the shared function ID with an `Idempotency-Key` and object input. Follow the [function registration, handler and caller examples](./functions.md). RPC requires an online owner; reconnect re-subscribes for new calls and does not replay previously dispatched invocations.
