# Troubleshooting

Check `/healthz` first, then `/readyz`. Health is process-only; readiness needs
Redis. Inspect redacted status/error codes and metrics. Never paste credentials,
signatures, full socket URLs or private event payloads into diagnostic output.

| Symptom | Next action |
| --- | --- |
| 401 `unauthorized` | Confirm app is enabled and key is current; synchronize clock; sign exact transmitted bytes and escaped query with the whole secret. |
| 403 on app route | Use the authenticated app's ID. Admin bearer does not replace app HMAC on signed routes. |
| 403 before WebSocket upgrade | Ensure token includes `ws:connect` and browser Origin exactly matches configured allowlist. |
| 400 `invalid_request` | Check schema, unknown fields, object data/input, limits and required idempotency key. |
| 413 `request_too_large` | Reduce full HTTP body below 1 MiB. RPC must also fit its 64 KiB wire frame. |
| 404 for event/job | Record expired, missing, or caller is not the source/relevant target. |
| 409 on requeue/ack | Read current job state and allowed transitions; requeue permits leased or dead-letter work. |
| Queue is empty | Confirm target identity, active competing lease, ack state and event retention. Empty result is a normal 200 `[]`. |
| Callback stuck pending | Confirm worker readiness, target URL/egress, current app config and retry_at. |
| Callback dead letter | Repair permanent failure or exhausted retry cause, then have an operator requeue. |
| Missing WebSocket event | Wait for ready and subscribe; reconnect/re-subscribe and poll durable queue. Socket.IO cannot connect. |
| 503 `function_unavailable` | Start an owner socket subscribed to functions before invoking; use a new key only for an intentional new attempt. |
| 504 `function_timeout` | Handler must return on the selected connection before deadline; a replay key keeps the same terminal result. |
| HTTP 200 function `ok:false` | Handle the handler's application error code/message; this is not an HTTP transport failure. |
| Docs changes absent | Run canonical build scripts and `go generate ./web`, then rebuild/deploy binary. |

Read [exact signing](developer/auth.md), [reliability](developer/reliability.md),
[WebSocket limits](developer/websocket.md), [RPC lifecycle](developer/functions.md)
and [deployment](deploy/README.md) before retrying blindly.
