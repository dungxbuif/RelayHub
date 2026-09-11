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

## Root Compose does not start

Generate all three required secrets in a private `.env`: admin token, server signing
secret and Redis password. `docker compose config --quiet` validates without
printing secrets. Use an unused `RELAYHUB_PORT`; only API publishes a host port.
If using the downloadable Compose file, run it with `--project-directory .` from
the repository root. Docker build rejects stale docs: run `go generate ./web`, then
`./scripts/check-contracts.sh --self-test` and rebuild.

## Containers are alive but unhealthy

Readiness checks Redis; liveness does not. Check Redis health, password and the
shared namespace/URL, then inspect API/worker logs. The distroless image has no
shell or curl: use `docker compose exec -T relayhub-worker /relayhub healthcheck
http://127.0.0.1:9090/readyz`. A failed probe prints no response body. Docker does
not restart a merely unhealthy process; repair the dependency and check recovery.
After changing `.env`, use `docker compose up -d --wait` to recreate configuration.

## WebSocket or function requests fail through a proxy

Use an RFC 6455 client, an allowed browser Origin and a fresh token. External
Traefik preserves upgrades; remove query/path rewriting, buffering and caching.
Bypass Cloudflare cache for `/api/*` and `/ws`; function replay is API behavior,
never a CDN cache hit. Keep proxy response timeouts above the registered maximum
30-second function deadline and queue wait (the example uses 40 seconds).
`503 function_unavailable` means no online owner claimed the call; `504
function_timeout` means a claimed call expired. An offline function is not queued.
Reconnect after edge/API restarts and use the same invocation key to inspect the
persisted outcome without redispatch. See [deployment](deploy/README.md) for TLS,
Cloudflare limits, private worker operations and restore guidance.

## Acceptance reports a failure

`./scripts/e2e.sh` prints the failing stage while withholding Docker output,
credentials and payloads. It cleans only its unique project. Retry with
`RELAYHUB_E2E_KEEP=1` to preserve that project's containers for local diagnosis;
handle Docker inspection output as sensitive. Ensure Docker can reach registries,
Go modules and the temporary host callback listener through `host-gateway`.
A host firewall blocking the callback listener can fail only the callback stage.
