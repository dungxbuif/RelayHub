# Deploy RelayHub

RelayHub runs one API and one callback worker from the same image, backed by Redis. The API serves application/event/queue endpoints, RFC 6455 WebSocket at `/ws`, metrics and embedded `/docs`. The worker sends signed callbacks and manages retries/dead-letter state. No separate documentation server is required.

## Start the stack

From the repository root:

```bash
export RELAYHUB_ADMIN_TOKEN='replace-with-a-long-random-admin-token'
export RELAYHUB_SIGNING_SECRET='replace-with-a-separate-long-random-secret'
go generate ./web
go test ./web
docker compose -f public-docs/deploy/docker-compose.relayhub.yml up --build -d
```

Compose starts API, worker and Redis with AOF and a persistent named volume. Only the API publishes a host port (8080 by default; override `RELAYHUB_PORT`). Service names are local deployment choices; configure callbacks with a URL reachable from the worker container. Both Go processes run as a non-root user. The worker needs outbound access to callback destinations. Scale it with `docker compose -f public-docs/deploy/docker-compose.relayhub.yml up -d --scale relayhub-worker=2`.

The same binary supports `relayhub api` and `relayhub worker`. No command defaults to `api`; unknown commands exit nonzero and print usage. Run these in separate terminals for local development with `RELAYHUB_REDIS_URL` set to your Redis URL. Both commands currently load the same required admin/server secret configuration.

## Worker configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `RELAYHUB_WORKER_HTTP_ADDR` | `:9090` | Private worker health/readiness/metrics listener; no externally published port |
| `RELAYHUB_REDIS_KEY_PREFIX` | `relayhub` | Shared Redis namespace, 1–64 letters/digits/underscore/hyphen |
| `RELAYHUB_CALLBACK_TIMEOUT` | `10s` | Timeout per outbound attempt, including response drain |
| `RELAYHUB_WORKER_CONCURRENCY` | `8` | Active attempt slots per worker, 1–1024 |
| `RELAYHUB_WORKER_RECLAIM_IDLE` | `30s` | Abandoned-message/lease interval; at least callback timeout + 5 seconds |
| `RELAYHUB_SHUTDOWN_TIMEOUT` | `10s` | Grace period for active work after SIGTERM |
| `RELAYHUB_ALLOW_INSECURE_CALLBACKS` | `false` | Permit HTTP callbacks for controlled local development |

Use HTTPS callbacks in production. Set Compose `stop_grace_period` longer than the configured shutdown timeout to allow process cleanup. API and worker must use the same Redis namespace; separate prefixes isolate deployments. See [reliability](../developer/reliability.md) for signatures, exact retry delays, Retry-After, retention and requeue operations.

## Check and operate

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
curl http://localhost:8080/metrics
curl http://localhost:8080/docs/llms.txt
docker compose -f public-docs/deploy/docker-compose.relayhub.yml logs relayhub-worker
```

A callback target that first returns `503` and then `204` should reach `delivered` with `attempts=2` after about one second. A `400` should reach `dead_letter` with `attempts=1`. Inspect job status with the authenticated API and use admin requeue after repairing the receiver. Receivers must deduplicate event IDs because a crash after receiver commit can cause another request.

An external reverse proxy can route the entire origin to the API, including WebSocket upgrades. Redis stays private to the Compose network. Preserve and back up its volume; API acceptance confirms a Redis transaction, not a guaranteed fsync. Embedded docs include Markdown and `llms.txt` for agents; regenerate and run the parity test after editing public documentation.

Worker metrics are available inside the Compose network at `http://relayhub-worker:9090/metrics`, including `relayhub_callback_outcomes_total{outcome="delivered|pending|dead_letter|store_error"}` and notification failure totals. This address is an example using the local service name. Worker `GET /healthz` checks the process and `GET /readyz` checks Redis. The worker listener serves only those three operations routes; Compose neither publishes nor exposes its port externally. Both HTTP operations and active callbacks stop gracefully on SIGTERM.
