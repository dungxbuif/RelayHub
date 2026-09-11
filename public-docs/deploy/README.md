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

## API configuration and production routing

| Variable | Default | Meaning |
| --- | --- | --- |
| `RELAYHUB_HTTP_ADDR` | `:8080` | API listen address |
| `RELAYHUB_REDIS_URL` | `redis://localhost:6379/0` | Shared Redis URL; `redis` or `rediss` |
| `RELAYHUB_ADMIN_TOKEN` | required | Operator bearer token |
| `RELAYHUB_SIGNING_SECRET` | required | Server token signing secret, shared by API instances |
| `RELAYHUB_ALLOWED_ORIGINS` | empty | Comma-separated exact browser origins; no wildcard |
| `RELAYHUB_EVENT_RETENTION` | `168h` | Event lifetime from publication |
| `RELAYHUB_JOB_RETENTION` | `168h` | Terminal job lifetime |
| `RELAYHUB_IDEMPOTENCY_RETENTION` | `24h` | Publication-key lifetime; RPC keys always last 24h |
| `RELAYHUB_SIGNING_SKEW` | `5m` | Permitted request timestamp skew |

Durations must be positive Go duration strings. The Compose example passes worker
settings and basic credentials; to override other settings, add them to its shared
`environment` mapping or a Compose override. Merely exporting an unlisted variable
does not pass it into a container. Configure exact browser Origins for your app.

Route the production hostname through a TLS proxy to the API, preserving escaped
paths, query order and WebSocket upgrades. Set proxy read timeouts beyond queue
wait/RPC deadlines and allow long-lived sockets. Restrict unauthenticated operations
routes and keep `/ws` query tokens out of access logs. See [security](../security.md).

## Backup and restore

Back up Redis according to its persistence policy and protect snapshots as secrets.
To restore, stop API/worker writers, restore a tested Redis snapshot/volume, start
Redis, then API/worker with the same namespace and server secret configuration.
Check readiness, publish a synthetic event, consume and ack it, then verify a
callback. Restored jobs can be delivered again; receiver event-ID deduplication
must survive restores too. Changing the key prefix selects a different namespace;
it does not migrate data.

## Rebuild documentation

After modifying public Markdown or contracts, run `go generate ./web`. This runs
the deterministic Skill and llms builders before embedding assets. Docker build
checks source parity and contracts before compiling; it fails on stale committed
artifacts. Build tooling uses Go and Python validators, while the deployed docs
have no Node or separate server dependency. Read the [API reference](../api.md)
and [Skills page](../skills.md) for stable resource URLs.
