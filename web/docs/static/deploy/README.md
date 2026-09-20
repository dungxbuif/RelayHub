# Deploy RelayHub

The supported root `compose.yaml` runs `relayhub-api`, `relayhub-worker`,
`relayhub-postgres`, `relayhub-nats` and `relayhub-redis` on one project-scoped `relayhub` network. API and worker share
one Go image; the API serves application routes, `/ws`, metrics and embedded Admin assets.
Only API publishes `${RELAYHUB_PORT:-8080}:8080`. Worker 9090, PostgreSQL 5432,
NATS 4222/8222 and Redis 6379 have no published or declared exposed port. No extra proxy or documentation container is
part of the local stack; Docusaurus is built and deployed separately.

## Start and check

From the cloned repository root, copy `.env.example` to `.env`, set its mode to
`600`, set `RELAYHUB_NATS_USERNAME=relayhub`, and fill the empty secret values
with **independent** outputs: `RELAYHUB_ADMIN_TOKEN`,
`RELAYHUB_SIGNING_SECRET`, `RELAYHUB_POSTGRES_PASSWORD`,
`RELAYHUB_SECRET_ENCRYPTION_KEY`, `RELAYHUB_NATS_PASSWORD` and
`RELAYHUB_REDIS_PASSWORD`. Use
`openssl rand -base64 32` for the encryption key and `openssl rand -hex 32` for
the other secrets. Do not reuse credentials or commit `.env`.
Then run:

```bash
docker compose up --build -d --wait --wait-timeout 90
docker compose ps
curl --fail http://localhost:8080/healthz
curl --fail http://localhost:8080/readyz
curl --fail http://localhost:8080/metrics
docker compose exec -T relayhub-worker /relayhub healthcheck http://127.0.0.1:9090/readyz
```

All five services must report healthy. Open `/admin/` on the API origin. `/docs/`
is available only after the operator deploys Docusaurus and configures routing.
The root README includes a complete first signed routed publish example.
The downloadable [Compose copy](docker-compose.relayhub.yml) is byte-identical to
root Compose. To use it from a repository checkout, preserve the root context:

```bash
docker compose --project-directory . -f web/docs/static/deploy/docker-compose.relayhub.yml up --build -d --wait
```

The two processes use `relayhub api` and `relayhub worker`; no argument defaults
to API. Invalid commands exit nonzero. `relayhub healthcheck URL` checks a local
HTTP endpoint for exactly 200 within two seconds without loading credentials,
following redirects, printing response bodies, or requiring a shell/curl. Docker
healthchecks require PostgreSQL, NATS/JetStream and Redis, so any outage makes
API and worker unhealthy while `/healthz` remains live. The worker serves only `/healthz`,
`/readyz` and `/metrics` internally.

## Metrics

The API exposes `relayhub_http_requests_total` and
`relayhub_http_request_duration_seconds` with bounded `method`, registered `route`
template and HTTP `status` labels. Unknown methods use `OTHER`; unmatched routes
use `unmatched`. Counts and duration are recorded when handlers finish; WebSocket
duration includes the session lifetime. Metrics never label raw paths, queries,
app/event/job IDs, credentials, event types or payloads.

`relayhub_event_outcomes_total{outcome="published|replayed|rejected|store_error"}`
counts completed publication calls. A durable new publication increments `published` once;
each replay increments `replayed` and does not increment `published`. Authenticated
invalid JSON/input increments `rejected`; unexpected internal/storage failures
increment `store_error`. Authentication/body-limit failures are HTTP outcomes and
do not reach event publication. Existing callback, notification, WebSocket and
function metrics remain available. Counters reset when the process restarts.
An unwound handler panic records an HTTP 500 and does not count as a completed
publication or a new durable acceptance.

NATS connectivity is `relayhub_nats_connected`. The
`relayhub_nats_events_total` counter uses only `disconnected`, `reconnected`,
`slow_consumer`, `async_error`, `drained` and `bootstrap_error`. See
[the NATS guide](nats.md) for the stream and readiness contract.

## Settings

The v1 PostgreSQL control store settings and key-generation procedure are in the
[PostgreSQL guide](postgresql.md).

Root Compose passes every application setting below except listen addresses,
which it fixes at `:8080` and `:9090` to preserve the topology and probes. For direct
binary runs, the listen settings remain configurable. Values in `.env` interpolate
only settings listed in Compose. All durations are positive Go duration strings.

| Variable | Default in root stack | Meaning |
| --- | --- | --- |
| `RELAYHUB_ADMIN_TOKEN` | required, empty example | Admin bearer secret |
| `RELAYHUB_SIGNING_SECRET` | required, empty example | Socket-token signing secret |
| `RELAYHUB_POSTGRES_PASSWORD` | required, empty example | PostgreSQL password used by the private Compose database |
| `RELAYHUB_POSTGRES_URL` | Compose generated | PostgreSQL connection string for API and worker |
| `RELAYHUB_SECRET_ENCRYPTION_KEY` | required, empty example | Base64 key used to encrypt stored app credentials |
| `RELAYHUB_NATS_URL` | `nats://relayhub-nats:4222` | Private `nats` or `tls` URL without embedded credentials; binary default is localhost |
| `RELAYHUB_NATS_USERNAME` | required, empty example | Dedicated internal RelayHub NATS user |
| `RELAYHUB_NATS_PASSWORD` | required, empty example | Dedicated internal RelayHub NATS password |
| `RELAYHUB_NATS_CONNECT_TIMEOUT` | `2s` | Initial connection deadline |
| `RELAYHUB_NATS_RECONNECT_WAIT` | `2s` | Base reconnect delay; the client adds jitter |
| `RELAYHUB_NATS_MAX_RECONNECTS` | `-1` | Reconnect attempts; `-1` retries indefinitely |
| `RELAYHUB_NATS_DRAIN_TIMEOUT` | `10s` | Graceful NATS client drain deadline |
| `RELAYHUB_NATS_STREAM_MAX_AGE` | `168h` | Managed JetStream message maximum age |
| `RELAYHUB_NATS_DUPLICATE_WINDOW` | `24h` | Managed message-ID duplicate window |
| `RELAYHUB_NATS_REPLICAS` | `1` | Managed stream replicas; valid values are 1, 3 or 5 |
| `RELAYHUB_REDIS_MODE` | `standalone` | `standalone`, `sentinel` or `cluster`; root Compose fixes standalone |
| `RELAYHUB_REDIS_ADDRS` | `relayhub-redis:6379` | Comma-separated `host:port` seeds without scheme, userinfo, path or query |
| `RELAYHUB_REDIS_USERNAME` | empty | Optional Redis ACL username, separate from addresses |
| `RELAYHUB_REDIS_PASSWORD` | required, empty example | Redis password generated independently with `openssl rand -hex 32` |
| `RELAYHUB_REDIS_SENTINEL_MASTER` | empty | Required master name only in Sentinel mode |
| `RELAYHUB_REDIS_DB` | `0` | Database 0–15 for standalone/Sentinel; Cluster requires zero |
| `RELAYHUB_REDIS_TLS` | `false` | Enable Redis TLS 1.3 or newer |
| `RELAYHUB_REDIS_KEY_PREFIX` | `rh` | Validated lowercase key namespace, 1–32 characters |
| `RELAYHUB_REDIS_CONNECT_TIMEOUT` | `2s` | Initial bounded connection and ping deadline |
| `RELAYHUB_REDIS_READ_TIMEOUT` | `1s` | Per-command read timeout |
| `RELAYHUB_REDIS_WRITE_TIMEOUT` | `1s` | Per-command write timeout |
| `RELAYHUB_REDIS_POOL_SIZE` | `32` | Per-replica pool bound, 1–4096 |
| `RELAYHUB_OBJECT_STORAGE_ENDPOINT` | empty | Optional HTTPS S3-compatible endpoint; empty disables file messaging |
| `RELAYHUB_OBJECT_STORAGE_BUCKET` | empty | Private bucket used only for RelayHub file objects |
| `RELAYHUB_OBJECT_STORAGE_REGION` | empty | Provider region when required |
| `RELAYHUB_OBJECT_STORAGE_ACCESS_KEY` | empty | Least-privilege object access key, server-side only |
| `RELAYHUB_OBJECT_STORAGE_SECRET_KEY` | empty | Secret key, server-side only |
| `RELAYHUB_INSTANCE_ID` | generated | Optional stable replica name; generation still changes every start |
| `RELAYHUB_PORT` | `8080` | Compose host publication; use `127.0.0.1:8080` for a host-local proxy |
| `RELAYHUB_HTTP_ADDR` | fixed `:8080` | API bind address for direct binary runs |
| `RELAYHUB_WORKER_HTTP_ADDR` | fixed `:9090` | Worker operations bind address for direct binary runs |
| `RELAYHUB_ALLOWED_ORIGINS` | empty | Exact comma-separated browser origins; wildcard is rejected |
| `RELAYHUB_EVENT_RETENTION` | `168h` | Event lifetime from publication |
| `RELAYHUB_JOB_RETENTION` | `168h` | Terminal job lifetime |
| `RELAYHUB_IDEMPOTENCY_RETENTION` | `24h` | Event replay lifetime; function replay always lasts 24h |
| `RELAYHUB_SIGNING_SKEW` | `5m` | Allowed timestamp skew |
| `RELAYHUB_SHUTDOWN_TIMEOUT` | `10s` | API/active callback graceful shutdown deadline |
| `RELAYHUB_STOP_GRACE_PERIOD` | `20s` | Compose API/worker stop budget; keep greater than shutdown timeout |
| `RELAYHUB_CALLBACK_TIMEOUT` | `10s` | Complete outbound callback deadline |
| `RELAYHUB_WORKER_CONCURRENCY` | `8` | 1–1024 active callback slots |
| `RELAYHUB_WORKER_RECLAIM_IDLE` | `30s` | Must exceed callback timeout by at least five seconds |
| `RELAYHUB_ALLOW_INSECURE_CALLBACKS` | `false` | Local-development HTTP exception; production uses HTTPS |

Separate PostgreSQL passwords safely support reserved URL characters. Generate hex
credentials for `.env` to avoid shell/Compose interpolation of punctuation. Passwords
embedded in a PostgreSQL URL remain supported for direct binary runs when the separate
password setting is absent. Changing the PostgreSQL namespace selects different data;
it is not a migration.

## Container security and persistence

The multi-stage Dockerfile uses Go 1.27.1 and a default-deny `.dockerignore` to build Linux amd64/arm64 binaries with embedded Admin assets and contract-check inputs,
contracts and Skills. The final distroless static image includes trusted CA roots
for HTTPS callbacks, uses UID/GID 65532, and contains no shell/package manager.
The API and worker containers drop all capabilities, enable `no-new-privileges`,
use a read-only root filesystem and need no writable mounts. PostgreSQL and NATS
use the official image defaults so their entrypoints can initialize runtime
directories and named-volume permissions. PostgreSQL is pinned to `postgres:17-alpine`
for the `/var/lib/postgresql/data` volume layout. Every service has a graceful
stop period.

NATS uses file-backed JetStream on the `relayhub-nats-data` volume. Its client and
monitoring listeners stay private to the project network, and its account limits
the runtime to RelayHub subjects, JetStream control requests and reply inboxes.

Redis is password-authenticated, private and intentionally has no volume. It holds
only reconstructible TTL state such as sessions, rate buckets and replica
ownership. A Redis outage makes readiness fail and blocks new Redis-dependent
admission while already accepted PostgreSQL/NATS work remains recoverable.
Standalone is the local default; Sentinel supplies non-sharded failover and
Cluster supplies sharding with database zero. Production deployments use private
TLS endpoints and least-privilege ACL credentials with a unique key prefix per
environment. Rotate passwords by staging the new ACL credential, recreating all
API/worker replicas, verifying readiness, and only then revoking the old one.
Credentials never belong in `RELAYHUB_REDIS_ADDRS`.

API replicas require no request affinity. A load balancer may send each HTTP
request to any healthy replica; WebSocket reconnects may land on another replica
because session, quota and ownership coordination is shared through Redis.

PostgreSQL is authenticated and has no host port. Keep the project network private;
PostgreSQL AUTH over this local bridge is not encryption. For a remote PostgreSQL service,
use TLS-capable connectivity or a private trusted network according to your database provider.
An API 202 confirms a committed PostgreSQL transaction and outbox record. Named-volume
loss is not recoverable without a backup. Back up PostgreSQL, NATS JetStream and
credentials together.

## External Traefik and Cloudflare

[The file-provider example](traefik/labels.yml) routes
`relayhub.dungxbuif.com` to API 8080 only. It is external infrastructure: set the
backend URL to the RelayHub host address reachable from your Traefik installation.
When Traefik runs on that host directly, bind RelayHub to `127.0.0.1:8080` and use
that address. A Traefik container's own loopback is not the host; use a reachable
private host address and restrict host-port access to the proxy.

Traefik preserves WebSocket Upgrade/Connection headers automatically; do not
rewrite canonical signed paths, escaped paths or query order. The example sets
`Cache-Control: no-store` at the proxy and a 40-second response-header timeout,
which exceeds RelayHub's maximum stream wait and 30-second function deadline. Configure
entrypoint write timeouts to at least 40 seconds and allow long-lived upgraded
connections. Avoid buffering/caching middleware on `/ws` and `/api/*`.
[Traefik WebSocket documentation](https://doc.traefik.io/traefik/v3.4/user-guides/websocket/).

Cloudflare may supply DNS, TLS, WAF and static-document CDN caching. Use Full
(strict) TLS to a valid origin certificate. Configure a Cache Rule to **Bypass
cache** for `/api/*`, `/ws`, `/healthz`, `/readyz` and `/metrics`, overriding any
Cache Everything rules. Function requests and responses are never cacheable.
Only versioned/static docs may be cached deliberately; purge them after an upgrade.
Cloudflare currently documents a 125-second proxy read timeout, but RelayHub calls
finish within 30 seconds. Raising that limit cannot make an offline function
available. WebSocket connections can close during edge restarts or idle periods;
clients must answer protocol pings, reconnect with a fresh token and recover events
through durable stream delivery or callbacks. RelayHub sends Ping every 25 seconds and requires Pong
within 60 seconds. [Cloudflare limits](https://developers.cloudflare.com/fundamentals/reference/connection-limits/),
[WebSocket behavior](https://developers.cloudflare.com/network/websockets/).

Keep `/ws` query tokens out of proxy access logs. Restrict metrics and readiness at
the external proxy/firewall; they have no application authentication. See
[security](../security.md) for callback egress and log handling.

## Backup, restore and upgrade

For a consistent simple homelab backup, stop API and worker writers, then stop
PostgreSQL gracefully and copy/archive the **entire** PostgreSQL and NATS named
volumes together. Encrypt backups and test a restore into a separate Compose project.
Never run `docker compose down --volumes` on a stack whose data you need to preserve.

Restore into stopped empty PostgreSQL and NATS volumes using matching service versions.
Start PostgreSQL and NATS, then API/worker with the same secrets. Verify readiness
and a fresh signed routed publish through stream delivery or callback. Receivers
must retain event-ID deduplication because a restore may replay committed side effects.

Before upgrading, make a tested backup and preserve the previous source revision
or image tag. Build and validate the candidate, then run `docker compose up -d
--build --wait`. Existing sockets reconnect; already claimed functions can time out
on API restart. Durable delivery state survives process restarts. Roll back application code
only when it is compatible with the stored schema; otherwise restore the paired
backup and deduplicate any replay. The repository's internal operations runbook
contains volume-copy commands and the full verification gate.

## Acceptance and documentation updates

For this v1 line, use the Go and docs checks as the source of truth. Run the default
suite, the PostgreSQL/NATS integration suite, the deterministic docs builders and
the static docs checker:

```bash
go -C backend test ./...
go test -tags=integration ./... -count=1 -timeout=180s
./backend/scripts/build-skill.sh
./backend/scripts/build-llms.sh
python3 scripts/check-docs.py --static
go -C backend generate ./web
```

After public documentation edits, run `go -C backend generate ./web`. The deterministic Skill
and llms builders run before embedding. `./backend/scripts/check-contracts.sh --self-test`
checks schemas, stale artifacts, root/public Compose, container restrictions and
negative controls. Build tooling uses Go/Python/Node; deployed docs have no separate
server or Node runtime.
Documentation contributors should use inline Markdown links. The checker rejects
reference-style links with a clear diagnostic and also checks links/images written
as embedded HTML. Desktop/mobile rendering, keyboard navigation and copy controls
are exercised in a real browser during release verification.
