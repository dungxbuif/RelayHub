# Deploy RelayHub

The supported root `compose.yaml` runs exactly `relayhub-api`, `relayhub-worker`
and `relayhub-redis` on one project-scoped `relayhub` network. API and worker share
one Go image; the API serves application routes, `/ws`, metrics and embedded docs.
Only API publishes `${RELAYHUB_PORT:-8080}:8080`. Worker 9090 and Redis 6379 have no
published or declared exposed port. No extra proxy or documentation container is
part of the stack.

## Start and check

From the cloned repository root, copy `.env.example` to `.env`, set its mode to
`600`, and fill the three empty required values with **independent** outputs of
`openssl rand -hex 32`: `RELAYHUB_ADMIN_TOKEN`, `RELAYHUB_SIGNING_SECRET` and
`RELAYHUB_REDIS_PASSWORD`. Do not reuse sample credentials or commit `.env`.
Then run:

```bash
docker compose up --build -d --wait --wait-timeout 90
docker compose ps
curl --fail http://localhost:8080/healthz
curl --fail http://localhost:8080/readyz
curl --fail http://localhost:8080/metrics
docker compose exec -T relayhub-worker /relayhub healthcheck http://127.0.0.1:9090/readyz
```

All three services must report healthy. Open `/docs/` on the same API origin.
The root README includes a complete first signed publish/lease/ack example.
The downloadable [Compose copy](docker-compose.relayhub.yml) is byte-identical to
root Compose. To use it from a repository checkout, preserve the root context:

```bash
docker compose --project-directory . -f public-docs/deploy/docker-compose.relayhub.yml up --build -d --wait
```

The two processes use `relayhub api` and `relayhub worker`; no argument defaults
to API. Invalid commands exit nonzero. `relayhub healthcheck URL` checks a local
HTTP endpoint for exactly 200 within two seconds without loading credentials,
following redirects, printing response bodies, or requiring a shell/curl. Docker
healthchecks use Redis-backed readiness, so a Redis outage makes API and worker
unhealthy while `/healthz` remains live. The worker serves only `/healthz`,
`/readyz` and `/metrics` internally.

## Settings

Root Compose passes every application setting below except listen addresses,
which it fixes at `:8080` and `:9090` to preserve the topology and probes. For direct
binary runs, the listen settings remain configurable. Values in `.env` interpolate
only settings listed in Compose. All durations are positive Go duration strings.

| Variable | Default in root stack | Meaning |
| --- | --- | --- |
| `RELAYHUB_ADMIN_TOKEN` | required, empty example | Admin bearer secret |
| `RELAYHUB_SIGNING_SECRET` | required, empty example | Socket-token signing secret |
| `RELAYHUB_REDIS_PASSWORD` | required, empty example | Redis AUTH; URL-encoded by Go, overrides URL password |
| `RELAYHUB_REDIS_URL` | `redis://relayhub-redis:6379/0` | Shared `redis` or `rediss` URL; binary default is `redis://localhost:6379/0` |
| `RELAYHUB_REDIS_KEY_PREFIX` | `relayhub` | 1–64 ASCII letters/digits/underscore/hyphen; same for API/worker |
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
| `RELAYHUB_E2E_KEEP` | `0` | Acceptance-only: `1` retains its isolated project for diagnosis |

Separate Redis passwords safely support reserved URL characters. Generate hex
credentials for `.env` to avoid shell/Compose interpolation of punctuation. Passwords
embedded in a Redis URL remain supported for direct binary runs when the separate
password setting is absent. Changing the Redis namespace selects different data;
it is not a migration.

## Container security and persistence

The multi-stage Dockerfile builds Linux amd64/arm64 binaries with embedded docs,
contracts and Skills. The final distroless static image includes trusted CA roots
for HTTPS callbacks, uses UID/GID 65532, and contains no shell/package manager.
Redis 7 runs as UID/GID 999 with AOF and `appendfsync everysec` on the project-scoped
`relayhub-data` named volume. Every container drops all capabilities, enables
`no-new-privileges`, uses a read-only root filesystem and has a graceful stop period.
Only Redis `/data` is writable; the Go processes need no tmpfs or writable mounts.

Redis is authenticated and has no host port. Keep the project network private;
Redis AUTH over this local bridge is not encryption. For a remote Redis service,
use `rediss` with a trusted certificate and a separately managed deployment.
An API 202 confirms a Redis transaction, not a disk fsync. Every-second AOF can lose
recent writes after a host crash. Named-volume loss is not recoverable without a
backup. Back up the complete Redis data directory and credentials together.

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
which exceeds RelayHub's maximum 30-second queue wait/function deadline. Configure
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
through the durable queue. RelayHub sends Ping every 25 seconds and requires Pong
within 60 seconds. [Cloudflare limits](https://developers.cloudflare.com/fundamentals/reference/connection-limits/),
[WebSocket behavior](https://developers.cloudflare.com/network/websockets/).

Keep `/ws` query tokens out of proxy access logs. Restrict metrics and readiness at
the external proxy/firewall; they have no application authentication. See
[security](../security.md) for callback egress and log handling.

## Backup, restore and upgrade

For a consistent simple homelab backup, stop API and worker writers, then stop
Redis gracefully and copy/archive the **entire** named volume. Redis 7 AOF uses a
manifest and multiple files; copying one appendonly file is insufficient. Encrypt
backups and test a restore into a separate Compose project. Never run
`docker compose down --volumes` on a stack whose data you need to preserve.

Backup/restore helpers must run as UID/GID 999, matching Redis's private AOF files,
with `--cap-drop ALL`. Stream the archive over stdout/stdin into a host file created
with `umask 077`; root with all capabilities dropped cannot read Redis's private
files. The internal runbook includes exact streaming commands and a disposable
`./scripts/e2e.sh --backup-rehearsal` that verifies bytes and 700/600 permissions.
Restore into a stopped empty Redis-initialized volume using the same Redis image
version and UID/GID 999. Start Redis, then API/worker with the same secrets and key prefix.
Verify readiness and a signed publish/lease/ack plus callback. Receivers must retain
event-ID deduplication because a restore may replay committed side effects.

Before upgrading, make a tested backup and preserve the previous source revision
or image tag. Build and validate the candidate, then run `docker compose up -d
--build --wait`. Existing sockets reconnect; already claimed functions can time out
on API restart. Queue work survives process restarts. Roll back application code
only when it is compatible with the stored schema; otherwise restore the paired
backup and deduplicate any replay. The repository's internal operations runbook
contains volume-copy commands and the full verification gate.

## Acceptance and documentation updates

Run `./scripts/e2e.sh` from the root with Go, Python 3, Docker and Compose installed. It uses
a random project name and process-local generated credentials, temporarily enables
HTTP callbacks to its own host listener via `e2e.internal:host-gateway`, and asserts
all required runtime paths without printing payloads. Only that project's resources
are removed on completion/failure unless `RELAYHUB_E2E_KEEP=1`. The test override
adds no services and does not alter production defaults.

After public documentation edits, run `go generate ./web`. The deterministic Skill
and llms builders run before embedding. `./scripts/check-contracts.sh --self-test`
checks schemas, live API/Redis behavior, stale artifacts, root/public Compose,
container restrictions, required credentials and mandatory CI gates. Build tooling
uses Go/Python/Node; deployed docs have no separate server or Node runtime.

Host contract tests accept the explicit `RELAYHUB_DOCS_TEST_REDIS_URL` dependency.
CI supplies its Redis service URL; each run uses a random namespace and cleans only
its own keys on success or failure. A missing external URL requires local
`redis-server`; an unreachable supplied URL fails without skipping. The separate
`RELAYHUB_TEST_REDIS_URL` setting controls Go Redis integration tests.
The docs-test URL accepts an optional nonnegative decimal database path and one
`db` query override: `/0?db=1` uses DB 1 for both application state and cleanup.
Only `redis://` and `rediss://` are supported. Database numbers must fit a signed
64-bit integer; duplicate/empty/invalid `db` values, other query options, invalid
paths and fragments are rejected before the API launches. This restriction keeps
the application and cleanup database selection consistent.
Acceptance and cleanup commands use Unix process groups, TERM/KILL cancellation
and bounded inherited-pipe waits so an orphan Compose child cannot block cleanup.
