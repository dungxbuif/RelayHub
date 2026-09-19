# RelayHub operations runbook

The supported deployment is root `compose.yaml`; its public downloadable copy is
byte-identical. All commands below run from the checkout used to start the stack.
Keep the same Compose project name for normal upgrades. Use `-p` explicitly when
operating more than one instance; networks and `relayhub-data` volumes are scoped
to that name. Do not use global Docker prune commands for RelayHub maintenance.
The API and worker containers run as non-root with a read-only filesystem.
PostgreSQL, NATS and Redis use the official image entrypoints and default runtime user
transitions so they can initialize named volume permissions and runtime sockets.
PostgreSQL is pinned to the 17 Alpine image while RelayHub uses the
`/var/lib/postgresql/data` volume layout.

## Health and diagnosis

```bash
docker compose ps
curl --fail http://localhost:8080/healthz
curl --fail http://localhost:8080/readyz
curl --fail http://localhost:8080/metrics
docker compose exec -T relayhub-worker /relayhub healthcheck http://127.0.0.1:9090/healthz
docker compose exec -T relayhub-worker /relayhub healthcheck http://127.0.0.1:9090/readyz
docker compose exec -T relayhub-worker /relayhub healthcheck http://127.0.0.1:9090/metrics
docker compose logs --since 10m relayhub-api relayhub-worker
```

`healthz` is process liveness; `readyz` requires PostgreSQL, NATS/JetStream and
Redis. A dependency failure makes API and worker Docker health unhealthy.
Docker restart policies restart exited processes, not merely unhealthy containers.
Restore connectivity/authentication, then
check readiness recovery. Scrape worker metrics from a trusted client already on
the project network at `http://relayhub-worker:9090/metrics`; no host port is opened.
The probe returns status only and deliberately suppresses bodies/URLs.

## Admin browser sessions

The React Admin is embedded at `/admin/`; extensionless `/admin/*` paths must be
routed to the same API service so client-side deep links can load. Do not route
public `/docs/*` into this binary: the Docusaurus site is an independently built
artifact and may be hosted behind the same external domain later.

An operator enters `RELAYHUB_ADMIN_TOKEN` once. RelayHub exchanges it for the
`__Host-relayhub_admin` cookie (`Secure`, `HttpOnly`, `SameSite=Strict`, `Path=/`,
no `Domain`) and a session-bound CSRF value held only in page memory. Sessions
expire after 30 minutes idle or 12 hours absolute time, whichever occurs first.
Logging out deletes the shared Redis session, so a captured old cookie is rejected
by every replica. Reloading intentionally requires re-entering the bootstrap token
because neither that credential nor the CSRF value is persisted in browser storage.

All cookie-authenticated mutations require `X-RelayHub-CSRF`. A `401` returns the
UI to sign-in; a `403` indicates missing or invalid CSRF. Rotate the bootstrap
token through the deployment secret mechanism. Existing browser sessions are
independently revocable Redis records and should also be logged out or purged
during an emergency rotation.

## Admin operational reads

Overview refreshes every five seconds and can be paused by the operator. Rolling
counters are fixed-cardinality Redis buckets retained for 25 hours. Supported
window/step pairs are `5m/1m`, `15m/1m`, `1h/1m`, `1h/5m`, `6h/5m`, `6h/15m`,
`24h/15m` and `24h/1h`. Request/status/event/NATS series and API-instance
heartbeats are reconstructible Redis state. Pending, retrying, dead-letter,
oldest-pending and completed-delivery latency percentiles come from PostgreSQL.

If Redis is unavailable, `/api/v1/admin/dashboard` continues returning durable
PostgreSQL truth and identifies `rolling_metrics` and/or `instances` in
`degraded_components`; do not interpret an empty degraded chart as a durable data
loss. A PostgreSQL failure makes Admin durable reads unavailable and must be
treated as an operational incident.

Events, Dead Letters and Audit Logs use stable descending keyset pagination.
`next_cursor` is opaque, versioned and bound to the active filters; clients must
not decode, edit or reuse it with different filters. Page limits are 1–100
(default 25). List responses exclude event payloads, callback URLs, credentials
and headers. The DLQ screen in this release is read-only; replay controls arrive
with the separately tested DLQ lifecycle module.

HTTP JSON logs contain a server-generated `request_id`, method, matched route
**template**, status, latency and bounded outcome. They never use the caller's
request-ID header as the logged ID. Unknown methods/paths have bounded labels.
WebSocket access logging happens when the upgraded connection finishes. Function
and callback operations log bounded outcomes. No headers, raw query/target,
callback URL, signature, request/response body, event data or function input/result
is logged. External proxy logging must apply equivalent redaction separately.
Metrics labels remain bounded; never add app/event IDs or payloads to metric labels.

NATS health is `relayhub_nats_connected`. Connection lifecycle and client faults
increment `relayhub_nats_events_total` with one of six fixed labels:
`disconnected`, `reconnected`, `slow_consumer`, `async_error`, `drained` or
`bootstrap_error`. A bootstrap error usually means JetStream is unavailable or an
existing `RH_DELIVERIES`, `RH_CALLBACKS` or `RH_DLQ` stream differs from managed
settings. Readiness repeats this exact validation, so later deletion or drift
changes `/readyz` to 503. RelayHub deliberately does not mutate that drift. Inspect and back up
`relayhub-nats-data`, correct the configuration deliberately, then restart. Do not
delete a stream merely to clear readiness.

```bash
docker compose logs --since 10m relayhub-nats relayhub-api relayhub-worker
docker compose exec -T relayhub-nats wget -q -O - \
  'http://127.0.0.1:8222/healthz?js-enabled-only=true'
```

API HTTP count/latency metrics are `relayhub_http_requests_total` and
`relayhub_http_request_duration_seconds`, labeled by normalized method, registered
route template and status. They record completed handlers, including stream waits
and WebSocket lifetime. `relayhub_event_outcomes_total` separates `published`,
`replayed`, `rejected` and `store_error`: replay never increments new-publication
counts. Invalid authenticated publication input is rejected; auth/body-limit
failures count only as HTTP requests. See the public deployment reference for
label values. Unexpected PostgreSQL/NATS failures during callback loading or acknowledgement
propagate to the worker's `store_error`; missing, mismatched and expired claims remain conflicts.

## Configuration changes

Keep `.env` mode 600 and store it securely outside source control. All settings and
defaults are listed in [the public deployment guide](../../web/docs/static/deploy/README.md).
Changing `.env` requires `docker compose up -d --wait` to recreate affected services;
`docker compose restart` alone does not load changed environment. Keep
`RELAYHUB_STOP_GRACE_PERIOD` greater than `RELAYHUB_SHUTDOWN_TIMEOUT`. Database
and broker services should have enough stop time to flush their own state. API/worker share namespace and secrets.
Changing the namespace selects another dataset and never migrates records.
NATS credentials stay separate from `RELAYHUB_NATS_URL`; URL userinfo is rejected.
Redis credentials likewise stay in `RELAYHUB_REDIS_USERNAME` and
`RELAYHUB_REDIS_PASSWORD`; `RELAYHUB_REDIS_ADDRS` accepts only comma-separated
`host:port` values. Local Compose uses password-authenticated standalone Redis,
database zero, no published port and no persistent volume. Redis is ephemeral:
PostgreSQL and NATS remain the recovery sources. Generate its password with
`openssl rand -hex 32`. Sentinel requires `RELAYHUB_REDIS_SENTINEL_MASTER`;
Cluster requires database zero. `RELAYHUB_INSTANCE_ID` may pin a valid replica
name, otherwise each API/worker process generates a role-prefixed UUID and a new
random fencing generation on every start.

For production, use Redis Cluster or a managed equivalent when sharding is
required; Sentinel provides non-sharded failover. Set the validated key prefix per
environment, enable TLS, and use least-privilege ACL credentials. Rotate a Redis
password by staging the new credential in the Redis/ACL provider, updating every
replica secret, recreating API/worker, then revoking the old credential. Readiness
must recover before revocation. Never put username/password in an address.
Load balancers may distribute every request and reconnect to any API replica;
sticky sessions are neither required nor a correctness mechanism.
Root Compose uses one stream replica. Values 3 or 5 require an externally managed
NATS cluster and matching capacity. See the public
[NATS guide](../../web/docs/static/deploy/nats.md) for exact managed fields.

Callbacks need outbound HTTPS and trusted CA roots. Local HTTP callbacks are only
for controlled testing. Callback URL validation does not provide network egress
policy; restrict destinations and redirects at the worker/network boundary.

## Consistent cold backup

Schedule a brief maintenance window or use storage snapshots that keep
PostgreSQL and NATS JetStream consistent. Redis is excluded because its state is
TTL-bounded and reconstructible. PostgreSQL owns applications,
credentials, routing rules, events, delivery rows, idempotency and outbox state.
NATS owns private streams and duplicate windows. Back up the encrypted `.env`,
source revision, Compose file and image digests alongside the database and
JetStream volumes. Keep backups out of the checkout/build context, encrypt them
and move them to independent storage.

## Restore rehearsal

Restore into a **new** project first with a separate directory, backed-up
configuration and unused API host port. Restore PostgreSQL and NATS volumes
together, then start the stack with `docker compose -p relayhub-restore up -d
--wait --wait-timeout 90`. Verify readiness, inspect retained events/jobs and
complete a fresh signed routed publish plus callback or stream delivery before
using the procedure for production rollback.

## Upgrade and rollback

1. Make and rehearse a backup; record the previous revision and immutable image ID.
2. Run the release gate below on the candidate. Read any schema/config changes.
3. Build, then `docker compose up -d --build --wait --wait-timeout 90` using the
   existing project. Check all three healthy services and a synthetic delivery.
4. If necessary, return to the prior compatible application revision/image and
   recreate API/worker. Schema-incompatible rollback requires the paired data
   backup. Do not guess at PostgreSQL schema downgrades.

Worker shutdown stops new claims and drains active work up to the application
timeout. Durable delivery state survives runtime restarts. Active sockets close; reconnect and
resubscribe. A function claimed before API shutdown may time out, so replay its
same key to inspect the persisted result before attempting new side effects.

## Release gate and acceptance ownership

Install Go 1.27.1+, Python validators (`jsonschema==4.26.0` and
`openapi-spec-validator==0.9.0`), Node for docs test tooling, and Docker only when
you choose to run container checks. The verification path covers PostgreSQL,
NATS and Redis:
`go test -tags=integration ./...` uses disposable testcontainers unless explicit
test service URLs are supplied. Redis integration uses disposable Redis 7.4
containers; Redis is shared ephemeral state, not the removed polling prototype.

```bash
test -z "$(gofmt -l .)"
go vet ./...
go -C backend test ./...
go test -race ./...
go test -race -tags=integration ./... -count=1 -timeout=180s
./backend/scripts/build-skill.sh
./backend/scripts/build-llms.sh
python3 scripts/check-docs.py --static
./backend/scripts/check-contracts.sh --self-test
go -C backend generate ./web
```

If sources changed, run `go -C backend generate ./web` before final review and inspect the
generated diff. Compose config requires private `.env` credentials; never print the
full interpolated configuration into logs. The static docs checker validates parsed
contracts, schema fixtures, links, generated resources, console JavaScript and router
manifest parity. Runtime delivery is verified by Go integration tests covering
PostgreSQL migrations, routing rules, private NATS fan-out, stream delivery,
realtime channels and remote function lifecycles.

Worker `store_error` counts unexpected load, dispatch-start, finish and stream
acknowledgement failures as well as claim/promotion failures. Expected missing or
stale claims and intentional shutdown cancellation are excluded. Metric labels
and logs never contain the underlying storage error detail.

Documentation Markdown uses inline links. Reference-style links are rejected with
a clear checker error; embedded HTML links/images are crawled. Public docs changes
must be mirrored into llms files, the integration Skill and embedded web assets.

Successful authenticated request logs also include the persisted app ID. Application
operation logs record generated event/job IDs for publish/lease/admin transitions,
the validated event ID for acknowledgement, a persisted function ID at registration,
and the persisted invocation ID for a completed call or replay. Worker logs record
persisted target app/event/job IDs, callback attempt and outcome only after the
transition commits. Function names, callback URLs and handler error details remain
excluded. A function replay may change its URL, so its unchecked path is never
logged as the original function ID. Capture tests and acceptance assert these
specific IDs and outcomes while checking every sensitive sentinel remains absent.
