# RelayHub operations runbook

The supported deployment is root `compose.yaml`; its public downloadable copy is
byte-identical. All commands below run from the checkout used to start the stack.
Keep the same Compose project name for normal upgrades. Use `-p` explicitly when
operating more than one instance; networks and `relayhub-data` volumes are scoped
to that name. Do not use global Docker prune commands for RelayHub maintenance.

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

`healthz` is process liveness; `readyz` requires Redis. Redis failure makes API and
worker Docker health unhealthy. Docker restart policies restart exited processes,
not merely unhealthy containers. Restore Redis connectivity/authentication, then
check readiness recovery. Scrape worker metrics from a trusted client already on
the project network at `http://relayhub-worker:9090/metrics`; no host port is opened.
The probe returns status only and deliberately suppresses bodies/URLs.

HTTP JSON logs contain a server-generated `request_id`, method, matched route
**template**, status, latency and bounded outcome. They never use the caller's
request-ID header as the logged ID. Unknown methods/paths have bounded labels.
WebSocket access logging happens when the upgraded connection finishes. Function
and callback operations log bounded outcomes. No headers, raw query/target,
callback URL, signature, request/response body, event data or function input/result
is logged. External proxy logging must apply equivalent redaction separately.
Metrics labels remain bounded; never add app/event IDs or payloads to metric labels.

## Configuration changes

Keep `.env` mode 600 and store it securely outside source control. All settings and
defaults are listed in [the public deployment guide](../../public-docs/deploy/README.md).
Changing `.env` requires `docker compose up -d --wait` to recreate affected services;
`docker compose restart` alone does not load changed environment. Keep
`RELAYHUB_STOP_GRACE_PERIOD` greater than `RELAYHUB_SHUTDOWN_TIMEOUT`. Redis always
receives a 30-second graceful stop budget. API/worker share namespace and secrets.
Changing the namespace selects another dataset and never migrates records.

Callbacks need outbound HTTPS and trusted CA roots. Local HTTP callbacks are only
for controlled testing. Callback URL validation does not provide network egress
policy; restrict destinations and redirects at the worker/network boundary.

## Consistent cold backup

Schedule a brief maintenance window. This copies the complete Redis 7 AOF directory
only after writers and Redis stop. It includes the manifest/base/incremental files.
The temporary helper is an operator tool, not a fourth long-lived stack service.
It runs UID/GID 999, matching Redis-owned 700 directories and 600 AOF files, with
zero capabilities. Root without DAC capabilities cannot read those private files.
Archives stream over stdout/stdin; the host creates the archive under umask 077,
so no container needs access to the host's private backup directory. A successful
archive is renamed from `.partial`; do not treat a partial archive as a backup.

```bash
backup_dir="$HOME/relayhub-backups/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$backup_dir"
chmod 700 "$backup_dir"
redis_id=$(docker compose ps -q relayhub-redis)
redis_volume=$(docker inspect --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Name}}{{end}}{{end}}' "$redis_id")
test -n "$redis_volume"
docker compose stop relayhub-api relayhub-worker
docker compose stop relayhub-redis
umask 077
docker run --rm --network none --read-only --cap-drop ALL \
  --security-opt no-new-privileges --user 999:999 \
  -v "$redis_volume:/data:ro" \
  redis:7-alpine tar -C /data -czf - . > "$backup_dir/redis-data.tar.gz.partial" && \
  mv "$backup_dir/redis-data.tar.gz.partial" "$backup_dir/redis-data.tar.gz"
docker compose up -d --wait --wait-timeout 90
```

Back up the encrypted `.env`, source revision, Compose file, image digests and Redis
version alongside the archive. Keep archives out of the checkout/build context,
encrypt them and move them to independent storage. Check successful backup before
resuming upgrade work. AOF every-second fsync can lose recent accepted writes on a
host crash; backups and graceful restart tests do not promise zero data loss.

## Restore rehearsal

Restore into a **new** project/volume first. Stop all its services before writing to
its volume. Use a separate directory with the backed-up Compose/configuration,
a distinct project name and an unused API host port. Never overwrite a running
production volume or restore over current AOF files.

```bash
# In the separate restore checkout, set its .env port and backed-up credentials.
docker compose -p relayhub-restore create
redis_id=$(docker compose -p relayhub-restore ps -aq relayhub-redis)
redis_volume=$(docker inspect --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Name}}{{end}}{{end}}' "$redis_id")
test -n "$redis_volume"
# backup_dir is the absolute directory holding redis-data.tar.gz.
docker run --rm --network none --read-only --cap-drop ALL \
  --security-opt no-new-privileges --user 999:999 -i \
  -v "$redis_volume:/data" \
  redis:7-alpine tar -C /data -xzf - < "$backup_dir/redis-data.tar.gz"
docker compose -p relayhub-restore up -d --wait --wait-timeout 90
```

Use the same Redis image version to load the snapshot. Keep secrets and namespace
unchanged. Verify readiness, inspect a retained job, and complete a fresh signed
publish/lease/ack and callback. Replayed events may represent already committed
side effects; receiver deduplication state must survive the restore too. Remove
only this rehearsal project when finished. Production cutover requires its own
maintenance window and a final consistent backup.

## Upgrade and rollback

1. Make and rehearse a backup; record the previous revision and immutable image ID.
2. Run the release gate below on the candidate. Read any schema/config changes.
3. Build, then `docker compose up -d --build --wait --wait-timeout 90` using the
   existing project. Check all three healthy services and a synthetic delivery.
4. If necessary, return to the prior compatible application revision/image and
   recreate API/worker. Schema-incompatible rollback requires the paired data
   backup. Do not guess at Redis schema downgrades.

Worker shutdown stops new claims and drains active work up to the application
timeout. Queue state survives runtime restarts. Active sockets close; reconnect and
resubscribe. A function claimed before API shutdown may time out, so replay its
same key to inspect the persisted result before attempting new side effects.

## Release gate and acceptance ownership

Install Go 1.27.1+, Python validators (`jsonschema==4.26.0` and
`openapi-spec-validator==0.9.0`), Node for docs test tooling, and Docker/Compose.
Use a disposable reachable Redis for `RELAYHUB_TEST_REDIS_URL`; integration tests
must fail if it is unreachable. CI supplies an explicit Redis service without
repository secrets. Contract smoke uses `RELAYHUB_DOCS_TEST_REDIS_URL` when supplied (CI points this at
its mandatory Redis service), creates a random `relayhubdocs_` namespace, and deletes
only that namespace after API shutdown, on both success and failure. It never
flushes the shared database. Without that setting, local smoke requires and spawns
host `redis-server`. A supplied but unreachable URL fails; it never falls back or
skips. `python3 scripts/test-docs-runtime.py` proves the external path without a
host Redis binary, while preserving unrelated Redis keys.

The docs-test URL supports lowercase `redis://` or `rediss://`, optional credentials/port,
and an optional nonnegative decimal database path. A single `?db=1` overrides
the path database, matching the application's pinned go-redis parser; readiness
and prefix cleanup select that effective database too. Database numbers must fit
a signed 64-bit integer. Duplicate, empty, signed, malformed or unsupported query
options, invalid paths and fragments fail before the API launches, without
printing the URL. Other go-redis query options are intentionally unsupported by
this test checker. The real-service regression verifies both success/failure
cleanup for `/0?db=1` and preserves unrelated keys in DB 0 and DB 1.

```bash
test -z "$(gofmt -l .)"
go vet ./...
go test ./...
go test -race ./...
go test -race ./scripts/e2e-client.go ./scripts/e2e-client_test.go -count=1 -timeout=20s
go test -race -tags=integration ./... -count=1 -timeout=180s
./scripts/build-skill.sh
./scripts/build-llms.sh
python3 scripts/check-docs.py
./scripts/check-contracts.sh --self-test
docker build -t relayhub:release-candidate .
docker compose config --quiet
./scripts/e2e.sh
./scripts/e2e.sh --backup-rehearsal
```

If sources changed, run `go generate ./web` before the gate and review the generated
diff. Compose config requires the private `.env` credentials; never print the full
interpolated configuration into logs. The `--quiet` flag validates without output.

Acceptance uses a cryptographically random project name, private generated process
credentials, a temporary host callback listener and a single host-gateway mapping
on the worker. No fourth service, external deployment or image push occurs. Build
and runtime commands are bounded. It validates exactly three healthy containers,
API-only publication, independent HMAC/RFC 6455 behavior, docs bytes/ZIP, API/worker
and AOF restart durability, generated log IDs/outcomes and secret redaction. Docker
output stays captured in memory; only stage names and safe assertion failures print.
Cleanup removes only that project's containers, volume, network and image tag.
`RELAYHUB_E2E_KEEP=1` deliberately preserves the project for diagnosis. Docker
inspect can reveal container environment; treat retained projects as sensitive.

KEEP prints a self-contained cleanup command scoped to the generated Compose
project label. It removes that project's containers, network, volumes and local
image without requiring credentials, the repository directory or temporary
Compose overrides. Copy and run the printed command when diagnosis is complete.

Worker `store_error` counts unexpected load, dispatch-start, finish and stream
acknowledgement failures as well as claim/promotion failures. Expected missing or
stale claims and intentional shutdown cancellation are excluded. Metric labels
and logs never contain the underlying storage error detail.

Documentation Markdown uses inline links. Reference-style links are rejected
with a clear checker error; embedded HTML links/images are crawled. Browser QA
supplements static checks with desktop/mobile rendering and real focus/clipboard
behavior. Release evidence and screenshots are indexed in
[MVP verification](../reviews/MVP-VERIFICATION.md).

Successful authenticated request logs also include the persisted app ID. Application
operation logs record generated event/job IDs for publish/lease/admin transitions,
the validated event ID for acknowledgement, a persisted function ID at registration,
and the persisted invocation ID for a completed call or replay. Worker logs record
persisted target app/event/job IDs, callback attempt and outcome only after the
transition commits. Function names, callback URLs and handler error details remain
excluded. A function replay may change its URL, so its unchecked path is never
logged as the original function ID. Capture tests and acceptance assert these
specific IDs and outcomes while checking every sensitive sentinel remains absent.

Run `./scripts/e2e.sh --backup-rehearsal` for the disposable-volume backup test.
It creates only its unique source/restore volumes, populates UID/GID 999 private
AOF-like files, streams a backup, restores into a fresh Redis-initialized volume,
and verifies exact AOF/manifest bytes, directory mode 700 and file modes 600. It
removes only those volumes and its temporary helper container on success/failure.
No production volume is opened. All backup/restore helpers have zero capabilities.

Acceptance subprocesses run in dedicated Unix process groups. Context cancellation
sends TERM to the group, follows with KILL after 150 ms, and uses a 250 ms Go
`WaitDelay` to bound inherited pipe waits even when the Docker CLI exits before
its Compose child. An orphan-child regression proves bounded return, descendant
death and deferred cleanup continuation. Cleanup commands use the same runner and
an independent bounded context.
