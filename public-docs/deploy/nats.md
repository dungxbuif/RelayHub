# Private NATS and JetStream

RelayHub uses NATS as an internal data plane. Applications connect to RelayHub's
HTTP and WebSocket APIs; they never receive NATS credentials, subjects, stream
names, durable consumer names or sequence numbers. Root Compose does not publish
the NATS client or monitoring port to the host.

At startup, each RelayHub process verifies JetStream and idempotently creates
these file-backed streams when absent:

| Stream | Internal subject | Retention | Default maximum age |
| --- | --- | --- | --- |
| `RH_DELIVERIES` | `rh.v1.delivery.*` | work queue | `168h` |
| `RH_CALLBACKS` | `rh.v1.callback.*` | work queue | `168h` |
| `RH_DLQ` | `rh.v1.dlq.*` | limits | `168h` |

All three use file storage, discard-old limits, a 1 MiB message limit, a `24h`
duplicate window and one replica by default. RelayHub validates existing stream
settings and refuses to start when a managed field differs. It never silently
changes retention, storage, subjects, replicas or the duplicate window. Correct
the configuration deliberately after preserving the JetStream volume.

Core NATS uses `rh.v1.realtime.<app_token>` for best-effort observation,
`rh.v1.rpc.<app_token>.<function_token>` for function requests and
`rh.v1.rpc.reply.<instance_token>` for fenced replies. Callback shards use
`rh.v1.callback.<shard>`. RelayHub generates every token and shard; these
subjects remain private implementation details.

Set `RELAYHUB_NATS_USERNAME` and `RELAYHUB_NATS_PASSWORD` separately. Credentials
inside `RELAYHUB_NATS_URL` are rejected so diagnostics cannot accidentally print
them. Direct binary runs default to `nats://localhost:4222`; Compose uses
`nats://relayhub-nats:4222`. `tls://` is supported for an externally managed NATS
server. Replica counts are limited to `1`, `3` or `5`; a single Compose server
uses `1`.

`/healthz` reports only process liveness. `/readyz` rechecks every managed stream
and returns 503 when any required store, NATS connection or JetStream stream is
missing or differs from the approved configuration. Prometheus
metrics expose `relayhub_nats_connected` and bounded
`relayhub_nats_events_total{event=...}` values for disconnect, reconnect, slow
consumer, asynchronous client error, drain and bootstrap failure. They never use
subjects, application IDs, server URLs or credentials as labels.

JetStream data lives in the project-scoped `relayhub-nats-data` volume. Back it up
with PostgreSQL and configuration only after stopping RelayHub writers and NATS
cleanly. Restoring only one system can replay already accepted work, so consumers
must keep their event or delivery idempotency records.
