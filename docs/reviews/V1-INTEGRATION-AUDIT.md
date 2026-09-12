# RelayHub v1 integration audit

Date: 2026-09-12

This audit checks the active v1 goal: integrate the designed use cases and features into the current implementation, with homelab Docker deployment readiness and no GitHub workflow dependency. The authoritative runtime shape is PostgreSQL for control/state, private NATS/JetStream for durable delivery and cross-instance fan-out, HTTP APIs for administration/public writes, and standard RFC 6455 WebSocket endpoints for realtime, stream delivery and functions.

## Requirement status

| Requirement | Current evidence | Status |
| --- | --- | --- |
| Admin can create applications and receive one-time API key/HMAC credentials. | `POST /api/v1/apps` is registered as admin in `internal/httpapi/routes.go`; console and SDK call the route; HTTP tests and SDK tests cover admin helpers. | Covered |
| Applications can publish signed durable events. | `POST /api/v1/events` is registered as app-authenticated; default and integration tests pass; SDK Go/TypeScript publish helpers exist. | Covered |
| Event routing can resolve targets when `target_app_ids` is omitted or empty. | `internal/service/routing.go`, `internal/store/postgres/routing.go` and migration `010_routing_rules.sql`; `TestFullStackRoutedEventReachesRealtimeAndDurableStream` covers admin rule creation and routed publish without explicit targets. | Covered |
| Realtime channels can be reused by future apps without each app owning a custom WebSocket backend. | `/ws` supports `channel:<name>` subscriptions; `POST /api/v1/realtime/channels/{channel}/publish` publishes signed app messages; NATS bridge fans out across API instances. | Covered |
| Durable consumer delivery must not be public polling. | Public `/api/v1/queue` is not registered; `internal/httpapi/events_test.go` asserts the old queue route returns 404. Durable delivery uses `/api/v1/stream` and callbacks. | Covered |
| Stream delivery must use private broker state and support acknowledgement. | `/api/v1/stream` is registered with `stream:connect`; `TestFullStackRoutedEventReachesRealtimeAndDurableStream` opens a real WebSocket stream, receives `event.delivery`, sends `delivery.ack`, and receives `delivery.accepted`. | Covered |
| Remote functions use live request/response and are not an offline queue. | Function routes and `/ws` RPC frames exist; docs state claim/timeout behavior; integration tests cover function wakeup and non-redispatch behavior. | Covered |
| Go SDK exposes publish, consume/observe, realtime, function and admin/routing helpers. | `sdk/go/client.go`, `events.go`, `functions.go`, `apps_routing.go`; `go test ./sdk/go -count=1` passes. | Covered |
| TypeScript SDK exposes Node trusted backend APIs and browser token-based entry points. | `sdk/typescript/src/node.ts`, `src/browser.ts`, stream/legacy clients; `npm --prefix sdk/typescript test` passes. | Covered |
| Management console supports the core integration flow. | `public-docs/console.html` and `public-docs/assets/console.js` support settings, create/list apps, create/list routing rules, signed event publish, realtime publish and realtime subscribe. Docs checker exercises console JavaScript behavior. | Covered |
| Public docs expose human docs and AI-agent-readable surfaces. | `public-docs/*.md`, `public-docs/openapi.json`, `public-docs/llms.txt`, `public-docs/llms-full.txt`, `public-docs/skills/relayhub-integration.zip`; static docs checker verifies generated parity. | Covered |
| Public docs must match current implementation. | Redis/polling queue text that described a current v1 path was removed from public user/security/deploy/OpenAPI docs. Remaining Redis mentions in internal ADR/history are explicitly historical, or are `redispatch` wording unrelated to Redis storage. | Covered |
| Docker acceptance script should represent v1 if used as a release signal. | The legacy Redis polling acceptance scripts have been removed. Deploy readiness now relies on local Go default/race tests, PostgreSQL/NATS integration tests, docs/contracts checks, Docker build and Compose config validation; GitHub workflow files are not part of the repo. | Covered |

## Fresh verification evidence

The following commands passed after the audit corrections:

```text
python scripts/check-docs.py --static
PASS: parsed contracts, schema fixtures, links, reproducible resources, route parity

go test ./... -count=1
352 passed in 22 packages

go test -tags=integration ./... -count=1
393 passed in 22 packages

go test ./sdk/go -count=1
8 passed in 1 package

npm --prefix sdk/typescript ci && npm --prefix sdk/typescript test
2 passed
```

The integration suite includes a full-stack HTTP/WebSocket smoke test over real PostgreSQL and NATS for the management-console use case: admin creates apps and a routing rule, producer publishes a routed event without explicit targets, realtime subscribers receive `channel.message`, durable stream consumers receive `event.delivery`, and ACK receives `delivery.accepted`.

## Remaining follow-up if scope expands

If a black-box Docker acceptance suite is needed later, write a new PostgreSQL/NATS suite around the current public API: create apps, create routing rules, publish routed events, consume `/api/v1/stream`, acknowledge delivery, and verify realtime channel fan-out. Do not resurrect the removed Redis polling acceptance path.
