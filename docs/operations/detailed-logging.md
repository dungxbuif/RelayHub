# Detailed diagnostic logging

Extend the existing structured JSON logs and event detail view. Return a server-generated X-Request-ID that matches the HTTP log. Record route templates, status, duration and response bytes with INFO/WARN/ERROR severity. Queue logs identify subscriptions and leased deliveries; settlement logs record outcomes without receipts. Callback logs identify the attempt, generation, delivery, bounded reason, HTTP status and elapsed time.

Admin event detail exposes the already-returned delivery and attempt records alongside the timeline. No new datastore, log ingestion service or public log-reading endpoint is needed. Never record raw bodies, document text, headers, callback URLs, user-supplied request IDs, HMAC material or queue receipts. Existing admin event data remains a separately authorized detail view.

Tests cover server-generated correlation, severity, byte counts, secret exclusion and rendering persisted delivery attempts. Validate API/worker readiness and public correlation headers after deployment. Docker log rotation should remain bounded on the actual deployment.

## Reading logs

API and worker logs rotate at 10 MiB per file, keeping three files per container. The canonical Compose stack includes this policy; the VM100 deployment applies `backend/deploy/compose.logging.override.yml` as `prod/compose.override.yml`. Recreate the containers to activate a logging driver policy change.

Use `docker logs --since 15m relayhub-api` and `docker logs --since 15m relayhub-worker`. Match a client's `X-Request-ID` to request and application-operation records; follow `event_id` into queue lease and callback records. Settlement logs deliberately omit bearer-like receipt values and record only subscription and outcome. WebSocket HTTP log duration is connection lifetime; HTTP body byte count does not include hijacked WebSocket frames.

Admin: Events -> event detail -> Deliveries / Attempts / Lifecycle. Attempt elapsed time is the difference between persisted start/update timestamps, not a separate network timing measurement. Existing timelines remain bounded by the read API; this is not an unlimited log archive.

Admin HTTP fixtures use password login, session cookies and CSRF, matching the deployed authentication contract. The bearer rejection regression remains in place. Run the full HTTP suite before release; never restore bearer access to satisfy outdated fixtures.

## Deployment verification, 2026-09-20

Deployed `homelab/relayhub:prod-20260920-152029` to VM100 API and worker. Both readiness checks passed. Docker inspect confirmed json-file rotation with `max-size=10m`, `max-file=3` for both containers. A public unauthenticated read returned 401 and a generated X-Request-ID; the matching API log recorded WARN, the route template, response bytes and duration, without the supplied client request ID.

Admin typecheck and all 19 frontend tests passed, including delivery/attempt detail. Focused HTTP logging/session/public-docs tests and worker tests passed; container contract/resource checks passed. The stale bearer-auth fixture failures noted above remain a test-suite maintenance item, not a claim that the complete HTTP suite is green. No production event or OCR job was created for these checks.
