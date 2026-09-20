# Changelog

All notable changes to RelayHub are documented here. RelayHub follows semantic
versioning from the first production release.

## [1.0.0] - 2026-09-20

First production release.

### Added

- Durable application events with PostgreSQL state, transactional outbox and
  private NATS JetStream delivery.
- Signed callbacks, durable streams, routing rules and standard WebSocket realtime.
- Realtime v2 channels with scoped ACLs, targeting, presence, rewind, uploads and
  cross-replica routing.
- App-scoped Queue v2 with pull leases, heartbeats, explicit settlement, retries,
  dead letters, replay, draining, schedules and result callbacks.
- Short remote functions with registered owners and bounded deadlines.
- Password-based administrator sessions, CSRF protection, user/app/routing/queue
  management, delivery observability and detailed structured logs.
- Official Go and TypeScript SDKs, downloadable SDK archives, public Docusaurus
  documentation, OpenAPI, `llms.txt`, `llms-full.txt` and agent integration skill.
- Docker Compose production stack for API, worker, PostgreSQL, NATS and Redis.

### Security

- HMAC request authentication with timestamp checks and idempotency keys.
- Encrypted application secrets, one-time credential display and audited admin
  mutations.
- Non-root application containers, private data-plane services and narrowly scoped
  proxy attribution.

### Operations

- Production image: `homelab/relayhub:prod-eee9e7c`.
- Deployed API and worker are healthy at `https://relayhub.dungxbuif.com`.
- Backend race tests, PostgreSQL integration tests, SDK tests, admin tests, docs
  build and public SDK hash verification passed before release.

[1.0.0]: https://github.com/dungxbuif/RelayHub/releases/tag/v1.0.0
