# RelayHub documentation

- [Root quick start](../README.md): healthy stack and first signed event.
- [Public human and agent documentation](../public-docs/README.md).
- [Architecture overview](architecture/overview.md).
- [Developer reference](developer/README.md).
- [Deployment decisions](developer/deployment-stack.md).
- [Operations runbook](operations/runbook.md): health, observability, backup/restore,
  upgrade/rollback and the release gate.
- [PostgreSQL control store](architecture/postgresql-control-store.md): schema,
  encryption, migrations and verification contract.
- [Transactional event outbox](architecture/outbox-dispatch.md): atomic
  acceptance, dispatcher recovery and readiness behavior.
- [RelayHub v1 NATS platform design](superpowers/specs/2026-09-12-relayhub-nats-platform-design.md):
  private NATS/JetStream data plane, PostgreSQL outbox, SDK streaming and console.
- [RelayHub v1 implementation plan](superpowers/plans/2026-09-12-relayhub-nats-platform.md):
  twelve tasks from contract freeze through Redis removal and final verification.

Technical changes include an implementation note before code and reconciled
internal/public documentation afterwards. Public Markdown is canonical; OpenAPI,
schemas, llms indexes and the integration Skill are stable agent surfaces.
Run `go generate ./web` after public edits, then `./scripts/check-contracts.sh
--self-test`. `.github/workflows/ci.yml` enforces source/runtime documentation parity
and the complete production-stack acceptance gate on pushes and pull requests.
