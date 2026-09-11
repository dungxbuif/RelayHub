# RelayHub execution status

## Completed: source bootstrap

- Go module tại src/, standard library only.
- Config loopback mặc định, validation host/port.
- HTTP health, readiness, JSON errors và request IDs.
- Graceful shutdown và HTTP timeout.
- Runtime OpenAPI, local README, smoke script.
- RED: tests thất bại do Load/NewHandler chưa có; GREEN: implementation qua tests.

## Verification

- Go 1.26.3 darwin/arm64.
- go test -race ./...: PASS (config + HTTP route tests).
- go vet ./...: PASS.
- go build ./cmd/relayhub: PASS.
- HTTP smoke: health 200, readiness 503, jobs 501, admin 404; SIGTERM exit 0: PASS.

## Not implemented

Project auth/provisioning, PostgreSQL ledger/outbox, NATS dispatch, leases/retry/replay, Centrifugo integration, SDK, dashboard, real OCR integration, TLS/domain deployment và production restore chưa có. Task 0–6 vẫn mở trong IMPLEMENTATION_PLAN.

## Next milestone

Task 0 dependency/capacity gates và Task 1 foundation/auth. Mục tiêu nghiệm thu tiếp theo là hai project có credentials và quyền tách biệt trên PostgreSQL thật. Bootstrap 501 không phải API job hoạt động.

## Workspace

Repository riêng: github.com/dungxbuif/RelayHub. Snapshot source/docs được tách từ homelab để review; không deploy hoặc tạo DNS.

## Documentation requirement added 2026-09-11

Local/internal docs and public integration docs maintained together. AGENTS.md and DOCUMENTATION_STRATEGY.md record the requirement; public-docs/ is initialized as a content boundary. Docusaurus site, agent export generator, Skills copy/download UI and packages are not implemented yet.
