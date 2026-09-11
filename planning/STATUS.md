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

Project auth/provisioning, PostgreSQL ledger/outbox, NATS dispatch, leases/retry/replay, Centrifugo integration, SDK, dashboard, queue/realtime sample integration, TLS/domain deployment và production restore chưa có. 17 items F1–D3 vẫn mở trong IMPLEMENTATION_PLAN.

## Next milestone

F1 dependency/test harness, F2 project/auth và F3 private admin provisioning. Mục tiêu nghiệm thu tiếp theo là hai project có credentials và quyền tách biệt trên PostgreSQL thật. Bootstrap 501 không phải API job hoạt động.

## Workspace

Repository riêng: github.com/dungxbuif/RelayHub. Snapshot source/docs được tách từ homelab để review; không deploy hoặc tạo DNS.

## Documentation requirement added 2026-09-11

Local/internal docs and public integration docs maintained together. AGENTS.md and DOCUMENTATION_STRATEGY.md record the requirement; public-docs/ is initialized as a content boundary. Docusaurus site, agent export generator, Skills copy/download UI and packages are not implemented yet.

## Detailed planning v0.2 — 2026-09-11

17 implementation items trong 5 work packages; ENGINEERING_DETAILS và TEST_MATRIX bổ sung schema/transactions/interfaces/acceptance. Tất cả implementation items vẫn chưa bắt đầu. Chỉ docs thay đổi trong revision này; bootstrap runtime/OpenAPI không thay đổi.

## Scope correction — external integrations

User clarified: external business apps integrate after RelayHub is complete; they are not MVP deliverables or release dependencies. Replace the domain-specific integration task with a self-contained queue/realtime sample and second-project isolation fixture. Update internal specs, flows and public docs scope; verify no domain-specific implementation requirements remain and validate Markdown links. Runtime unchanged.
