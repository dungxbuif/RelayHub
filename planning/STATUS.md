# RelayHub execution status

## Implemented: in-memory provider baseline (partial)

- Config now includes runtime credentials env:
  `RELAYHUB_BACKEND_TOKEN`, `RELAYHUB_WORKER_TOKEN`, `RELAYHUB_REALTIME_TOKEN`.
- HTTP handler chạy thực tế cho:
  - `GET /healthz`, `GET /readyz`
  - `POST /api/v1/jobs` (idempotency-key + project scope)
  - `GET /api/v1/jobs/{id}`
  - `POST /api/v1/workers/claim`
  - `POST /api/v1/attempts/{id}/heartbeat|progress|complete|fail`
  - `POST /api/v1/realtime/sessions`
  - `POST /api/v1/realtime/grants`
  - `POST /api/v1/realtime/publish`
- Health/readiness và lỗi JSON có `X-Request-ID` mới mỗi request.
- Tests hiện tại bao phủ lifecycle cơ bản: auth, idempotency job, claim, heartbeat, complete, read, realtime stubs.
- Documentation cập nhật cùng docs: `src/README.md`, `public-docs/README.md`, và note runtime trong `README.md`.
- `api/openapi.json` vẫn cần tái sinh cho contract khớp đầy đủ khi đóng package 0.

## Verified

- Unit tests cho `internal/config` và `internal/httpapi` đã được bổ sung theo logic mới.
- `main.go` dùng `config` có tokens.
- PR scope vẫn ở mức implementation in-memory, chưa phải full production.

## Not implemented in this phase

PostgreSQL ledger, outbox/dispatch, Centrifugo real-time engine, durable queue replay/redistribution, admin portal, external app sample, skills exports, và các guardrail production vẫn chưa triển khai.

## Next milestone

- Harden API contracts (OpenAPI regeneration), thêm validation sâu hơn, mở rộng admin/API key provisioning thật.
- Tách data store chuẩn hoá theo ENGINEERING_DETAILS cho `J1–J4`.
