# RelayHub

**Realtime & Messaging Provider — planning baseline v0.1 — 2026-09-10.**

RelayHub là provider dùng chung cho realtime, queue, functions và webhook. Các app tích hợp qua HTTPS/WSS và SDK, không tự triển khai WebSocket server hoặc message broker riêng.

Tên RelayHub là tên làm việc, chưa kiểm tra trùng thương hiệu. Định hướng sản phẩm và domain đã chốt; các default kỹ thuật là baseline để triển khai và kiểm chứng, chưa phải SLA. Đã có runtime in-memory cơ bản tại `src/`; các lớp hạ tầng bền chưa thay đổi.

## Tài liệu

- [Đặc tả sản phẩm](SPEC.md): mục tiêu, ranh giới provider, yêu cầu và tiêu chí MVP.
- [System design](SYSTEM_DESIGN.md): công nghệ đề xuất, luồng dữ liệu, quyền truy cập, lưu trữ và vận hành.
- [Lộ trình và quyết định còn mở](ROADMAP.md): thứ tự triển khai và việc cần xác minh.

## Định hướng đã thống nhất

- Thiết kế như third-party provider, kể cả khi khách hàng đầu tiên là các app do cùng một người sở hữu.
- App không cần tham gia Tailnet hoặc truy cập database/broker nội bộ.
- Realtime, job queue và event fan-out có ngữ nghĩa riêng.
- Webhook là adapter vào/ra; tunnel là tính năng bổ sung, không phải lõi sản phẩm.
- API/SDK, credentials và project isolation cần có từ đầu; billing và đăng ký công khai để sau.

Stack baseline: **Go + Centrifugo OSS + NATS JetStream + PostgreSQL**, dashboard React/TypeScript/Vite, SDK TypeScript trước. Centrifugo chạy riêng; không viết WebSocket engine hoặc universal MQ adapter. Ghim phiên bản và đo capacity là Task 0 trước triển khai.

## Bộ planning hoàn chỉnh

- [API contract](API_CONTRACT.md): provisioning, scopes, tokens, jobs, worker lease và lỗi.
- [Integration flows](INTEGRATION_FLOWS.md): đăng ký queue/socket và pseudocode cho app.
- [Operations](OPERATIONS.md): giới hạn ban đầu, domain routing, backup và deployment gates.
- [Implementation plan](planning/IMPLEMENTATION_PLAN.md): 17 work items, 5 work packages, file map và acceptance.

## Điểm vào duy nhất

| Giao diện | Địa chỉ |
|---|---|
| Dashboard admin | `https://relayhub.dungxbuif.com/` — Tailnet + login |
| API | `https://relayhub.dungxbuif.com/api/v1` |
| WebSocket | `wss://relayhub.dungxbuif.com/connection/websocket` |
| Webhook tương lai | `https://relayhub.dungxbuif.com/hooks/*` — chưa mở MVP |

## Trạng thái

- Planning: hoàn thiện v0.1 theo quyết định một domain và third-party provider.
- Implementation: đã triển khai runtime in-memory cho jobs/attempts và realtime stub ở `src/`; provider MVP đầy đủ chưa triển khai, DNS/TLS chưa thay đổi.
- Nhắm mục tiêu: PostgreSQL ledger + outbox commit; JetStream phân phối, Centrifugo phục vụ realtime.
- Đọc theo thứ tự: SPEC → SYSTEM_DESIGN → API_CONTRACT → INTEGRATION_FLOWS → OPERATIONS → IMPLEMENTATION_PLAN.

## Source và tiến độ

- [Source README](src/README.md): chạy local, behavior thực tế và test.
- [Execution status](planning/STATUS.md): phần đã làm và milestone tiếp theo.
- [Source bootstrap plan](planning/SOURCE_BOOTSTRAP.md): phạm vi bước khởi tạo.

## Documentation requirements

Maintain [internal and public documentation](DOCUMENTATION_STRATEGY.md) together. [Public docs source](public-docs/README.md) is separate from internal planning. Future site includes a Skills tab with copy/download resources and agent-readable Markdown/OpenAPI.

## Review

Bắt đầu tại [Review guide](planning/REVIEW_GUIDE.md). Repo độc lập là nơi tiếp tục phát triển; bản snapshot cũ trong homelab được giữ nguyên.

Plan v0.2: [work package index](planning/IMPLEMENTATION_PLAN.md), [schema và recovery design](planning/ENGINEERING_DETAILS.md), [verification matrix](planning/TEST_MATRIX.md).
