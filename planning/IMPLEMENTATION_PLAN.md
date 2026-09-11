# RelayHub MVP — detailed implementation plan v0.2

> **For agentic workers:** Use superpowers:executing-plans task-by-task. User requested planning here; do not start implementation or deployment from this document alone.

**Goal:** Hai app sử dụng một third-party provider cho realtime và jobs, hoàn thiện docs tích hợp + Skills, kiểm chứng trên một luồng OCR thật.

**Architecture:** Go API/admin/dispatcher, PostgreSQL ledger + outbox, NATS JetStream dispatch, Centrifugo WSS. Public API/SDK che broker; SDK RelayHub optional; raw WebSocket tuân thủ Centrifugo protocol.

**Tech Stack:** Go, PostgreSQL, NATS JetStream, Centrifugo OSS; TypeScript SDK, React/Vite admin, Docusaurus public docs.

**Spec:** [SPEC](../SPEC.md), [API_CONTRACT](../API_CONTRACT.md), [SYSTEM_DESIGN](../SYSTEM_DESIGN.md), [OPERATIONS](../OPERATIONS.md), [DOCUMENTATION_STRATEGY](../DOCUMENTATION_STRATEGY.md).

## Trạng thái

Source bootstrap đã có; provider implementation chưa thực hiện. Plan v0.2 thay bản 7 task tổng quát bằng 17 work items trong 5 work packages. Không thay đổi code/runtime/API đang chạy. Các quyết định kỹ thuật mới ở [ENGINEERING_DETAILS](ENGINEERING_DETAILS.md) là baseline để review trước code.

## Global constraints

- Domain relayhub.dungxbuif.com: /api/v1, /connection/websocket, /docs/; admin private.
- Project từ credentials; scope và quyền channel bắt buộc; không public NATS/DB/admin engine.
- Accepted job = PostgreSQL commit; outbox dispatch async, at-least-once, no exactly-once side-effect promise.
- Giữ config/httpapi bootstrap, không tạo module nền tảng trùng lặp.
- Ghim dependency versions ở F1; source paths repo-relative, shell commands prefix rtk.
- Không làm Kafka, Redis, arbitrary-code functions, billing, workflow DAG hoặc tunnel riêng trong MVP.
- Internal docs + public human/agent docs cập nhật cùng tính năng. Skills chỉ hướng dẫn feature đã kiểm chứng.
- Không bỏ qua integration tests vì dependencies vắng mặt; gate unavailable phải ghi chưa kiểm chứng.

## Work packages và sản phẩm review

| Package | Items | Phụ thuộc | Kết quả review được |
|---|---|---|---|
| [01 Foundation](work-packages/01-foundation.md) | F1–F4 | Bootstrap | DB/test harness, project auth, admin private, docs site base |
| [02 Jobs](work-packages/02-jobs.md) | J1–J4 | F1–F3 | Enqueue bền, outbox, lease, retry, replay, queries |
| [03 Realtime/SDK](work-packages/03-realtime-sdk.md) | R1–R3 | F2; progress/worker cần J3–J4 | Native SDK + raw WS compatibility, worker SDK |
| [04 Product/docs](work-packages/04-product-docs.md) | P1–P3 | F4, J4, R3 | Admin UI, OCR thật, Skills và agent exports |
| [05 Release](work-packages/05-release.md) | D1–D3 | Tất cả trên | Ingress, fault/load/restore, release evidence |

Thứ tự mặc định: F1 → F2 → F3 → F4 → J1 → J2 → J3 → J4 → R1 → R2 → R3 → P1 → P2 → P3 → D1 → D2 → D3.

Realtime auth R1 có thể bắt đầu sau F2 khi có nhu cầu tách việc; không yêu cầu parallel agents. Chưa cần ưu tiên tối ưu lịch trước khi interface đầu tiên được kiểm chứng.

## Detailed engineering và data contracts

[ENGINEERING_DETAILS](ENGINEERING_DETAILS.md) xác định migrations 001–005, ownership FKs, lock order, outbox leasing, MQ topic/consumer topology, generation fencing, worker lease expiry, admin read APIs và channel wire mapping.

[TEST_MATRIX](TEST_MATRIX.md) có 27 tình huống kiểm chứng lỗi, cách ly, protocol, downloads và vận hành. Mỗi item trong work package có files/interfaces, acceptance, RED/GREEN command, docs impact và scoped commit gate.

## Review checkpoints

1. Sau F4: tạo project A/B, quyền tách biệt, admin public bị từ chối; xem docs base.
2. Sau J4: demo NATS offline rồi phục hồi, worker bị kill, retry/replay; xem job/attempt ledger.
3. Sau R3: demo cùng provider bằng official Centrifugo JS client và raw WebSocket; xem worker SDK.
4. Sau P3: một app OCR thật cùng app thứ hai, dashboard, Skills copy/download và agent fetch.
5. Sau D2: review measured load/restore, ingress diff và rollback. D3 chỉ deploy trong phạm vi authorization hiện hành.

## Definition of done cho mỗi item

- Test thể hiện failure trước thay đổi, rồi pass sau implementation.
- Không có cross-project bypass hoặc fake success trả từ route chưa làm.
- Runtime OpenAPI khớp code; public examples khớp release; internal decisions và runbook đúng behavior.
- Relevant skill/resources cập nhật hoặc ghi rõ chưa liên quan; không đóng docs sau code ở task khác.
- Record exact commit/test output; commit chỉ files thuộc task, cập nhật STATUS.

## Scope sau MVP

Phase 2: webhook ingress/egress và durable event fan-out API. Phase 3: hosted function runner, scheduler, Python SDK. Phase 4: self-service/team/billing/tunnel tùy nhu cầu. [ROADMAP](../ROADMAP.md) vẫn là lộ trình sản phẩm; 5 work packages ở đây là cách thực thi Phase 1, không đổi tên product phases.

## Tài liệu cần tạo khi thực thi

DEPENDENCY_LOCK là versions/digests đã kiểm tra. CAPACITY_REPORT chỉ sanitized metrics; exact host inventories nằm local không commit. VALIDATION_REPORT/RELEASE_CHECKLIST chứa observed evidence, không điền PASS trước khi chạy. Site Docusaurus và downloads chưa tồn tại ở thời điểm viết plan.
