# Review guide — RelayHub v0.1

## Review scope

Repository riêng cho planning và source bootstrap. Chưa triển khai auth, durable queue, Centrifugo, SDK, dashboard hoặc public docs site. Không cần review như một production provider hoàn chỉnh.

## Thứ tự đọc

1. [SPEC](../SPEC.md): mục tiêu third-party provider, ranh giới trách nhiệm và MVP.
2. [SYSTEM_DESIGN](../SYSTEM_DESIGN.md): Go + Centrifugo + JetStream + PostgreSQL, một domain.
3. [API_CONTRACT](../API_CONTRACT.md): queue, token, lease, idempotency và scopes.
4. [INTEGRATION_FLOWS](../INTEGRATION_FLOWS.md): client và worker pseudocode.
5. [ROADMAP](../ROADMAP.md) và [implementation plan](IMPLEMENTATION_PLAN.md): 4 phase, 7 task MVP.
6. [DOCUMENTATION_STRATEGY](../DOCUMENTATION_STRATEGY.md): docs engineering/local và public integration, Skills tab, agent exports.
7. [Source README](../src/README.md): behavior hiện có và cách chạy local.

## Quyết định cần review

- Realtime dùng Centrifugo; raw WebSocket phải nói protocol của Centrifugo. Socket.IO chưa hỗ trợ.
- Job ledger/outbox ở PostgreSQL, JetStream lo dispatch; chấp nhận trách nhiệm viết lease/reconciler trong RelayHub.
- Public API/WSS và docs chung relayhub.dungxbuif.com; admin chỉ Tailnet + login.
- Functions MVP là trusted worker handler; không chạy code tùy ý của khách.
- Default quota/retention là thiết kế ban đầu, không là capacity đo được hoặc SLA.

## Tiến độ

Bootstrap local đã có health/error/config/shutdown và tests. Tất cả milestone provider vẫn mở. Website public docs và skill packages mới ở planning. Bước kế tiếp sau review là dependency/capacity audit và auth/project provisioning.

## Repository separation

Tách bản snapshot RelayHub khỏi thư mục homelab vào repo độc lập; bản cũ ở homelab chưa xóa. Repo mới là nơi tiếp tục phát triển. Không kèm credentials hoặc tài liệu hạ tầng của các dịch vụ khác. Engineering docs trong repo này không đồng nghĩa được đưa vào website public-docs.
