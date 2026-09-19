---
title: RelayHub Introduction
description: RelayHub hướng dẫn chung cho developer và operator.
slug: /
---

# RelayHub Docs

RelayHub giúp bạn kết nối ứng dụng qua các lớp:

- **HTTP event API** với chữ ký HMAC và idempotency.
- **Routing relay** theo `event_type`/`app`.
- **Real-time / Durable**: WebSocket cho nhánh realtime và stream/Jetsream cho xử lý bền.
- **Shared scale foundation**: Redis giữ session, rate limit và ownership có TTL;
  PostgreSQL/NATS vẫn giữ dữ liệu bền.

Tài liệu này là track Docusaurus mới cho đội ngũ vận hành và tích hợp.
API và worker có thể scale ngang mà không cần sticky session. Khi Redis mất kết
nối, `/readyz` trả 503 và admission phụ thuộc Redis fail closed, nhưng event đã
được PostgreSQL chấp nhận không bị mất.

## Bạn có thể đi theo 2 lộ trình

### 1) User track
- Tạo app và config delivery mode.
- Kiểm tra health/readiness.
- Theo dõi flow xử lý sự kiện.

### 2) Developer track
- Tích hợp HMAC (không cần tenant).
- Đăng ký events, route rules và realtime channel.
- Xem skill mẫu để tích hợp nhanh.

## Dữ liệu tài liệu gốc

Các nội dung hợp đồng kỹ thuật vẫn phải khớp với:

- `/docs/openapi.json`
- `/docs/asyncapi.yaml`
- `/docs/skills/relayhub-integration.zip`
- `/docs/llms-full.txt`

Mọi thay đổi contract bắt buộc cập nhật đồng thời: `web/docs/static/`, `docs/` và `internal` kiểm tra `scripts/check-contracts.sh`.
