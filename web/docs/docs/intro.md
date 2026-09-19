---
title: RelayHub Introduction
description: RelayHub hướng dẫn chung cho developer và operator.
slug: /
---

# RelayHub Docs

RelayHub giúp bạn kết nối ứng dụng qua 3 lớp:

- **HTTP event API** với chữ ký HMAC và idempotency.
- **Routing relay** theo `event_type`/`app`.
- **Real-time / Durable**: WebSocket cho nhánh realtime và stream/Jetsream cho xử lý bền.

Tài liệu này là track Docusaurus mới cho đội ngũ vận hành và tích hợp.

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
