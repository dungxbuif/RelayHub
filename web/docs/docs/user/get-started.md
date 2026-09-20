---
title: User Onboarding
description: Bắt đầu vận hành RelayHub cho ứng dụng của bạn.
---

# User Onboarding

## Bước nhanh 5 phút

1. Đảm bảo API + Worker chạy (PostgreSQL + NATS) và `/readyz` = 200.
2. Vào **Control Panel** để tạo app producer / consumer.
3. Thiết lập `delivery_mode` và `callback_url` nếu cần.
4. Đăng ký rule routing theo `event_type`.
5. Tạo event test và theo dõi realtime.

## Kiểm tra nhanh

- `POST /api/v1/apps` để tạo app.
- `POST /api/v1/routing/rules` để nối event → target app.
- `POST /api/v1/events` để publish signed event.
- `GET /docs` / `GET /docs/developer/getting-started.md` để đọc reference chi tiết.

## Khi nào dùng stream thay cho websocket

Websocket chỉ dùng cho **notifies** / realtime. Nếu cần xử lý bền, luôn dùng callback/stream/job acknowledgement.
