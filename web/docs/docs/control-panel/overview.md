---
title: Control Panel UI (track)
description: Luồng vận hành qua console tích hợp.
---

# Control Panel UI

Track này mô tả cách vận hành trên UI:

1. **Connect**: đặt `Base URL`, `Admin token`.
2. **Apps**: tạo app, xem danh sách, lấy `api_key` và `hmac_secret`.
3. **Routing**: tạo rule `source_app_id` → `event_type` → `target_app_id`.
4. **Events**: publish event có idempotency.
5. **Realtime**: tạo token, subscribe channel, publish test message.

Console hiện tại là **v1 local console** và gọi trực tiếp cùng API origin, lưu cấu hình ở browser storage tạm thời.

### Ưu điểm của v1 console

- Deploy thấp, không cần server thêm.
- Bao gồm đủ luồng vận hành cốt lõi.
- Dùng được ngay cho smoke test + staging.

### Hạn chế

- Không có RBAC chi tiết theo vai trò.
- Không có multi-tenant UI.
- Không có lịch sử audit chuyên sâu theo giao diện.

Các mục này sẽ là track mở rộng khi nâng cấp Control Panel.
