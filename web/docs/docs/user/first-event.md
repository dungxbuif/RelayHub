---
title: First Event End-to-End
description: Hướng dẫn publish event đầu tiên và nhận lại kết quả.
---

# First Event End-to-End

Dùng đúng luồng sau:

1. **Producer app** publish event:
   - ký HMAC với `X-RelayHub-Timestamp` + `X-RelayHub-Signature`.
   - gửi `POST /api/v1/events`.
2. **Routing** tự động chuyển sang target app theo `event_type`.
3. Delivery theo `delivery_mode`:
   - `callback`: gọi HTTP callback endpoint.
   - `queue`: đẩy xuống durable flow.
   - `all`: kết hợp callback + realtime hints.
4. **Observability**: theo dõi job state và lỗi trong bảng điều khiển.

## Mẫu lỗi thường gặp

- `401` / `403` → chữ ký lỗi hoặc app key sai.
- `404` route → event_type chưa có routing.
- `503` hàm RPC → chưa có owner-subscription cho function.

## Chuẩn vận hành

- Idempotency key phải ổn định theo logic nghiệp vụ.
- Callback phải verify chữ ký, xử lý retry/deduplicate.
- Websocket không phải lớp đảm bảo exactly-once.
