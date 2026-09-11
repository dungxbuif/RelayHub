# Flow đăng ký ứng dụng và socket

## 1) Tạo app

1. Developer gọi `POST /api/v1/apps` với thông tin callback domain.
2. RelayHub trả về `app_id`, `secret`, `public_signing_key`.
3. Lưu vào KV/secret manager của service bạn.

## 2) Kết nối realtime

1. Tạo client websocket theo SDK chuẩn của hệ thống.
2. Authenticate bằng token có scope `events:read events:stream`.
3. Đăng ký topic `app:{app_id}:jobs`.
4. Lắng nghe event theo schema `event.envelope`.

## 3) Nhận event async

- Khi provider gửi event nhưng app target chưa online, event vào queue.
- App pull/subscribe job khi reconnect.

## 4) Idempotency

- Mọi payload đều có `event_id`.
- Server side phải chấp nhận `event_id` trùng nhưng không xử lý lại side-effect.
