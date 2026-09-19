---
title: Admin Control Panel
description: Đăng nhập và vận hành RelayHub qua React Admin nhúng.
---

# Admin Control Panel

React Admin được phục vụ tại `/admin/` từ chính RelayHub API. Overview hiển thị dữ
liệu thật của toàn cluster: request rate, HTTP status, event/delivery outcomes,
trạng thái NATS, số WebSocket đang hoạt động, trạng thái delivery bền vững và
latency p50/p95/p99 lấy từ timestamp PostgreSQL. Người vận hành có thể pause/resume
chu kỳ refresh 5 giây; request refresh không chạy chồng lên request còn dở.

Events, Dead Letters và Audit Logs hỗ trợ filter trên URL và phân trang bằng cursor
opaque. Danh sách không trả payload, callback URL, credential hay header nhạy cảm.
Event detail hiển thị timeline bền vững theo delivery generation và attempt. Dead
Letters cho phép chọn tối đa 100 ID cụ thể, xác nhận danh sách chính xác rồi replay
single/batch. UI giữ cùng idempotency key khi retry request không chắc chắn và vô
hiệu hóa submit trùng khi request đang chạy. Apps, Routing Rules, Realtime Studio
và System vẫn hiển thị boundary trung thực cho đến phase triển khai tương ứng.

Replay chỉ hợp lệ khi toàn bộ selection đang ở `dead_letter`. Thao tác tăng
generation của delivery hiện có, xóa lease/dispatch state thuộc generation cũ và
tạo lại durable wake-up; không tạo event mới, không xóa attempt cũ. Callback hoặc
stream receipt từ generation cũ bị từ chối. Một idempotency key được giữ 24 giờ,
không thể gắn lại với selection khác, và mỗi delivery thành công có audit record.

## Nguồn dữ liệu và degraded state

Redis giữ rolling series tối đa 25 giờ và heartbeat ngắn hạn của từng API replica.
PostgreSQL là nguồn sự thật cho pending/retrying/dead-letter, oldest pending và
delivery latency. Khi Redis lỗi, Overview vẫn trả dữ liệu bền vững nhưng hiển thị
`rolling_metrics` hoặc `instances` là degraded. Khi PostgreSQL lỗi, read model bền
vững không được thay bằng số 0 giả.

Các window/step được hỗ trợ: `5m/1m`, `15m/1m`, `1h/1m`, `1h/5m`, `6h/5m`,
`6h/15m`, `24h/15m`, `24h/1h`. Cursor gắn với filter hiện tại, không được chỉnh sửa
hay dùng lại sau khi đổi filter; giới hạn mỗi trang là 1–100, mặc định 25.

## Đăng nhập

1. Mở `/admin/` qua HTTPS.
2. Nhập bootstrap token được cấu hình bằng `RELAYHUB_ADMIN_TOKEN`.
3. RelayHub đổi token thành cookie phiên có thể thu hồi trên toàn cluster.

Bootstrap token không được ghi vào URL, DOM sau submit, local storage hay session
storage. Cookie `__Host-relayhub_admin` là `Secure`, `HttpOnly`,
`SameSite=Strict`, `Path=/` và không có `Domain`. CSRF token gắn với phiên chỉ nằm
trong bộ nhớ của tab.

Phiên hết hạn sau 30 phút không hoạt động hoặc tối đa 12 giờ. Vì CSRF không được
lưu lâu dài, reload/deep-link mới sẽ yêu cầu đăng nhập lại; URL deep-link vẫn được
giữ nguyên. Logout thu hồi bản ghi Redis, do đó cookie cũ không dùng lại được trên
replica khác.

## Routing khi triển khai

Proxy bên ngoài cần chuyển toàn bộ `/admin/*` về RelayHub API và giữ nguyên HTTPS.
Các đường dẫn extensionless dùng SPA fallback; asset hoặc file không tồn tại vẫn
trả `404`. Public docs Docusaurus là artifact riêng, không được nhúng vào binary;
việc route `/docs/*` sẽ được cấu hình độc lập bởi người vận hành.
