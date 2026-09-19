---
title: Admin Control Panel
description: Đăng nhập và vận hành RelayHub qua React Admin nhúng.
---

# Admin Control Panel

React Admin được phục vụ tại `/admin/` từ chính RelayHub API. Bản foundation hiện
cung cấp đăng nhập an toàn, điều hướng responsive và các boundary rõ ràng cho
Overview, Events, Dead Letters, Apps, Routing Rules, Realtime Studio, Audit Logs
và System. Dữ liệu live của từng module sẽ được nối theo các phase tiếp theo;
giao diện không hiển thị số liệu giả.

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
