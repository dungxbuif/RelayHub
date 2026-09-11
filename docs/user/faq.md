# FAQ cho User

## Hệ thống còn giữ event khi service đích tắt?
Có, trong cơ chế **Persistent Queue**.

## Muốn gửi nhận kết quả ngay lập tức?
Dùng **Tunnel (Realtime)**.

## Webhook cũ có dùng được nữa không?
Không còn dùng tên "webhook" cho sản phẩm mới. Mình đang hướng tới khung **Relay API** (Inbound/Outbound Events) thống nhất theo event envelope.

## Có cần tự viết websocket server không?
Không bắt buộc. RelayHub cung cấp luồng realtime socket chuẩn cho apps partner tích hợp.
