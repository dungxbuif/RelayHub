# User Guide — Bắt đầu nhanh

RelayHub là nền tảng third-party provider nhận event từ các client, hỗ trợ cả luồng realtime và queue.

## Bạn cần gì để bắt đầu

- Một domain: `relayhub.dungxbuif.com` (ví dụ theo bản kế hoạch).
- API key dành cho workspace/tenant.
- Kiến thức cơ bản về endpoint/URL callback.

## Cách dùng nhanh

1. Mở tab **/docs/user**.
2. Tạo workspace trong dashboard nội bộ.
3. Tạo app relay trong khu vực `Connections`.
4. Lấy `tenant_key` + `app_id`.
5. Cấu hình destination cho endpoint của bạn.

## Hai cơ chế giao nhận sự kiện

- **Realtime Tunnel**: app đích cần online, nhận request/response tức thì.
- **Persistent Relay Queue**: app đích tắt tạm thời vẫn nhận event và retry theo backoff, xử lý sau khi lên lại.

## Các thao tác phổ biến

- Kiểm tra health của đường truyền.
- Xem dead-letter cho event thất bại.
- Retry thủ công theo nhóm event.
