# Operations baseline và deployment gates

Ngày 2026-09-10. Các con số là default thiết kế để bắt đầu thử nghiệm, không phải capacity đo được hay SLA.

## Defaults MVP

| Tham số | Giá trị ban đầu |
|---|---|
| Job payload/result | Mỗi phần tối đa 64 KiB |
| Realtime payload | 16 KiB |
| Queue max attempts | 5 |
| Retry delay sau lần 1–4 | 5s, 30s, 120s, 600s, jitter ±20% |
| Worker lease / heartbeat | 60s / 20s |
| Max run time / attempt | 30 phút; function dài hơn cần điều chỉnh policy trước enqueue |
| Max job age | 7 ngày; quá hạn ghi failed, giữ audit |
| Max nonterminal/project | 10.000 jobs |
| Concurrency/project | 10 attempts; worker mặc định 2 |
| Token TTL | 5 phút |
| Realtime history | 60 giây hoặc 100 messages/channel, giới hạn nào đến trước |
| Connection/project | 100 trong trial |
| Publish/project | 50 requests/s, burst 100 |
| Enqueue/project | 10 requests/s, burst 20 |
| Terminal jobs/attempts/idempotency | Giữ 7 ngày sau terminal |
| Claim long poll | 20s; proxy timeout ít nhất 30s |

Giới hạn được enforce qua shared DB/engine hoặc bộ điều phối duy nhất của MVP; không dùng per-process counter rồi gọi đó là global quota. Queue config được snapshot vào job lúc enqueue để sửa config không đổi policy của job đang chạy.

Idempotency mapping không xóa khi job nonterminal. Progress lỗi không được làm thất bại nghiệp vụ OCR mặc định; SDK coalesce progress và giữ last state tại app. Realtime channel có history được tạo theo policy, không bật lưu history vô hạn.

## Triển khai

Một VPS chạy Compose cho API, dispatcher, PostgreSQL, NATS JetStream, Centrifugo và dashboard; worker chạy ở Mac mini. Persist PostgreSQL và NATS bằng volume riêng, secrets ngoài Git. Chỉ edge bind public 443; DB/broker/admin engine không publish host port ra Internet.

Nginx hiện có cần route riêng cho `relayhub.dungxbuif.com` đến TLS termination tại VPS. Không đưa provider qua đường tunnel về nhà. Trước thay đổi phải lưu cấu hình edge và xác minh các domain hiện hữu vẫn hoạt động. Domain đang là quyết định thiết kế, chưa tạo DNS/certificate.

Admin bootstrap tạo một tài khoản local, hash password và yêu cầu login; quản trị chỉ nhận trên Tailnet. Không trust forwarded client IP từ nguồn tùy ý để vượt admin restriction. Session tối đa 8 giờ, logout/revoke có server-side invalidation. Không đặt admin password mặc định trong Compose.

## Gates trước release

1. Read-only inventory CPU/RAM/free SSD/VPS architecture, ports và Nginx routes. Lưu số đo không kèm secrets trong `planning/CAPACITY_REPORT.md` khi thực thi.
2. Ghim version/digest của image, verify license và ARM64 cho Mac/Pi; ghi `planning/DEPENDENCY_LOCK.md`. Không dùng `latest`.
3. Xác minh fsync/synchronous_commit PostgreSQL bật. Đặt NATS stream byte limits theo capacity; work dispatch reject-new khi đầy, không làm mất ledger. Outbox retry và cảnh báo disk hoạt động.
4. Chạy capacity trial: 2 project, 100 sockets/project, tổng 20 realtime publish/s, 2 enqueue/s trong 30 phút, payload ≤4 KiB và synthetic worker. Ghi p50/p95/p99, RAM, disk, lỗi; mục tiêu trial p95 publish-to-browser <500ms cùng region và enqueue <500ms, không phải SLA công khai.
5. Fault tests theo kế hoạch; restore vào môi trường tách biệt và kiểm tra jobs/credentials/config.
6. Production ingress smoke trên API/WSS và các domain cũ; có rollback image/migration tương thích trước chuyển traffic.

Nếu trial không đạt, giảm quota hoặc thay placement có ghi nhận; không tuyên bố capacity chưa đo. Không tự dựng thêm cluster để che kết quả.

## Backup và quan sát

Backup PostgreSQL hằng ngày sang nơi độc lập với VPS, giữ 7 bản ngày; Pi5 chỉ là bản phụ vì có thể offline. Backup NATS theo stream snapshot nếu dùng event history; MVP có thể tái tạo dispatch từ ledger PostgreSQL. Restore phải phối hợp terminal ledger để tránh chạy lại job đã xong.

Mục tiêu ban đầu cho mất toàn bộ VPS: RPO tối đa 24 giờ theo backup và RTO mục tiêu 4 giờ sau khi có máy thay thế, chỉ xác nhận sau restore drill. Crash process khác mất ổ đĩa: job commit còn trên disk cần phục hồi được. Chưa có HA/SLA.

Alert: outbox oldest >60s, worker queue chờ >5 phút khi kỳ vọng worker online, disk >80%, backup quá 26 giờ, lỗi auth bất thường và retry exhausted. Healthz không trả credentials/topology. Logs có correlation IDs; payload và token được redact.
