# RelayHub — lộ trình v0.1

Ngày: 2026-09-10

## Phase 1 — Provider realtime + queue dùng được

Project isolation, API key scopes, realtime tokens, SDK TypeScript, job queue, worker gateway, retry, log và dashboard tối thiểu. Functions ở mức handler đăng ký trong worker tin cậy. Tích hợp OCR thật và app mẫu thứ hai.

Hoàn thành khi đạt các [tiêu chí MVP](SPEC.md#7-tiêu-chí-nghiệm-thu), gồm mất mạng/restart và cách ly project. Không lấy việc demo publish thành công làm bằng chứng hệ thống đã bền.

## Phase 2 — Webhook và vận hành tin cậy

Webhook ingress/egress, xác thực nguồn, delivery attempts, retry/replay, dead-letter, alert theo backlog, retention/backup/restore được kiểm chứng. Event subscriptions độc lập, trạng thái theo từng đích. Khả năng tiếp tục nhận khi nhà offline được thiết kế từ phase 1 bằng placement trên VPS.

## Phase 3 — Functions và trải nghiệm tích hợp

Function version/deployment, runner nhẹ được quản lý, event/HTTP/schedule triggers, secrets, SDK Python, SDK docs và ví dụ cho app mới. Runtime cho code tùy ý chỉ mở sau khi có thiết kế isolation và resource limits.

## Phase 4 — Provider tự phục vụ

Portal tạo project/channel/queue/function, usage/quota, credential rotation, RBAC và nhiều thành viên. Tunnel UI/agent riêng nếu có nhu cầu. Billing, public signup, HA hoặc multi-region chỉ khi mục tiêu sử dụng đòi hỏi.

## Gates kiểm chứng và lựa chọn phase sau

| Câu hỏi | Hướng hiện tại / việc cần làm |
|---|---|
| VPS còn bao nhiêu CPU/RAM/SSD? | Audit read-only trước sizing; chưa hứa đáp ứng |
| Số app, connection đồng thời, job/ngày, payload? | Benchmark theo tải dự kiến rồi đặt quota |
| Version/image/license dependency? | Ghim release, kiểm tra ARM64 và phạm vi OSS |
| Replay job, idempotency và result retention | Đã định nghĩa baseline trong API_CONTRACT.md và OPERATIONS.md |
| JetStream persistence/retention | Ledger + outbox và bounded dispatch đã chốt; byte limits theo capacity report |
| Durability/RPO/RTO | Single VPS, RPO mục tiêu 24h/RTO 4h khi mất host; cần restore drill |
| Lưu file và kết quả ở đâu? | App giữ dữ liệu nghiệp vụ; object storage ngoài nhà nếu cần nhận lúc nhà offline |
| Public domain và TLS termination | Đã chốt relayhub.dungxbuif.com, TLS tại VPS |
| Dashboard authentication | Tailnet + một admin local, session cookie; public portal phase 4 |
| Function ngôn ngữ đầu tiên? | Handler worker theo app; TypeScript/Go/Python tùy luồng đầu |
| Mở cho người khác hay chỉ app riêng? | API thiết kế như third-party; thương mại hóa chưa quyết định |

## Các lựa chọn chưa dùng

- Convoy: ứng viên adapter webhook sau này, không chọn làm lõi realtime/queue.
- Full Supabase: bộ tích hợp đáng tham khảo; hiện ưu tiên các engine riêng để không buộc app dùng database/auth của provider.
- Redis: chưa cần trong MVP một node Centrifugo; đánh giá khi cần history hoặc scale.
- Kafka/Kubernetes: chưa có nhu cầu chứng minh để tăng chi phí vận hành.

Kế hoạch MVP nằm ở [planning/IMPLEMENTATION_PLAN.md](planning/IMPLEMENTATION_PLAN.md). Chưa cam kết lịch triển khai hoặc SLA.

## Trạng thái

Planning baseline hoàn tất; source bootstrap đã có; các phase provider chưa hoàn thành. Capacity, image compatibility và restore là gate kiểm chứng trong kế hoạch, không phải quyết định sản phẩm chưa chốt.
