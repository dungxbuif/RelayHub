# RelayHub — đặc tả sản phẩm v0.1

Ngày: 2026-09-10 · Trạng thái: Planning baseline · Chưa triển khai

## 1. Mục tiêu

Cung cấp một dịch vụ độc lập để mọi app sử dụng realtime, hàng đợi công việc, functions và webhook qua API/SDK. App không phải tự vận hành WebSocket server, broker, cơ chế phân phối và retry riêng.

Người dùng đầu tiên là chủ homelab cùng các app bên ngoài sử dụng chung provider. Thiết kế giao diện tích hợp như third-party provider; chưa mặc định mở dịch vụ thương mại cho bên ngoài.

## 2. Ranh giới trách nhiệm

| Provider xử lý | App vẫn xử lý |
|---|---|
| WebSocket connection, heartbeat, reconnect, phân phối channel | Quyền nghiệp vụ: user được xem tài nguyên nào; hiển thị UI |
| Lưu job, giao cho worker, ACK, retry, trạng thái lỗi | Handler nghiệp vụ và chống tác dụng phụ trùng |
| Runtime/runner, timeout, concurrency, log | Code function và dữ liệu nghiệp vụ |
| Webhook delivery, lịch sử, replay | Nguồn/đích hợp lệ, xác thực theo từng nhà cung cấp |
| Project isolation, credentials, quota | Bảo vệ credentials, tích hợp SDK và xử lý lỗi API |

Database nghiệp vụ của app vẫn là nguồn dữ liệu chính. Realtime không thay thế API lấy trạng thái hoặc cơ chế lưu kết quả lâu dài. Khi ghi database và phát event phải nhất quán, app cần transactional outbox hoặc cơ chế tương đương.

## 3. Các năng lực

### Realtime

- Backend phát dữ liệu qua API; frontend subscribe qua WSS bằng SDK.
- Channel thuộc project; token ngắn hạn giới hạn user và channel.
- Ưu tiên backend publish, frontend receive trong MVP.
- Reconnect và recovery trong cửa sổ hữu hạn; khi recovery thất bại client lấy snapshot mới từ app.
- Ví dụ: tiến độ công việc, thông báo hoàn thành, stream phản hồi AI.

### Jobs / Queue

- Enqueue trả job ID sau khi lưu bền được xác nhận.
- Một job được một worker trong nhóm xử lý ở mỗi lần giao; có thể giao lại.
- At-least-once: handler phải chịu được giao trùng; không cam kết exactly-once cho tác dụng phụ.
- Có idempotency key cho enqueue, giới hạn concurrency, heartbeat/lease, retry có backoff, trạng thái failed và replay.
- Worker SDK kết nối outbound qua HTTPS; không bắt buộc truy cập NATS trực tiếp.

### Events

- Một sự kiện có thể có nhiều consumer độc lập: realtime, webhook, function.
- Mỗi consumer có tiến độ riêng; lỗi của một consumer không làm bên đã thành công chạy lại theo chủ ý điều phối.
- Không cam kết thứ tự toàn cục. Phạm vi ordering cần chốt theo use case.

### Functions

- MVP: handler tin cậy, có tên/phiên bản, đóng gói trong worker/container do chủ hệ thống quản lý.
- Sau MVP: triển khai function, HTTP/event/schedule trigger, secrets và runner được quản lý.
- Handler nghiệp vụ do app bên ngoài triển khai trên worker của app; RelayHub không triển khai engine nghiệp vụ.
- Chạy code tùy ý từ dashboard cần thiết kế isolation riêng trước khi hỗ trợ.

### Webhook và tunnel

- Webhook là nguồn sự kiện và đích giao HTTP, có retry/log/replay.
- Raw body và header cần thiết phải được giữ khi xác minh chữ ký; transform là bước tường minh.
- Tunnel HTTP phục vụ request/response trực tiếp, cần đích online; không đưa mọi request tunnel vào durable queue.
- Tunnel UI/agent riêng chưa thuộc MVP; tận dụng Rathole/Tailnet khi thích hợp.

## 4. Mô hình khách hàng và bảo mật

- Project là ranh giới credentials, channel, queue, route và dữ liệu. Một app có thể có project dev/prod riêng.
- MVP chỉ một chủ quản trị, nhưng có ít nhất hai project để kiểm chứng cách ly.
- Backend credentials có scope; frontend không giữ API key hoặc khóa ký.
- Backend app xác thực user và kiểm tra quyền tài nguyên rồi yêu cầu RelayHub cấp token đúng phạm vi.
- RelayHub suy ra project từ credentials; không tin project ID/channel tùy ý trong payload.
- Giới hạn payload, publish rate, kết nối và backlog theo project; giá trị số xác định khi benchmark.
- Billing, tổ chức nhiều thành viên và đăng ký công khai chưa thuộc MVP.

## 5. API/SDK dự kiến

Các tên dưới đây là hợp đồng nháp, chưa có implementation:

```ts
await relay.realtime.publish("jobs/123", { type: "progress", percent: 75 });
const job = await relay.jobs.enqueue("demo.process", { itemId: "item_456" }, {
  idempotencyKey: "demo:item_456:v1",
});
worker.handle("demo.process", async input => processDemo(input.itemId));
realtime.subscribe("jobs/123", event => updateProgress(event));
```

API dùng `/api/v1`; lỗi có code, request ID và khả năng retry. SDK backend và frontend tách quyền. Worker SDK che cơ chế claim/heartbeat/complete; app không biết subject NATS.

## 6. Phạm vi MVP

1. Tạo project và credentials qua CLI/config hoặc API quản trị nội bộ.
2. Realtime channel có quyền, SDK TypeScript và ví dụ frontend.
3. Queue bền, worker SDK, retry, failed-job inspection và replay.
4. Handler chạy trên worker đã đăng ký; không xây arbitrary-code runtime.
5. Dashboard nhỏ cho projects, jobs và attempts.
6. Một app mẫu queue + realtime độc lập và project mẫu thứ hai để thử cách ly. Tích hợp app nghiệp vụ bên ngoài diễn ra sau khi RelayHub hoàn thành, không phải điều kiện nghiệm thu MVP.

Không thuộc MVP: billing, marketplace, Kubernetes, multi-region, HA cluster, workflow DAG, editor function, tunnel tự phục vụ, webhook gateway đầy đủ.

## 7. Tiêu chí nghiệm thu

- App mẫu enqueue job, worker mẫu xử lý, browser nhận tiến độ và lấy được kết quả khi mở lại; không phụ thuộc app nghiệp vụ bên ngoài.
- App thứ hai dùng cùng provider nhưng không đọc/publish/claim dữ liệu của project mẫu đầu tiên.
- Tắt worker hoặc mất mạng nhà: job đã nhận vẫn chờ trên VPS và được xử lý khi worker trở lại.
- Restart API/broker: job đã được xác nhận lưu không biến mất trong phạm vi lưu trữ đã cấu hình; thử cả lỗi giữa publish và trả response.
- Worker chết trước ACK: job được giao lại, handler không tạo tác dụng phụ trùng trong bài thử.
- Token sai, hết hạn hoặc sai channel bị từ chối.
- Backlog/payload vượt quota nhận lỗi rõ ràng, không âm thầm xóa job đang chờ.
- Client mất lịch sử realtime lấy snapshot thay vì hiển thị dữ liệu đầy đủ giả định.
- Có thử restore backup; một VPS đơn không được mô tả là high availability.

## 8. Baseline được chốt

- Domain duy nhất: `relayhub.dungxbuif.com`; TLS kết thúc tại VPS.
- Public API: `https://relayhub.dungxbuif.com/api/v1`; realtime: `wss://relayhub.dungxbuif.com/connection/websocket`.
- Go API/dispatcher + Centrifugo OSS + NATS JetStream + PostgreSQL. Không hỗ trợ nhiều broker trong MVP.
- RelayHub là provider wrapper; app không phụ thuộc giao thức NATS hoặc API quản trị Centrifugo.
- Dashboard `/` chỉ dành cho admin qua Tailnet trong MVP; API/WSS public có xác thực.
- API và SDK là thiết kế chuẩn bị triển khai, chưa có endpoint chạy thật.
- Chi tiết hợp đồng tại [API_CONTRACT.md](API_CONTRACT.md); flow tại [INTEGRATION_FLOWS.md](INTEGRATION_FLOWS.md).
- Default vận hành trong [OPERATIONS.md](OPERATIONS.md) là giới hạn ban đầu do thiết kế chọn, không phải số đo capacity hoặc SLA.

## 9. Documentation as a product requirement

Maintain detailed local docs and separate public developer integration docs. Public site prefers Docusaurus with Markdown/MDX and includes a Skills tab for copyable/downloadable skill packages. Plain Markdown, OpenAPI and agent indexes must be fetchable without browser execution. See [documentation strategy](DOCUMENTATION_STRATEGY.md) for ownership and release acceptance.
