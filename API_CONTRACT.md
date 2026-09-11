# RelayHub API contract v0.1

Planning baseline, 2026-09-10. Đây là hợp đồng để implementation, chưa phải API đang chạy.

## Địa chỉ và credentials

- Base URL: `https://relayhub.dungxbuif.com/api/v1`.
- WebSocket: `wss://relayhub.dungxbuif.com/connection/websocket`.
- Backend/worker dùng `Authorization: Bearer <key>`; browser chỉ giữ token realtime ngắn hạn.
- Mọi key gắn đúng một project. Project lấy từ key, không lấy từ body.
- Resource ID thuộc project khác trả 404; thiếu key trả 401; thiếu scope trả 403.
- Admin API dưới `/api/v1/admin` chỉ qua Tailnet và admin session cookie HttpOnly/Secure/SameSite=Strict; mutation kiểm tra CSRF. Không dùng project key làm admin.

## Scopes

| Credential | Scopes mặc định |
|---|---|
| Backend | `jobs:enqueue`, `jobs:read`, `realtime:publish`, `realtime:grant` |
| Worker | `workers:claim`, `attempts:update`; allowlist queue; không cấp grant |
| Operator project | `jobs:read`, `jobs:replay` |

Worker publish progress qua attempt-scoped endpoint, channel do backend gắn khi enqueue. Worker không được tự chọn channel khác hoặc dùng realtime publish toàn project.

## Provisioning

| Method/path (tương đối base URL) | Request | Response |
|---|---|---|
| `POST /admin/projects` | `{name, allowedOrigins}` | `201 {id,name}` |
| `POST /admin/projects/{id}/queues` | `{name,maxAttempts,leaseSeconds}` | `201 {id,name}` |
| `POST /admin/projects/{id}/channel-policies` | `{pattern,clientPublish:false,requireGrant:true}` | `201 {id}` |
| `POST /admin/projects/{id}/keys` | `{name,scopes,queues?}` | `201 {id,key}`; key chỉ hiện một lần |
| `DELETE /admin/projects/{id}/keys/{keyId}` | Không body | `204`; revoke |

Queue name: `[a-z][a-z0-9._-]{0,63}`. Channel logical dùng chữ/số, `/`, `_`, `-`, tối đa 128 ký tự; không cho wildcard từ client. Pattern ví dụ `jobs/{jobId}` do admin khai báo. Server map project và channel sang tên nội bộ, không ghép chuỗi không kiểm soát vào NATS subject.

## Realtime

| Endpoint | Request | Response |
|---|---|---|
| `POST /realtime/sessions` | `{userId}` | `201 {url,token,expiresAt}` |
| `POST /realtime/grants` | `{userId,channel}` | `201 {channel,wireChannel,token,expiresAt}` |
| `POST /realtime/publish` | `{channel,eventId,type,data}` | `202 {eventId}` sau Centrifugo chấp nhận |

Connection và subscription token TTL 5 phút. Principal nội bộ gồm project + user; grant buộc cùng principal và đúng channel. Backend app kiểm tra quyền nghiệp vụ trước khi gọi grant API. User ID trong một project không đồng nhất với cùng chuỗi user ID ở project khác.

Publish accepted không có nghĩa browser đã nhận/đọc. Không bảo đảm offline delivery ở tuyến realtime tức thời. Frontend tự refresh session/grant qua backend app; grant refresh phải kiểm tra quyền lại. Revoke API key chặn request mới; JWT đã cấp còn hiệu lực đến expiry, trừ khi có lệnh disconnect rõ ràng. MVP không hứa revoke tức thì mọi JWT.

## Jobs

### Enqueue

`POST /jobs`, header `Idempotency-Key` bắt buộc (1–128 ký tự).

```json
{
  "queue": "extract-text",
  "handlerVersion": "v1",
  "data": {"appJobId": "123", "fileId": "file_456"},
  "progressChannel": "jobs/123"
}
```

Trả `202 {jobId,status:"accepted"}` sau transaction ledger + outbox commit. Cùng project/queue/key và cùng body trả cùng job ID; khác body trả `409 IDEMPOTENCY_CONFLICT`. Body hash dùng canonical JSON. Key tồn tại suốt thời gian job nonterminal và ít nhất 7 ngày sau terminal; ngoài thời hạn này key có thể tạo job mới.

State machine:

```text
accepted → queued → running → succeeded
                      └──→ retry_wait → queued
                      └──→ failed
accepted / queued / retry_wait → failed (max job age)
```

`queued` nghĩa outbox đã publish, không có nghĩa worker đã nhận. Nếu worker claim nhanh trước dispatcher cập nhật queued, không được ghi đè running thành queued. Retry do lỗi retryable hoặc lease hết hạn; terminal state không lùi.

`GET /jobs/{id}` trả `{jobId,status,attemptCount,createdAt,updatedAt,result?,error?}`. `GET /jobs?cursor=&limit=` phân trang, limit mặc định 50 và tối đa 100. Result chứa dữ liệu nhỏ/tham chiếu, không chứa file.

`POST /jobs/{id}/replay` yêu cầu scope replay và Idempotency-Key, chỉ job failed; tạo job ID mới có `replayOf`, giữ job cũ và audit. Không reset attempt ledger cũ. Cancel và ưu tiên job ngoài MVP.

### Worker protocol

| Endpoint | Hành vi |
|---|---|
| `POST /workers/claim` | `{queue,handlerVersion,workerId,waitSeconds:20}` → `200 {job,attemptId,leaseToken,leaseExpiresAt}` hoặc `204` |
| `POST /attempts/{id}/heartbeat` | `{leaseToken}` → deadline mới; gia hạn tối đa run deadline |
| `POST /attempts/{id}/progress` | `{leaseToken,eventId,data}` → phát vào progressChannel của job |
| `POST /attempts/{id}/complete` | `{leaseToken,result}` → terminal transaction, sau đó broker ACK |
| `POST /attempts/{id}/fail` | `{leaseToken,error:{code,message,retryable}}` → retry_wait hoặc failed |

Lease mặc định 60 giây; heartbeat mỗi 20 giây, có jitter. Mỗi claim lấy một job; SDK mở tối đa concurrency claim slots. `workerId` là nhãn quan sát, credentials mới là nguồn quyền. Queue/handlerVersion không khớp không được giao.

Lease lưu hash trong DB; API instance bất kỳ kiểm tra được. Lease hết hạn hoặc attempt đã bị thay thế trả `409 LEASE_LOST`. Complete retry của attempt đã succeeded với cùng lease và result hash trả 200; payload khác trả 409. Complete ACK bị thất lạc không chạy lại handler đã succeeded: gateway nhận broker redelivery thì ACK từ ledger terminal.

## Lỗi và giới hạn

```json
{"error":{"code":"QUOTA_EXCEEDED","message":"Project backlog limit reached","retryable":true},"requestId":"req_123"}
```

HTTP: 400 invalid input, 401 auth, 403 scope, 404 not found, 409 conflict/lease lost, 413 payload, 429 quota/rate có Retry-After, 503 dependency unavailable. Không trả 2xx khi ghi bền thất bại. SDK retry network/429/503 có jitter và dùng lại idempotency key; không tự retry lỗi quyền hoặc body conflict.

API path ổn định sau v1 release; thêm field tương thích được phép. Client bỏ qua unknown fields, xử lý unknown enum bằng fallback. OpenAPI là artifact sẽ được tạo từ contract trong task Foundation.

## Runtime và compatibility

Contract này là target. API thực tế hiện tại xem [runtime OpenAPI](src/api/openapi.json); bootstrap chưa xử lý queue hoặc realtime. Centrifugo là engine baseline: SDK Centrifugo dùng được khi Task 4 hoàn thành; raw WebSocket phải nói protocol Centrifugo. Socket.IO không tương thích trực tiếp. SDK RelayHub là tiện ích tùy chọn, không là điều kiện bắt buộc để mở transport.

## Implementation planning amendments v0.2

Admin session/read/list/attempt endpoints cho dashboard được thiết kế tại [ENGINEERING_DETAILS](planning/ENGINEERING_DETAILS.md). Chúng chưa được triển khai. Admin routes chạy trên listener riêng, không mount vào public API router.

Grant trả `wireChannel` để client Centrifugo trực tiếp biết tên subscribe sau project mapping; `channel` là tên logical app đã yêu cầu. SDK helper có thể che mapping nhưng raw/native SDK guide phải thể hiện rõ.

Queue policy snapshot và hash body idempotency phải ổn định giữa process; lease/run deadline không vượt max job age. Xem engineering contract để review transaction và recovery trước implementation.

JSON input planning rule: reject duplicate object keys, invalid UTF-8 and integer values outside ±(2^53−1) with HTTP 400. Encode larger exact integers as strings. Supported JSON is canonicalized using RFC 8785 for request hashes; this rule is a target for J1, not implemented in bootstrap.
