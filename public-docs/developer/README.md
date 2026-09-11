# Developer Integration Docs

RelayHub phục vụ tài liệu tích hợp và API trên cùng một origin. Trang này và các file Markdown/`llms.txt` được nhúng vào API binary, vì vậy client và AI agent có thể đọc cùng một tài liệu tại `/docs/` mà không cần docs server riêng.

## Chạy RelayHub API

Bạn cần Go 1.24 trở lên và một Redis instance:

```bash
export RELAYHUB_ADMIN_TOKEN='replace-me'
export RELAYHUB_SIGNING_SECRET='replace-me-too'
export RELAYHUB_REDIS_URL='redis://localhost:6379/0'
go run ./cmd/relayhub
```

API mặc định listen tại `:8080`. Hai secret là bắt buộc; lỗi startup chỉ nêu tên biến bị thiếu hoặc không hợp lệ và không in giá trị secret.

## Operations và docs

| Route | Behavior |
| --- | --- |
| `GET /healthz` | Process liveness, không phụ thuộc Redis. |
| `GET /readyz` | Redis readiness; trả `503` khi Redis không reachable. |
| `GET /metrics` | Prometheus text metrics. |
| `GET /docs` | Redirect tới `/docs/`. |
| `GET /docs/*` | Embedded HTML, Markdown, `llms.txt` và static resources. |

Unknown routes trả JSON:

```json
{"error":{"code":"not_found","message":"The requested resource was not found."}}
```

Request body tối đa 1 MiB. API graceful shutdown khi nhận `SIGINT` hoặc `SIGTERM`.

Docs routes chỉ chấp nhận `GET`. Method khác trả JSON `method_not_allowed`; file docs không tồn tại trả JSON `not_found` thay vì plain text.

## Runtime variables

| Variable | Default |
| --- | --- |
| `RELAYHUB_HTTP_ADDR` | `:8080` |
| `RELAYHUB_REDIS_URL` | `redis://localhost:6379/0` |
| `RELAYHUB_ADMIN_TOKEN` | required |
| `RELAYHUB_SIGNING_SECRET` | required |
| `RELAYHUB_ALLOWED_ORIGINS` | empty; comma-separated; wildcard rejected |
| `RELAYHUB_EVENT_RETENTION` | `168h` |
| `RELAYHUB_JOB_RETENTION` | `168h` |
| `RELAYHUB_IDEMPOTENCY_RETENTION` | `24h` |
| `RELAYHUB_SIGNING_SKEW` | `5m` |
| `RELAYHUB_SHUTDOWN_TIMEOUT` | `10s` |

Khi build từ source sau khi sửa `public-docs`, chạy `go generate ./web` để cập nhật snapshot trong binary. `go test ./web` và Docker build đều từ chối snapshot bị lệch.

## Tài liệu tích hợp

- [API reference](./api-overview.md)
- [Flow tích hợp đăng ký](./registration-flow.md)
- [Auth & Signature](./auth.md)
- [Retry / DLQ](./reliability.md)
- [Skills Resources](./skills.md)
- AI index: [`/docs/llms.txt`](../llms.txt) và [`/docs/llms-full.txt`](../llms-full.txt)

Các route ứng dụng, event, queue, WebSocket và worker được bổ sung trong các phase tiếp theo; Task 1 chỉ cung cấp executable skeleton và operations surface ở trên.
