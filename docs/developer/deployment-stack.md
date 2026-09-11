# Deployment Stack (RelayHub Docs + API)

## Mục tiêu

RelayHub API là một Go binary tự phục vụ API, metrics và tài liệu đã được nhúng lúc build. Reverse proxy chỉ cần chuyển toàn bộ origin tới API; không cần mount hoặc chạy một docs server riêng.

## Topology

- `relayhub-api`: phục vụ HTTP API, `/metrics`, `/docs/*` và sau này là WebSocket.
- `relayhub-worker`: xử lý nền cho queue và retry trong các phase sau.
- `relayhub-redis`: lưu state, queue và Pub/Sub; chỉ truy cập trong network nội bộ.

Task 1 tạo image dùng chung từ `Dockerfile`. Entrypoint mặc định chạy API bằng `/relayhub`; worker command sẽ được bổ sung khi worker được triển khai.

## Runtime configuration

Tất cả biến môi trường có prefix `RELAYHUB_`:

| Variable | Default | Meaning |
| --- | --- | --- |
| `RELAYHUB_HTTP_ADDR` | `:8080` | Địa chỉ HTTP listen. |
| `RELAYHUB_REDIS_URL` | `redis://localhost:6379/0` | Redis URL; chỉ chấp nhận scheme `redis` hoặc `rediss`. |
| `RELAYHUB_ADMIN_TOKEN` | required | Admin bearer token. Giá trị không bao giờ xuất hiện trong lỗi startup. |
| `RELAYHUB_SIGNING_SECRET` | required | Server signing secret. Giá trị không bao giờ xuất hiện trong lỗi startup. |
| `RELAYHUB_ALLOWED_ORIGINS` | empty | Danh sách origin phân tách bằng dấu phẩy. Wildcard `*` bị từ chối. |
| `RELAYHUB_EVENT_RETENTION` | `168h` | Thời gian giữ event. |
| `RELAYHUB_JOB_RETENTION` | `168h` | Thời gian giữ terminal job. |
| `RELAYHUB_IDEMPOTENCY_RETENTION` | `24h` | Thời gian giữ idempotency record. |
| `RELAYHUB_SIGNING_SKEW` | `5m` | Sai lệch tối đa của signing timestamp. |
| `RELAYHUB_SHUTDOWN_TIMEOUT` | `10s` | Thời gian tối đa để HTTP server graceful shutdown. |

Các duration phải lớn hơn 0. Allowed origins được trim, bỏ phần tử rỗng và deduplicate trong khi giữ thứ tự.

## Operations routes

- `GET /healthz` trả `200` khi process còn phục vụ; route này không truy cập Redis.
- `GET /readyz` ping Redis, trả `200` khi kết nối được và `503` khi không kết nối được.
- `GET /metrics` trả Prometheus text exposition.
- `GET /docs` redirect tới `/docs/`; `/docs/*` phục vụ `public-docs` được nhúng trong binary, gồm HTML, Markdown, `llms.txt` và static descendants với content type phù hợp.
- Route không tồn tại trả JSON error envelope chuẩn của RelayHub.

API giới hạn request body ở 1 MiB, gắn request ID, recover panic và graceful shutdown khi nhận `SIGINT` hoặc `SIGTERM`.

## Chạy local

```bash
export RELAYHUB_ADMIN_TOKEN='replace-me'
export RELAYHUB_SIGNING_SECRET='replace-me-too'
go run ./cmd/relayhub
```

## Verification plan

```bash
go test ./internal/config ./internal/httpapi -v
go test ./...
go vet ./...
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
curl http://localhost:8080/metrics
curl -L http://localhost:8080/docs/
curl http://localhost:8080/docs/llms.txt
```
