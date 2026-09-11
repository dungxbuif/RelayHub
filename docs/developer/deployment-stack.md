# Deployment Stack (RelayHub Docs + API)

## Mục tiêu

RelayHub API là một Go binary tự phục vụ API, metrics và tài liệu đã được nhúng lúc build. Reverse proxy chỉ cần chuyển toàn bộ origin tới API; không cần mount hoặc chạy một docs server riêng.

## Topology

- `relayhub-api`: phục vụ HTTP API, `/metrics`, `/docs/*` và WebSocket RFC 6455 tại `/ws`.
- `relayhub-worker`: xử lý signed HTTP callbacks, retry scheduler và dead-letter.
- `relayhub-redis`: lưu state, queue và Pub/Sub; chỉ truy cập trong network nội bộ.

API và worker dùng chung image từ `Dockerfile`: `relayhub api` và `relayhub worker`; không có command mặc định chạy API. Command không hợp lệ trả usage và exit nonzero.

File `public-docs/deploy/docker-compose.relayhub.yml` chạy đủ ba service API, worker và Redis với persistent AOF volume. Chỉ API publish port; worker cần outbound access tới callback URL. Service names chỉ là lựa chọn của deployment, không phải public contract.

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
| `RELAYHUB_SHUTDOWN_TIMEOUT` | `10s` | Grace period cho API hoặc active worker calls. |

Các duration phải lớn hơn 0. Allowed origins được trim, bỏ phần tử rỗng và deduplicate trong khi giữ thứ tự.

## Operations routes

- `GET /healthz` trả `200` khi process còn phục vụ; route này không truy cập Redis.
- `GET /readyz` ping Redis, trả `200` khi kết nối được và `503` khi không kết nối được.
- `GET /metrics` trả Prometheus text exposition.
- `GET /docs` redirect tới `/docs/`; `/docs/*` phục vụ `public-docs` được nhúng trong binary, gồm HTML, Markdown, `llms.txt` và static descendants với content type phù hợp.
- Docs routes chỉ chấp nhận `GET`; method khác và file không tồn tại trả JSON error envelope chuẩn.
- Route không tồn tại trả JSON error envelope chuẩn của RelayHub.

API giới hạn request body ở 1 MiB, gắn request ID, recover panic và graceful shutdown khi nhận `SIGINT` hoặc `SIGTERM`.

## Đồng bộ embedded docs

`web/embed.go` là snapshot compile-time của toàn bộ `public-docs`. Sau khi sửa public docs, chạy:

```bash
go generate ./web
go test ./web
```

Generator sắp xếp path và format output để cùng một docs tree luôn tạo cùng một source file. Parity test so sánh cả generated source và nội dung `web.Public` với `public-docs`; Docker build cũng chạy gate này trước khi build binary.

## Chạy local

```bash
export RELAYHUB_ADMIN_TOKEN='replace-me'
export RELAYHUB_SIGNING_SECRET='replace-me-too'
go run ./cmd/relayhub
```

## Verification plan

```bash
go test ./internal/config ./internal/httpapi -v
go test ./web -v
go test ./...
go vet ./...
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
curl http://localhost:8080/metrics
curl -L http://localhost:8080/docs/
curl http://localhost:8080/docs/llms.txt
```

## Callback worker (Task 5)

| Variable | Default | Behavior |
| --- | --- | --- |
| `RELAYHUB_REDIS_KEY_PREFIX` | `relayhub` | Applies to durable records, callback stream/group, retries, leases and Pub/Sub. |
| `RELAYHUB_CALLBACK_TIMEOUT` | `10s` | Full outbound attempt deadline. |
| `RELAYHUB_WORKER_CONCURRENCY` | `8` | 1–1024 active callback slots. |
| `RELAYHUB_WORKER_RECLAIM_IDLE` | `30s` | Must be at least callback timeout + 5 seconds. |
| `RELAYHUB_ALLOW_INSECURE_CALLBACKS` | `false` | Enable HTTP callbacks only for local development. |

Worker SIGTERM stops claims and grants active attempts the shutdown timeout before cancellation. Keep Compose `stop_grace_period` above that timeout. Both commands currently load required admin and server signing configuration. Multiple worker replicas coordinate through fenced Redis leases and generations. All internal key names and deployment service names remain configurable/private.

See [reliability](./reliability.md) for `max_retries=5`, `max_attempts=6`, retry table, outbound signature verification and crash recovery. The public [deployment guide](../../public-docs/deploy/README.md) is the runnable human/agent-readable setup surface. Task 5 verification includes real Redis plus HTTP runtime success-after-retry and terminal 400 cases, Docker build and Compose smoke.

### Worker operations listener decision

Task 5 adds `RELAYHUB_WORKER_HTTP_ADDR` (default `:9090`) for private `GET /healthz`, Redis-backed `GET /readyz`, and `GET /metrics`. No application API or docs routes are served by the worker. Compose does not publish or expose this port externally. Scrape `http://relayhub-worker:9090/metrics` from the deployment network; the service hostname is a local Compose choice. Worker operations and active callbacks shut down gracefully together. Binary integration tests verify these routes, the callback outcome metric and SIGTERM behavior.

## Contract and documentation build

The shipped console uses canonical Markdown with stable OpenAPI, JSON Schema,
llms indexes and a reproducible Skill archive. `go generate ./web` rebuilds the
Skill and llms artifacts before embedding. Docker validates all sources/parity
before compiling. `/docs` returns 308; `.zip` resources use `application/zip` and
attachment metadata. See [contract maintenance](contracts.md) for check commands,
fixtures, real API/Redis smoke and the future generator migration decision.
The public [deployment guide](../../public-docs/deploy/README.md) now includes the
complete API configuration table, Compose override behavior and restore procedure.
