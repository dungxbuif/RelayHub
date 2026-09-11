# RelayHub source bootstrap

Source khởi đầu, chưa phải provider MVP hoàn chỉnh. Go API chỉ có liveness và lỗi JSON; chưa có PostgreSQL, NATS, Centrifugo, auth, SDK hay dashboard chạy thật.

## Chạy local

Yêu cầu Go 1.26+, bản đã kiểm thử: Go 1.26.3 darwin/arm64. Module dùng standard library, không cần Docker hoặc external dependency ở bước này.

```sh
cd /path/to/RelayHub/src
rtk go run ./cmd/relayhub
```

Mặc định bind `127.0.0.1:8080`. Đổi bằng biến môi trường `RELAYHUB_ADDR`; `.env.example` chỉ là tham khảo, binary không tự đọc .env. Đặt port 0 để hệ điều hành chọn port trống. SIGINT/SIGTERM dừng server graceful trong 10 giây.

```sh
rtk curl -i http://127.0.0.1:8080/healthz
rtk curl -i http://127.0.0.1:8080/readyz
rtk go test -race ./...
rtk go vet ./...
rtk go build -o /tmp/relayhub-bootstrap ./cmd/relayhub
rtk proxy python3 scripts/smoke.py /tmp/relayhub-bootstrap
```

## API thực tế

| Route | Kết quả |
|---|---|
| GET /healthz | 200 JSON, tiến trình hoạt động |
| GET /readyz | 503 NOT_READY, dependencies chưa triển khai |
| /api/v1/jobs, /api/v1/workers/claim, /api/v1/realtime/sessions, /api/v1/realtime/grants, /api/v1/realtime/publish | 501 NOT_IMPLEMENTED |
| /connection/websocket | 501; chưa hỗ trợ Upgrade |
| Admin, hooks, route khác, / | 404 JSON |

GET là method duy nhất cho health/readiness; method khác trả 405. Routes 501 là sentinel cho mọi method, chưa thực hiện validation hoặc authentication. Không dùng source bootstrap làm public provider. Readiness 503 là có chủ đích, không nên sửa thành 200 để vượt deployment gate.

Mỗi response có X-Request-ID mới, lỗi có requestId tương ứng; server không tin request ID từ client. Không log body, query hay auth header.

[OpenAPI runtime](api/openapi.json) chỉ mô tả bootstrap. [Target API contract](../API_CONTRACT.md) mô tả sản phẩm sẽ triển khai, không phải runtime hiện có.

## Source map

- `cmd/relayhub/main.go`: listener, timeouts, graceful shutdown.
- `internal/config`: cấu hình bind address.
- `internal/httpapi`: health và lỗi JSON, test route boundary.
- `scripts/smoke.py`: HTTP smoke trên binary thật và kiểm tra SIGTERM.
- `api/openapi.json`: contract của runtime bootstrap.

Modules tiếp theo theo [implementation plan](../planning/IMPLEMENTATION_PLAN.md): projects/auth → ledger/outbox → leases/retry → realtime → SDK/dashboard/OCR → deployment verification. Không tạo package rỗng hoặc database migration chưa kiểm chứng.
