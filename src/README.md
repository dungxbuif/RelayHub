# RelayHub source status

Implementation phase đang chạy được: provider dùng bộ nhớ trong (in-memory) cho job queue + attempt lifecycle + realtime mock APIs.

## Chạy local

Yêu cầu Go 1.26+, hiện test chạy tốt trên Go 1.26.3 darwin/arm64.

```sh
cd /path/to/RelayHub/src
cp .env.example .env  # nếu bạn muốn dùng env mặc định của shell
rtk go run ./cmd/relayhub
```

Mặc định bind `127.0.0.1:8080`. Đổi bằng biến môi trường `RELAYHUB_ADDR`.

Mặc định token mẫu (có thể override bằng env):

```sh
RELAYHUB_BACKEND_TOKEN=rh_backend_demo_token
RELAYHUB_WORKER_TOKEN=rh_worker_demo_token
RELAYHUB_REALTIME_TOKEN=rh_realtime_demo_token
```

Server hỗ trợ shutdown nhẹ: `SIGINT`/`SIGTERM` timeout 10 giây.

```sh
rtk curl -i http://127.0.0.1:8080/healthz
rtk curl -i http://127.0.0.1:8080/readyz
```

## API runtime đang chạy

- `GET /healthz` => 200 JSON
- `GET /readyz` => 200 JSON (runtime in-memory ready)
- `POST /api/v1/jobs` => enqueue job (yêu cầu Authorization + Idempotency-Key)
- `GET /api/v1/jobs/{id}` => xem trạng thái job
- `POST /api/v1/workers/claim` => worker claim job
- `POST /api/v1/attempts/{id}/heartbeat` => gia hạn lease
- `POST /api/v1/attempts/{id}/progress` => push progress event stub
- `POST /api/v1/attempts/{id}/complete` => complete job
- `POST /api/v1/attempts/{id}/fail` => fail job
- `POST /api/v1/realtime/sessions` => session token stub
- `POST /api/v1/realtime/grants` => grant token stub (`wireChannel`)
- `POST /api/v1/realtime/publish` => publish event stub
- `GET /connection/websocket` => 501 (runtime này chưa embed socket server)

Còn thiếu cho MVP đầy đủ: PostgreSQL ledger/outbox, NATS/JetStream, Centrifugo thật, admin portal, durable offline storage.

## Kiểm thử

```sh
cd /path/to/RelayHub/src
rtk go test ./...
rtk go test -race ./...
rtk go vet ./...
rtk go build -o /tmp/relayhub ./cmd/relayhub
rtk proxy python3 scripts/smoke.py /tmp/relayhub
```

## Source map

- `cmd/relayhub/main.go`: listener, timeout, graceful shutdown
- `internal/config`: cấu hình addr + token mẫu
- `internal/httpapi`: health/readiness + runtime API in-memory
- `api/openapi.json`: đang được cập nhật theo runtime hiện tại
- `scripts/smoke.py`: smoke cho binary chạy thật và SIGTERM
