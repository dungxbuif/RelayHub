# Deploy RelayHub

## Trạng thái Task 1

Task 1 cung cấp API binary và image hiện tại. API phục vụ các operations routes và toàn bộ `/docs` từ snapshot đã nhúng trong binary. Không cần mount `public-docs` hoặc chạy docs server riêng.

Các tính năng application API, WebSocket và worker chưa tồn tại trong phase này. `relayhub-worker` là service thứ ba của topology MVP cuối cùng và chỉ được thêm vào stack chạy thật sau khi worker command được triển khai.

## Build image

```bash
go generate ./web
go test ./web
docker build -t relayhub:task1 .
```

Docker build chạy lại parity test của embedded docs trước khi compile binary. Image dùng entrypoint `/usr/local/bin/relayhub` và chạy bằng non-root user.

## Chạy stack Task 1

Đặt hai secret bắt buộc trong shell rồi chạy Compose:

```bash
export RELAYHUB_ADMIN_TOKEN='replace-me'
export RELAYHUB_SIGNING_SECRET='replace-me-too'
docker compose -f public-docs/deploy/docker-compose.relayhub.yml up --build
```

Stack hiện tại có hai service chạy được:

- `relayhub-api`: API, health/readiness, metrics và embedded docs.
- `relayhub-redis`: Redis 7 nội bộ với AOF và named volume.

Smoke test:

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
curl http://localhost:8080/metrics
curl -L http://localhost:8080/docs/
curl http://localhost:8080/docs/llms.txt
```

## Topology MVP đã lên kế hoạch

MVP cuối cùng có đúng ba service trong network `relayhub`:

1. `relayhub-api`
2. `relayhub-worker`
3. `relayhub-redis`

API và worker sẽ dùng cùng source image với command khác nhau. Chỉ API publish port `8080`; Redis và worker ở trong internal network. File Compose ghi topology dự kiến trong extension `x-relayhub-planned-mvp`, nhưng không khai báo worker chưa tồn tại thành runnable service.

Traefik/LB bên ngoài route toàn bộ `relayhub.dungxbuif.com` tới `relayhub-api`. `/ws` sẽ được thêm ở WebSocket phase; không cấu hình health check hoặc proxy cho route đó ở Task 1.
