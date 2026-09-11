# Deploy stack: RelayHub + docs trong cùng API service

## Kiến trúc mong muốn

- Traefik/LB ở ngoài chỉ trỏ domain vào service `relayhub-api`.
- `relayhub-api` tự đáp ứng:
  - API endpoints
  - WebSocket
  - `/docs` (user + developer + skills + llms files)
- `relayhub-worker` chạy nền xử lý queue/retry.
- `relayhub-redis` dùng cho queue/state store.

## Tệp mẫu

- `docker-compose.relayhub.yml` (3 container trong cùng network)
- `traefik/labels.yml` (ví dụ router service)

## Build/deploy

1. Build FE docs + backend bằng quy trình của bạn (frontend docs là static), rồi publish image:

```bash
docker build -t your-registry/relayhub-api:latest .

docker build -t your-registry/relayhub-worker:latest -f Dockerfile.worker .
```

2. Chạy stack:

```bash
docker compose -f public-docs/deploy/docker-compose.relayhub.yml up -d
```

3. Smoke test:

- `GET /docs` => trả về trang `/docs`
- `GET /docs/developer/skills.md` => tài nguyên tích hợp
- `GET /docs/llms.txt` => AI-readable
- API route chính: `GET /health`, `GET /ws`

## Lưu ý

- `relayhub-api` đang mount `../public-docs` read-only để serve `/docs`.
- Nếu API đã có middleware tách static path khác, điều chỉnh cho tương thích.
- Nếu bạn dùng Kafka thay Redis cho queue, đổi service `relayhub-redis` tương ứng.
