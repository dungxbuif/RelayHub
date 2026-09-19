# Release note: RelayHub v1.0 (Production Readiness)

- **Ngày tạo:** 2026-09-14
- **Nhánh:** `feat/relayhub-mvp`
- **Mục tiêu:** Chuẩn bị đánh dấu bản phát hành v1 ở môi trường **homelab production** với luồng đăng ký ứng dụng, khóa API, routing event + realtime channel.

## Tình trạng

RelayHub v1 đã đạt trạng thái "**sẵn sàng deploy**" cho mục tiêu nền tảng nội bộ nếu đi qua đủ checklist dưới đây.

### Chuẩn kỹ thuật đã có trong bản này

- API + worker + PostgreSQL + NATS chạy cùng stack Docker Compose.
- API/worker chạy non-root/read-only; Postgres/NATS giữ entrypoint mặc định của image để tự init quyền volume.
- Bản v1 đã có:
  - Đăng ký ứng dụng (app registration).
  - API key / token (bearer) dùng cho client và callback signing.
  - Route-based publish event (event class -> app target).
  - Realtime channel qua WebSocket với token signed.
  - Durable delivery (stream + callback), gồm retry/độ bền và idempotency.
  - Giao diện control panel cơ bản để cấu hình app/event/consumer.
  - SDK/Docs chuẩn hóa cho user + developer, kèm skill integration.
  - Public docs bằng Markdown + llms artifact (`llms.txt`, `llms-full.txt`) và Docusaurus source.
- Quy trình lỗi và retry không phụ thuộc polling thủ công; không dùng Redis queue custom.

## Điều kiện bắt buộc trước khi deploy prod

- [ ] Cập nhật `RELAYHUB_` env thật đầy đủ trong `.env` (không commit file này):
  - `RELAYHUB_ADMIN_TOKEN`
  - `RELAYHUB_SIGNING_SECRET`
  - `RELAYHUB_POSTGRES_PASSWORD`
  - `RELAYHUB_SECRET_ENCRYPTION_KEY`
  - `RELAYHUB_NATS_PASSWORD`
  - `RELAYHUB_NATS_USERNAME`
- [ ] Đặt permission `.env` = `600`.
- [ ] Cài đặt lại hostname/public endpoint tương ứng domain homelab (`relayhub.dungxbuif.com`).
- [ ] Traefik/Cloudflare chỉ route tới API `8080`; giữ worker không publish port.
- [ ] Kiểm tra callback HTTPS, CA, network policy để chặn egress ngoài danh sách hợp lệ.
- [ ] Đảm bảo backup PostgreSQL + NATS/JetStream đã diễn tập hồi phục thành công.

## Validation gate (đã chạy trước khi release)

1. **Code quality + tests**
   - `test -z "$(gofmt -l .)"`
   - `go vet ./...`
   - `go -C backend test ./...`
   - `go test -race ./...`
   - `go test -race -tags=integration ./... -count=1 -timeout=180s`

2. **Build/docs contracts**
   - `./backend/scripts/build-skill.sh`
   - `./backend/scripts/build-llms.sh`
   - `python3 scripts/check-docs.py --static`
   - `./backend/scripts/check-contracts.sh --self-test`
   - `go -C backend generate ./web` (sau khi đổi docs công khai)

3. **Stack và observability smoke**
   - `docker compose up --build -d --wait`
   - `curl --fail http://localhost:8080/healthz`
   - `curl --fail http://localhost:8080/readyz`
   - `curl --fail http://localhost:8080/metrics`
   - Gửi 1 lần publish signed event thử nghiệm + nhận callback hoặc stream replay.

## Runbook deploy/prod

- Chuẩn hóa network nội bộ: API/Worker/Postgres/NATS cùng project network; PostgreSQL/NATS không expose public.
- Dùng domain routing tĩnh, giữ TLS, không cache `/api/*`, `/ws`, `/healthz`, `/readyz`, `/metrics`.
- Đảm bảo `stop_grace_period >= shutdown_timeout` để đóng socket/claims sạch.
- Kiểm tra logs qua health probes trước khi bật truy cập toàn bộ.

## Rollback

- Nếu nghi ngờ lỗi ứng dụng: rollback image/revision API+Worker trước đó, giữ nguyên dữ liệu Postgres/NATS hiện có nếu schema tương thích.
- Nếu cần schema-level rollback: restore backup toàn bộ cặp Postgres + NATS + `.env`.

## Rủi ro/nhận xét trước release

- WebSocket phụ thuộc infrastructure ngoài (Traefik/Cloudflare) cho timeout/keep-alive; lỗi edge có thể làm kết nối reset. Reconnect là bắt buộc ở client.
- Cloudflare/proxy không dùng cho callback caching.
- Sau đổi cấu hình NATS replica/namespace cần tái kiểm chứng chuẩn stream settings và chạy lại `readyz`.

## Tài liệu liên quan

- [Operations runbook](./runbook.md)
- [Deployment guide](../web/docs/static/deploy/README.md)
- [Public docs](../web/docs/static/README.md)
- [Developer contracts](./developer/contracts.md)
