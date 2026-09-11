# RelayHub Public Docs

## Mục tiêu

Public docs serve on route `https://relayhub.dungxbuif.com/docs`.

- **User docs**: `user/` (hướng dẫn sử dụng)
- **Developer docs**: `developer/` (contract, đăng ký, auth, reliable delivery)
- **AI-readable**: `llms.txt`, `llms-full.txt`

## Bố trí nhanh

- `public-docs/index.html`: entry page cho `/docs`
- `public-docs/user/*`: docs cho người dùng
- `public-docs/developer/*`: docs cho developer

## Deploy mẫu

### Nginx

Copy `deploy/nginx/conf.d/docs.conf` vào server proxy, mount source docs tại `/srv/docs`.

### Caddy

Copy `deploy/caddy/Caddyfile` và thay `localhost:8080` bằng API backend.

### Nginx + Docker (local)

Mở `deploy/docker-compose.yml` và thay `your-api-image` + port backend nếu cần.

## Route check (kiểm tra nhanh)

- `GET /docs` → redirect to `/docs/` and homepage
- `GET /docs/developer/skills.md` → lấy tài nguyên tích hợp + SDK
- `GET /docs/developer/auth.md` -> auth/signature docs cho developer
- `GET /docs/llms.txt`, `GET /docs/llms-full.txt` → máy đọc tài liệu

## Kiểm thử docs tại local

```bash
cd public-docs
./scripts/test-docs.sh
```

Script sẽ kiểm tra:

1. Endpoint công khai bắt buộc tồn tại:
   - `/docs`, `/docs/`, `/docs/user`, `/docs/developer`
   - `/docs/user/getting-started.md`, `/docs/user/faq.md`
   - `/docs/developer/skills.md`, `/docs/developer/auth.md`
   - `/docs/llms.txt`, `/docs/llms-full.txt`
2. Kiểm tra marker nội dung quan trọng ở các trang cốt lõi.
3. Quét toàn bộ markdown trong `public-docs` để bắt lỗi link nội bộ bị hỏng.

## CI

File `.github/workflows/docs-smoke.yml` chạy tự động:

- `push` trên nhánh `main`
- mọi `pull_request`

Mỗi lần kích hoạt, pipeline sẽ chạy `./scripts/test-docs.sh` trong thư mục `public-docs`.

## Stack chuẩn (API tự serve docs)

- Dùng `public-docs/deploy/docker-compose.relayhub.yml` khi muốn chạy theo mô hình 3 service trong cùng network relayhub (API + worker + queue/redis).
- API giữ `/docs` trong cùng service, không cần proxy/docs riêng.
- Traefik/edge ở ngoài chỉ trỏ domain vào API service.
