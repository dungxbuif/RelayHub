# RelayHub Documentation (Local + Public)

Mục tiêu của bộ docs này là: **toàn bộ tài liệu tích hợp và vận hành của RelayHub nằm ở một nơi duy nhất**, phân tách rõ:

- **`/docs/user/`**: nội dung cho người dùng (hướng dẫn sử dụng, onboarding, vận hành cơ bản).
- **`/docs/developer/`**: nội dung cho developer (API, luồng đăng ký, websocket/queue, sample integration).

## Quy tắc triển khai

- Mọi thay đổi tính năng mới phải có cập nhật tương ứng tại:
  - `public-docs/` (phiên bản user-facing và developer-friendly).
  - `public-docs/llms-full.txt` (AI-agent-readable).
- Mọi cập nhật flow hoặc schema API bắt buộc có:
  - Mục **Request/Response behavior**.
  - Mục **Failure mode**.
  - Mục **Rate limit / security / auth**.
- Nếu có thay đổi ảnh hưởng cấu hình môi trường: cập nhật `docs/architecture/config.md` trước khi triển khai code.

## Khu vực docs công khai

- Public docs chạy tại route: **`/docs`**.
- Đánh dấu rõ 2 tab lớn:
  1. **User Docs** (dùng cho user/PM).
  2. **Developer Docs** (SDK/API + tích hợp service).


## Kiểm thử docs

- Kiểm tra nhanh trước khi review/merge docs: `cd public-docs && ./scripts/test-docs.sh`.
- CI tự động hóa: workflow `.github/workflows/docs-smoke.yml` sẽ chạy test docs smoke trên `push` và `pull_request`.
- Test đang kiểm tra: endpoints bắt buộc, nội dung marker, và link nội bộ trong markdown.
