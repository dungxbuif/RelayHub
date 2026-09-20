# RelayHub Docusaurus Docs

Đây là track tài liệu đẹp (Docusaurus) cho team người dùng, dev và operator.

## Chạy local

```bash
cd web/docs
npm install
npm run start
```

- Docs site mặc định chạy tại `http://localhost:3000`.
- Đây là track trình bày, không thay đổi contract runtime.

## Đồng bộ tài liệu và contract tĩnh

Contract gốc vẫn là:
- `web/docs/static/openapi.json`
- `web/docs/static/llms.txt`
- `web/docs/static/llms-full.txt`
- `web/docs/static/skills/relayhub-integration.zip`

Cập nhật `web/docs/static` trước sau đó cập nhật nội dung Docusaurus cho cùng ý nghĩa.
