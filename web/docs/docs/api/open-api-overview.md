---
title: API Reference Overview
description: Bản tóm tắt API cho operator và dev.
---

# API Reference Overview

Tài liệu kỹ thuật gốc là `web/docs/static/openapi.json`. Track này giúp team đọc nhanh phần thường dùng:

- Apps: `POST /api/v1/apps`, `GET /api/v1/apps`, `PATCH /api/v1/apps/{id}`
- Routing: `POST /api/v1/routing/rules`, `GET /api/v1/routing/rules`
- Events: `POST /api/v1/events`
- Realtime: `POST /api/v1/socket/token`, `POST /api/v1/realtime/channels/{channel}/publish`
- Jobs: `GET /api/v1/jobs/{id}`
- Functions (RPC): `POST /api/v1/functions`, `GET /api/v1/functions`

Tất cả endpoint theo base URL `/api/v1`, lỗi chuẩn dạng:

```json
{"error":{"code":"invalid_request","message":"The request is invalid."}}
```
