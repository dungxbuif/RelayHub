---
title: Quick Integrate
description: Tích hợp app RelayHub nhanh trong 10 phút.
---

# Quick Integrate

## Chuẩn HTTP

- Base path: `/api/v1`
- Event API: `POST /api/v1/events`
- Routing rules: `/api/v1/routing/rules`
- Realtime: `POST /api/v1/realtime/channels/{name}/publish` + websocket token từ `/api/v1/socket/token`
- Stream: `GET /api/v1/stream`

## Ví dụ ký header

```bash
X-RelayHub-Api-Key: <app_api_key>
X-RelayHub-Timestamp: <unix_seconds>
X-RelayHub-Signature: hmac_sha256(api_secret, "ts\nMETHOD\npath\nsha256(body)")
```

## Độ tin cậy

- Event được lưu durable trước khi fan-out.
- NATS/JetStream chịu phần vận hành realtime và durable transport.
- PostgreSQL chịu state, retry progress, job lifecycle.

## SDK

SDK trong kho chính đã cung cấp mẫu gọi API/đọc stream theo spec trong contract:
- `public-docs/developer/typescript-sdk.md`
- `skills/relayhub-integration/references/openapi.json`
