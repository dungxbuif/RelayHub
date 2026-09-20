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
- Queue v2: `/api/v2/subscriptions` + batch pull/settle/lease extension

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
- `web/docs/static/developer/typescript-sdk.md`
- `skills/relayhub-integration/references/openapi.json`

Worker cần chủ động kiểm soát batch, concurrency và backpressure nên dùng Queue
v2 qua TypeScript `relayhub.queue.work(...)` hoặc Go `WorkQueue(...)`. Queue v2
là at-least-once; lưu side effect idempotent trước ACK.
