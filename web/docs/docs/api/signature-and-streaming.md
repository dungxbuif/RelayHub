---
title: Signature & Streaming
description: Chính sách bảo mật và hướng dẫn realtime/stream.
---

# Signature & Streaming

## HTTP signature

- Header bắt buộc: `X-RelayHub-Api-Key`, `X-RelayHub-Timestamp`, `X-RelayHub-Signature`.
- Message: `timestamp\nMETHOD\npath_and_query\nsha256(raw_body_hex)`.
- Dùng secret đầy đủ do `POST /api/v1/apps` trả về (one-time).

## WebSocket

- Realtime v2 dùng subprotocol `relayhub.realtime.v2`; không gửi subprotocol sẽ giữ giao thức v1 cũ.
- Token v2 từ `POST /api/v1/socket/token` chứa `client_id` và quyền chính xác theo channel (`subscribe`, `publish`, `presence`).
- Token v1 vẫn dùng scope `ws:connect`; durable stream dùng `stream:connect` và subprotocol `relayhub.stream.v1`.
- Mỗi frame JSON có giới hạn kích thước theo spec.
- Realtime chỉ là online hint, không có replay. Dùng durable stream/callback cho xử lý bắt buộc.

## Stream channel

- Durable stream dùng cho luồng processing/dispatch.
- Worker bền, retry + lease + dead-letter.
- Không dùng websocket như cơ chế đảm bảo bền.
