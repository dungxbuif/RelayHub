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

- Subprotocol: `relayhub.v1`.
- Token từ `POST /api/v1/socket/token` (scoped: `ws:connect`, `ws:subscribe`, `ws:read`).
- Mỗi frame JSON có giới hạn kích thước theo spec.

## Stream channel

- Durable stream dùng cho luồng processing/dispatch.
- Worker bền, retry + lease + dead-letter.
- Không dùng websocket như cơ chế đảm bảo bền.
