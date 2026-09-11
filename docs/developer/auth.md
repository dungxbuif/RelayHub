# Authentication & Request Signing

RelayHub ưu tiên dùng **HMAC signature** cho server-to-server và token cho realtime.

## HTTP API

- Header: `X-RelayHub-Api-Key`
- Header bổ sung: `X-RelayHub-Signature` (HMAC-SHA256)
- Timestamp `X-RelayHub-Timestamp` tránh replay.

## Signature (gợi ý)

Signature = `HMAC_SHA256(secret, timestamp + method + path + body)`.

## Realtime

- OAuth2-style access token ngắn hạn hoặc API key đổi mới theo định kỳ.
- Scope cần rõ: `ws:connect`, `ws:subscribe`, `ws:read`.
