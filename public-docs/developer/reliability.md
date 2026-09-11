# Reliability, Retry, and Delivery Modes

## Realtime (Direct)

- Phản hồi nhanh, ưu tiên cho request-response.
- Có timeout 2-10s tùy route.

## Persistent Queue

- Đảm bảo không mất event khi target tạm thời không phản hồi.
- Hỗ trợ backoff exponential + jitter + max retry attempts.
- Kết thúc sau số lần retry sẽ đưa vào **dead-letter**.

## Retry policy (đề xuất)

`1s -> 5s -> 15s -> 60s -> 300s`, giới hạn theo tenant plan.

## Error semantic

- `4xx`: lỗi payload hoặc signature, không retry tự động.
- `5xx/timeout/network`: retry theo policy.
- `429`: giảm tốc độ theo `Retry-After`.
