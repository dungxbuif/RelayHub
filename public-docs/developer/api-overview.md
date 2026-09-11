# API Overview (Draft)

## Endpoint nhóm điều khiển app

- `POST /api/v1/apps` tạo app.
- `GET /api/v1/apps/{app_id}` xem chi tiết.
- `PATCH /api/v1/apps/{app_id}` cập nhật cấu hình.
- `DELETE /api/v1/apps/{app_id}` vô hiệu hóa.

## Endpoint event

- `POST /api/v1/events` tiếp nhận event từ upstream.
- `POST /api/v1/events/{event_id}/ack` xác nhận consume.
- `GET /api/v1/events/{event_id}` kiểm tra trạng thái.

## Job/Queue

- `GET /api/v1/jobs/{job_id}`
- `POST /api/v1/jobs/{job_id}/requeue`
- `POST /api/v1/jobs/{job_id}/dead-letter`

## Realtime

- `GET /ws` WebSocket endpoint.
- `GET /api/v1/socket/issue-token` cấp token phiên.
