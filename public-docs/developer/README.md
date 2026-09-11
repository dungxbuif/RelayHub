# Developer Integration Docs

Đây là tài liệu tích hợp cho team/đối tác.

## Luồng tích hợp tối thiểu

- `POST /api/v1/providers/{provider}/events`: đẩy event vào RelayHub.
- `GET /api/v1/apps/{app_id}/config`: lấy cấu hình app (để client bootstrap).
- `POST /api/v1/apps/{app_id}/signing-keys/rotate`: xoay key khi bị lộ.
- `GET /ws`: mở WebSocket/Broadcast khi app muốn theo dõi status realtime.
- `GET /jobs/{job_id}`: tra trạng thái job/retry.

## Các file tài liệu kèm theo

- [API reference](./api-overview.md)
- [Flow tích hợp đăng ký](./registration-flow.md)
- [Auth & Signature](./auth.md)
- [Retry / DLQ](./reliability.md)
- [Skills Resources](./skills.md)
