---
title: Control Panel Track Plan
description: Theo dõi lộ trình track UI vận hành.
---

# Control Panel Track Plan

## Track hiện tại (v1)

- `App lifecycle`: tạo/list apps.
- `Routing`: quản lý rule theo event type.
- `Event`: publish nhanh và test signed request.
- `Realtime`: subscribe/publish channel.

## Track nâng cấp (v1+)

- Dashboard job/event với filter theo app/time.
- Retry/requeue operator tools.
- Secrets view policy (không hiển thị secret raw).
- Audit event cho mỗi lần thay đổi rule/app.

## Checklist trước khi dùng production internal

- Gắn auth layer cho UI.
- Gọi lại docs/contract sau mỗi deploy.
- Kiểm tra `/readyz` và worker lag cho mỗi lần thay đổi rule.

## Mapping endpoint

| Action | Endpoint |
| --- | --- |
| Tạo app | `POST /api/v1/apps` |
| Danh sách app | `GET /api/v1/apps` |
| Tạo routing | `POST /api/v1/routing/rules` |
| Publish event | `POST /api/v1/events` |
| Realtime token | `POST /api/v1/socket/token` |
| Publish realtime | `POST /api/v1/realtime/channels/{channel}/publish` |
