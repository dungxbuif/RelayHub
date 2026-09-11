# Kiến trúc tài liệu

## Mục tiêu kiến trúc docs

- Tài liệu có thể dùng trực tiếp bởi **user** và **developer**.
- Cấu trúc tách đôi:
  - Docs trải nghiệm (human): `docs/user/*`
  - Docs tích hợp (machine + engineer): `docs/developer/*`

## Gợi ý route

- `/docs/user` -> user docs.
- `/docs/developer` -> developer docs.
- `/docs/developer/skills` -> page tab kỹ năng/tài nguyên tích hợp.

## Tương thích API
docs

- Cách đặt tên endpoint thống nhất (REST-style, versioned).
- Event envelope có `event_id`, `type`, `tenant_id`, `app_id`, `payload`, `signature`.
