# Deployment Stack (RelayHub Docs + API)

## Mục tiêu

Triển khai theo nguyên tắc: **docs là trách nhiệm của RelayHub API**, không cần reverse proxy docs riêng.

## Topology

- `relayhub-api`:
  - serve API + websocket
  - serve `/docs/*` từ thư mục docs nội bộ
- `relayhub-worker`:
  - xử lý nền cho queue/retry
- `relayhub-queue/redis`:
  - backing services cho bus/state

## Quy tắc route tại API

- `GET /docs` → chuyển sang `/docs/` và trả index
- `GET /docs/*` → static file under docs root
- Route còn lại -> API app handlers

## Dấu hiệu test chính

- `curl https://relayhub.dungxbuif.com/docs`
- `curl https://relayhub.dungxbuif.com/docs/developer/skills.md`
- `curl https://relayhub.dungxbuif.com/health`
- `wscat -c wss://relayhub.dungxbuif.com/ws`
