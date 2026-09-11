# Source bootstrap — kế hoạch chi tiết

## Mục tiêu và phạm vi

Tạo source runnable tại `src/`, giữ planning ở thư mục cha. Đây là bước khởi tạo trước Task 1, không thực hiện toàn bộ MVP. Centrifugo vẫn là engine baseline; Socket.IO chưa tương thích trực tiếp, raw WebSocket cần nói protocol Centrifugo. SDK RelayHub không bắt buộc cho transport.

## Deliverables

- Go module không external dependency: config, HTTP routing, JSON errors, request ID, graceful shutdown.
- Health 200, readiness 503 khi chưa có dependencies; API chưa làm trả 501, route lạ 404, admin 404. Không nhận job hoặc cấp token giả.
- OpenAPI cho đúng API bootstrap đang chạy; API_CONTRACT tiếp tục là target.
- Hướng dẫn local và source map để triển khai từng subsystem.

## Các bước

- [x] Viết tests cho default loopback/config lỗi, health/readiness, API 501, admin 404, method và request IDs.
- [x] Chạy test red trước source implementation.
- [x] Implement config, router và lifecycle với timeouts, signal cancellation.
- [x] Chạy tests race, vet, build và smoke qua HTTP thật.
- [x] Đồng bộ README, implementation plan, trạng thái feature và API machine-readable.

## Files

`src/go.mod`, `src/cmd/relayhub/main.go`, `src/internal/config/`, `src/internal/httpapi/`, `src/api/openapi.json`, `src/README.md`. Các thư mục jobs/projects/realtime/dispatch chỉ tạo khi có implementation, không thêm empty packages giả.

## Verification

Từ src: `rtk go test -race ./...`, `rtk go vet ./...`, `rtk go build -o /tmp/relayhub-bootstrap ./cmd/relayhub`. Smoke dùng bind loopback ephemeral, kiểm tra health/readiness/API rồi SIGTERM, exit 0. Không DNS/VPS/deploy.
