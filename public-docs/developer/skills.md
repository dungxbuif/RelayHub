# Skills Pack cho team tích hợp

Mục này đóng vai trò **tab Skills** trong public docs.

## Danh sách tài nguyên để copy

Bạn có thể copy nguyên khối config dưới đây vào project đối tác:

### 1) cURL checklist

```bash
# Create app
curl -X POST "$RELAYHUB_BASE/api/v1/apps" \
  -H "X-RelayHub-Api-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name":"my-integration","callback_url":"https://api.acme.local/events"}'

# Issue socket token
curl -X POST "$RELAYHUB_BASE/api/v1/socket/issue-token" \
  -H "X-RelayHub-Api-Key: $API_KEY"
```

### 2) SDK suggestion

- Node.js: `npm i @relayhub/client`
- Go: `go get github.com/dungxbuif/relayhub-go`
- Python: `pip install relayhub-client`

### 3) Event schema mẫu

```json
{
  "event_id": "evt_01HX...",
  "type": "order.created",
  "tenant_id": "t_123",
  "app_id": "app_123",
  "payload": {"id":"ord_456"},
  "created_at": "2026-09-11T00:00:00Z",
  "signature": "base64(...)"
}
```

### 4) Troubleshooting quick links

- Signature mismatch
- Socket disconnect loop
- Retry exceeded / dead-letter
