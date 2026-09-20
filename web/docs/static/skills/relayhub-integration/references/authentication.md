# Authentication reference

Production origin: `https://relayhub.dungxbuif.com`. Operator routes use
`Authorization: Bearer <admin-token>`. Application routes require all three HMAC
headers; there is no API-key-only mode. See
[full examples](https://relayhub.dungxbuif.com/docs/developer/auth.md).

## Exact algorithm

1. Serialize body once to UTF-8 bytes; GET requests use empty bytes.
2. Timestamp is decimal Unix seconds.
3. Canonical text joins timestamp, uppercase method, exact escaped path/query,
   and lowercase SHA256 hex of body with LF and no trailing newline.
4. HMAC-SHA256 uses the entire issued secret's UTF-8 bytes; encode lowercase hex.
5. Send `X-RelayHub-Api-Key`, `X-RelayHub-Timestamp`, `X-RelayHub-Signature`.

Preserve query order and percent escaping; exclude scheme/host. Never decode the
`rhs_` suffix or reserialize after signing. Default skew is ±300 seconds.
Invalid keys, signature or timestamps produce a generic 401 error envelope.

```python
import hashlib, hmac, json, os, time, urllib.request
path = "/api/v1/events"
body = json.dumps({"type":"order.created","target_app_ids":[os.environ["TARGET_APP_ID"]],"data":{"order_id":"123"}}, separators=(",", ":")).encode()
timestamp = str(int(time.time()))
canonical = "\n".join((timestamp, "POST", path, hashlib.sha256(body).hexdigest()))
headers = {
    "X-RelayHub-Api-Key": os.environ["RELAYHUB_API_KEY"],
    "X-RelayHub-Timestamp": timestamp,
    "X-RelayHub-Signature": hmac.new(
        os.environ["RELAYHUB_HMAC_SECRET"].encode(),
        canonical.encode(), hashlib.sha256).hexdigest(),
    "Idempotency-Key": os.environ["EVENT_KEY"],
    "Content-Type": "application/json",
}
request = urllib.request.Request("https://relayhub.dungxbuif.com" + path, data=body, headers=headers, method="POST")
with urllib.request.urlopen(request, timeout=40) as response:
    data = response.read()  # Process without logging credentials or payloads.
```

Use an approved secret store or environment. Never print headers/keys or embed
secrets in generated source/output. Signing stays on trusted backends. Browser
clients get short-lived socket tokens from their authenticated backend. Rotation
replaces both credentials; existing socket tokens/sessions are not revoked.
