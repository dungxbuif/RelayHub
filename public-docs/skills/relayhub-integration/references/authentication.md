# Authentication reference

Production origin: `https://relayhub.dungxbuif.com`. Operator routes use
`Authorization: Bearer <admin-token>`. Application routes require all three HMAC
headers; there is no API-key-only mode. See
[full examples](https://relayhub.dungxbuif.com/docs/developer/auth.md).

## Exact algorithm

1. Serialize body once to UTF-8 bytes; GET/ack use empty bytes.
2. Timestamp is decimal Unix seconds.
3. Canonical text joins timestamp, uppercase method, exact escaped path/query,
   and lowercase SHA256 hex of body with LF and no trailing newline.
4. HMAC-SHA256 uses the entire issued secret's UTF-8 bytes; encode lowercase hex.
5. Send `X-RelayHub-Api-Key`, `X-RelayHub-Timestamp`, `X-RelayHub-Signature`.

Preserve query order and percent escaping; exclude scheme/host. Never decode the
`rhs_` suffix or reserialize after signing. Default skew is ±300 seconds.
Invalid keys, signature or timestamps produce a generic 401 error envelope.

```python
import hashlib, hmac, os, time, urllib.request
path = "/api/v1/queue?limit=20&wait=0"
timestamp = str(int(time.time()))
canonical = "\n".join((timestamp, "GET", path, hashlib.sha256(b"").hexdigest()))
headers = {
    "X-RelayHub-Api-Key": os.environ["RELAYHUB_API_KEY"],
    "X-RelayHub-Timestamp": timestamp,
    "X-RelayHub-Signature": hmac.new(
        os.environ["RELAYHUB_HMAC_SECRET"].encode(),
        canonical.encode(), hashlib.sha256).hexdigest(),
}
request = urllib.request.Request("https://relayhub.dungxbuif.com" + path, headers=headers)
with urllib.request.urlopen(request, timeout=40) as response:
    data = response.read()  # Process without logging credentials or payloads.
```

Use an approved secret store or environment. Never print headers/keys or embed
secrets in generated source/output. Signing stays on trusted backends. Browser
clients get short-lived socket tokens from their authenticated backend. Rotation
replaces both credentials; existing socket tokens/sessions are not revoked.
