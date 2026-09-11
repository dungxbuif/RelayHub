# Authentication and request signing

RelayHub has two authentication paths. Administrators manage applications with a bearer token. Applications authenticate each HTTP request with an API key and an HMAC signature. Credentials are returned only when an application is created or rotated; store them in a secret manager immediately.

## Administrative requests

Send the configured `RELAYHUB_ADMIN_TOKEN` as a bearer token:

```http
Authorization: Bearer <RELAYHUB_ADMIN_TOKEN>
```

Application creation and listing require this header. Invalid or missing credentials return `401` using the standard JSON error envelope.

## Signed application requests

Send all three headers:

```http
X-RelayHub-Api-Key: <api-key>
X-RelayHub-Timestamp: <unix-seconds>
X-RelayHub-Signature: <lowercase-hex-hmac>
```

By default, the timestamp may differ from RelayHub's clock by at most 300 seconds (configurable with `RELAYHUB_SIGNING_SKEW`). Both `-300` and `+300` seconds are accepted. Generate a new timestamp and signature for retries.

Compute the body hash from the exact bytes sent on the wire, including whitespace. For an empty body, hash zero bytes. The request target is the path and encoded query string, such as `/api/v1/socket/token?audience=browser`; it does not include the scheme or host. Use the uppercase HTTP method.

```text
body_hash = lowercase_hex(SHA256(body_bytes))
canonical = timestamp + "\n" + method + "\n" + request_target + "\n" + body_hash
signature = lowercase_hex(HMAC_SHA256(hmac_secret, canonical))
```

Canonical test vector:

```text
hmac_secret:   test-secret
timestamp:     1770000000
method:        POST
request-target:/api/v1/socket/token?audience=browser
body:          {"scopes":["ws:connect"],"ttl_seconds":600}
body SHA-256:  c2c24c6018f5adc3c2fd4ff027e5c9bf7b892f06f6e56cb31960b3f571ee80bf
signature:     4b6a6ada471e93240169f8878e877be6aa8d8cbf764e48f4a9d1b3c2ffef82ca
```

### Go example

```go
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func sign(secret []byte, timestamp, method, target string, body []byte) string {
	bodyHash := sha256.Sum256(body)
	canonical := timestamp + "\n" + strings.ToUpper(method) + "\n" + target + "\n" + hex.EncodeToString(bodyHash[:])
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(canonical))
	return hex.EncodeToString(mac.Sum(nil))
}

func main() {
	body := []byte(`{"scopes":["ws:connect"],"ttl_seconds":600}`)
	target := "/api/v1/socket/token"
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	req, _ := http.NewRequest(http.MethodPost, "https://relayhub.dungxbuif.com"+target, strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-RelayHub-Api-Key", "replace-with-api-key")
	req.Header.Set("X-RelayHub-Timestamp", timestamp)
	req.Header.Set("X-RelayHub-Signature", sign([]byte("replace-with-hmac-secret"), timestamp, req.Method, target, body))
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal("socket token request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		log.Fatalf("socket token request returned status %d", response.StatusCode)
	}
	var result struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil || result.Token == "" {
		log.Fatal("socket token response was invalid")
	}
	// Pass result.Token directly to the WebSocket client. Do not log it.
	log.Println("Socket token issued.")
}
```

### Node.js example

```js
import { createHash, createHmac } from "node:crypto";

const body = JSON.stringify({ scopes: ["ws:connect"], ttl_seconds: 600 });
const target = "/api/v1/socket/token";
const method = "POST";
const timestamp = Math.floor(Date.now() / 1000).toString();
const bodyHash = createHash("sha256").update(Buffer.from(body)).digest("hex");
const canonical = `${timestamp}\n${method}\n${target}\n${bodyHash}`;
const signature = createHmac("sha256", process.env.RELAYHUB_HMAC_SECRET)
  .update(canonical)
  .digest("hex");

const response = await fetch(`https://relayhub.dungxbuif.com${target}`, {
  method,
  headers: {
    "content-type": "application/json",
    "X-RelayHub-Api-Key": process.env.RELAYHUB_API_KEY,
    "X-RelayHub-Timestamp": timestamp,
    "X-RelayHub-Signature": signature,
  },
  body,
});
if (!response.ok) {
  throw new Error(`Socket token request returned status ${response.status}`);
}
const { token } = await response.json();
if (typeof token !== "string" || token.length === 0) {
  throw new Error("Socket token response was invalid");
}
// Pass token directly to the WebSocket client. Do not log it.
console.log("Socket token issued.");
```

## Socket tokens

`POST /api/v1/socket/token` is itself a signed application request. Request one or more explicit scopes from `ws:connect`, `ws:subscribe`, and `ws:read`, with `ttl_seconds` from 1 through 900. RelayHub returns an application-scoped HMAC token. A token expires at its `exp` time, cannot be used for a missing scope, and cannot be reassigned to another application.

## Implementation and verification note

Task 2 stores only a SHA-256 API-key lookup index, compares bearer tokens and signatures in constant time, and atomically removes the old credential index on rotation. Verification covers canonical signing, request mutation, malformed and expired authentication, scope and TTL policy, one-time credential responses, disable/rotation invalidation, Redis concurrency, docs parity, race detection, vet, and the production Docker build.
