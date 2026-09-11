# Task 2 implementation report

## Status

DONE

## Delivered behavior

- Added the application domain model with `queue`, `websocket`, `callback`, and `all` delivery modes, optional callback URL, enabled state, and creation/update timestamps.
- Added `service.AppService` with create, list, get, update, disable, rotate-secret, and hashed API-key authentication operations.
- Application creation and rotation generate opaque random identifiers and 32-byte credentials. Only the SHA-256 API-key hash is passed to persistence. Query models contain no credential fields.
- Added canonical request signing with lowercase hex HMAC-SHA256 over `timestamp + "\n" + method + "\n" + request-target + "\n" + SHA256(body)`, typed authentication failures, constant-time signature comparison, and inclusive 300-second boundary handling based on Unix seconds.
- Added short-lived URL-safe HMAC socket tokens with version, application ID, explicit scopes, issued-at and expiry claims. Issue and verification reject invalid or duplicate scopes, tampering, expiry, missing scopes, non-second TTL values, and TTLs outside 1 second through 15 minutes.
- Added constant-time admin bearer authentication and signed application middleware using the exact RelayHub header names. Authentication errors are deliberately generic and never include key, secret, or signature material.
- Added all Task 2 routes. Create/list/disable/rotate are admin-authorized; get/update are signed and constrained to the authenticated application; socket-token issuance is signed and allows only `ws:connect`, `ws:subscribe`, and `ws:read`.
- Added absolute callback URL validation. HTTPS is the default. `RELAYHUB_ALLOW_INSECURE_CALLBACKS=true` permits HTTP only for loopback/private IP addresses and `.localhost`, `.local`, or `.internal` hosts.
- Added Redis application hashes, the application index, SHA-256 credential lookup indexes, and Lua-backed atomic create, update, disable, credential lookup, and rotation behavior. Rotation deletes the old index in the same Redis operation that installs the new one.
- Wired the Redis application store, application service, admin token, signing skew, and socket token issuer into `cmd/relayhub`.
- Replaced draft internal and public authentication/API pages with the shipped contract, canonical test vector, copyable Go and Node.js signing examples, route authorization, response shapes, errors, callback rules, and socket-token constraints. Regenerated `web/embed.go` from `public-docs`.

Public user documentation did not change because Task 2 exposes developer integration and administration contracts rather than an end-user workflow. The public Markdown pages are both human-facing and stable AI-readable sources under `/docs/developer/`; the generated embedded snapshot preserves the same content.

## TDD evidence

### Request signatures and socket tokens

RED:

```text
rtk go test ./internal/auth -v
exit 1: internal/auth/signature_test.go reported undefined Sign, Verify, and typed errors
```

GREEN:

```text
rtk go test ./internal/auth -v
24 passed
```

The tests cover the hand-derived canonical literal `4b6a6ada471e93240169f8878e877be6aa8d8cbf764e48f4a9d1b3c2ffef82ca`, method/path/body mutation, malformed headers, expired replay, both inclusive skew boundaries, token round-trip, URL-safe encoding, tampering, expiry, missing scope, malformed/duplicate scopes, and the inclusive 15-minute limit.

An additional boundary regression was driven red with a fractional server clock:

```text
rtk go test ./internal/auth -run TestVerifyAcceptsInclusiveTimestampBoundary -v
exit 1: the -300 second boundary was rejected as timestamp_expired
```

After comparing Unix seconds instead of fractional durations:

```text
rtk go test ./internal/auth -run TestVerifyAcceptsInclusiveTimestampBoundary -v
1 passed
```

### Insecure callback configuration

RED:

```text
rtk go test ./internal/config -run TestLoadParsesInsecureCallbackPolicy -v
exit 1: Config had no AllowInsecureCallbacks field
```

GREEN:

```text
rtk go test ./internal/config -run TestLoadParsesInsecureCallbackPolicy -v
5 passed
```

### Application service

RED:

```text
rtk go test ./internal/service -run TestApp -v
exit 1: the application domain package did not exist
```

GREEN:

```text
rtk go test ./internal/service -run TestApp -v
12 passed
```

The tests cover one-time credential output and redacted query JSON, hashed API-key lookup, rotation invalidation, disable invalidation, callback/delivery-mode policy, and clearing callbacks during an update.

### HTTP contracts

RED:

```text
rtk go test ./internal/httpapi -run 'Test(Admin|Signed|App|SocketToken)' -v
exit 1: Task 1 Dependencies had no Apps, AdminToken, TokenIssuer, Now, or SigningSkew fields
```

GREEN:

```text
rtk go test ./internal/httpapi -run 'Test(Admin|Signed|App|SocketToken)' -v
11 passed
```

The tests cover admin-only create/list, one-time response credentials, query redaction, signed self-service get/update, cross-app denial, malformed/unknown/expired app authentication, exact JSON error envelopes, admin rotation and disable invalidation, callback validation, and scoped socket-token issuance.

### Redis persistence

RED:

```text
rtk proxy go test -tags=integration ./internal/store/redisstore -run TestApplicationPersistenceAndCredentialIndexes -v
exit 1: Client had no application CRUD or credential-index methods
```

GREEN against a real Redis 7 testcontainer:

```text
rtk proxy go test -tags=integration ./internal/store/redisstore -v
4 subtests passed
```

The integration test covers create/get/list/update/disable, scanning Redis keys and values to prove the plaintext API key is absent, atomic old/new credential index rotation, and 16 concurrent identical creates producing exactly one winner and 15 conflicts. The test uses `RELAYHUB_TEST_REDIS_URL` when provided and otherwise starts a bounded Redis 7 testcontainer, skipping only when Docker is unavailable.

## Final verification

```text
rtk gofmt -w cmd internal
completed

rtk go test ./internal/auth ./internal/service ./internal/httpapi ./internal/config -v
89 passed in 4 packages

rtk go test ./...
91 passed in 12 packages

rtk go test -race ./internal/auth ./internal/service ./internal/httpapi
66 passed in 3 packages

rtk proxy go test -tags=integration ./internal/store/redisstore -v
4 Redis integration subtests passed

rtk go vet ./...
no issues found

rtk proxy ./public-docs/scripts/test-docs.sh
10 endpoints checked, 25 Markdown files scanned, 0 link errors

rtk go generate ./web
completed

rtk go test ./web -v
2 parity tests passed

rtk docker build -t relayhub:task2-test .
exit 0 using golang:1.24-alpine; web parity test and Linux binary build passed

rtk git diff --check
no whitespace errors
```

The testcontainers dependency is pinned at `v0.38.0`, and its required OpenTelemetry modules are pinned to Go-1.24-compatible versions. The production Docker build confirms the dependency graph works with the repository's `golang:1.24-alpine` builder.

## Fix round 1

### Concurrent update and disable

Root cause: `AppService.Update` read a complete application record and passed that stale record to `UpdateApplication`; the Redis Lua script then wrote the stale `enabled` value together with the configuration fields. A disable completed between the read and write could therefore be permanently reversed.

The regression uses channels in the service/store fake to pause `UpdateApplication` after the service has read the enabled record, complete `Disable`, and then release the update. It asserts that the update response and stored record remain disabled and that API-key authentication still returns `ErrUnauthorized`.

RED:

```text
rtk go test ./internal/service -run TestAppDisableWinsWhenUpdateReadPrecedesDisable -v
exit 1: Update() response re-enabled an application disabled after its initial read
```

GREEN after changing `ApplicationStore.UpdateApplication` to merge only configuration fields and return the persisted record:

```text
rtk go test ./internal/service -run 'TestApp(DisableWins|Update|Disable)' -v
3 passed
```

Redis now updates only `name`, `callback_url`, `delivery_mode`, and `updated_at`, then returns the complete record from the same Lua operation. Direct integration coverage also passes a stale enabled record after disable and proves Redis preserves `enabled=false` while applying the configuration change.

### Safe authentication examples

The Go example now sends the signed request with `http.DefaultClient.Do`, validates the status and token response, and prints only a fixed success message. The Node.js example likewise validates the response and keeps the returned token in memory for direct WebSocket use. Neither example prints the request, credential headers, signature, response body, or socket token.

The public source and generated embedded snapshot were reconciled and verified:

```text
rtk go generate ./web
completed

rtk go test ./web -v
2 parity tests passed

rtk proxy ./public-docs/scripts/test-docs.sh
10 endpoints checked, 25 Markdown files scanned, 0 link errors
```

### Redis testcontainer provisioning

Root cause: the integration helper called `GenericContainer` directly and treated every returned error as evidence that Docker was unavailable. That incorrectly hid image-pull, container startup, wait-strategy, and testcontainers configuration failures.

RED:

```text
rtk proxy go test -tags=integration ./internal/store/redisstore -run TestRedisContainerProvisioningOnlyClassifiesDockerProbeFailuresAsUnavailable -v
exit 1: provisionRedisContainer and errDockerUnavailable were undefined
```

GREEN:

```text
rtk proxy go test -tags=integration ./internal/store/redisstore -run 'Test(RedisContainerProvisioning|ApplicationPersistence)' -v
PASS: Docker availability classification plus five Redis application subtests
```

The helper now probes Docker daemon availability independently through the Docker client. Only a failed daemon probe is wrapped as `errDockerUnavailable` and skipped. Once that probe succeeds, any Redis image, container configuration, startup, or readiness failure is returned as a normal test failure. The green run exercised the available-Docker branch and successfully provisioned Redis 7.

### Fix-round verification

```text
rtk go test ./internal/service ./internal/httpapi -v
43 passed in 2 packages

rtk go test ./...
92 passed in 12 packages

rtk go test -race ./internal/service ./internal/httpapi ./internal/store/redisstore
43 passed in 3 packages

rtk go vet ./...
no issues found
```

## Concerns

None.
