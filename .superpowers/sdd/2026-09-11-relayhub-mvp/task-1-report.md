# Task 1 implementation report

## Status

Implementation complete with one maintainability concern recorded below.

## TDD evidence

### Configuration RED

Command:

```text
rtk go test ./internal/config -run 'TestLoad' -v
```

Observed expected build failure before `config.Load` existed:

```text
Go test: 0 passed, 1 failed in 1 packages
config [build failed]
internal/config/config_test.go:49:14: undefined: Load
internal/config/config_test.go:66:14: undefined: Load
internal/config/config_test.go:121:14: undefined: Load
internal/config/config_test.go:152:14: undefined: Load
internal/config/config_test.go:170:14: undefined: Load
internal/config/config_test.go:185:12: undefined: Load
```

### Configuration GREEN

Command:

```text
rtk go test ./internal/config -run 'TestLoad' -v
```

Observed after the minimal configuration and store boundary implementation:

```text
Go test: 18 passed in 1 packages
```

### HTTP RED

After dependency checksums were resolved, command:

```text
rtk go test ./internal/httpapi -run 'Test(Health|Ready|Metrics|Docs|NotFound)' -v
```

Observed expected build failure before the router contract existed:

```text
Go test: 0 passed, 1 failed in 1 packages
httpapi [build failed]
internal/httpapi/operations_test.go:189:9: undefined: NewRouter
internal/httpapi/operations_test.go:189:19: undefined: Dependencies
```

The first implementation run exposed a real platform portability failure: `.md` was served as `text/plain` on macOS. Registering the Markdown MIME type fixed the contract.

### HTTP GREEN

Command:

```text
rtk go test ./internal/httpapi -run 'Test(Health|Ready|Metrics|Docs|NotFound)' -v
```

Observed:

```text
Go test: 12 passed in 1 packages
```

The complete package run, including JSON 405 and the 1 MiB body guard, observed:

```text
rtk go test ./internal/httpapi -v
Go test: 14 passed in 1 packages
```

## Implemented behavior

- Go 1.24 module using chi v5, go-redis v9, and the Prometheus Go client.
- Typed `config.Config` with the documented defaults, required secret validation, safe errors, positive duration parsing, Redis/HTTP/origin validation, normalized/deduplicated origins, and wildcard rejection.
- `store.HealthChecker`, Redis client construction/ping/close, and `platform.Clock` with `RealClock`.
- Chi router with request IDs, JSON panic recovery, 1 MiB request body guard, JSON not-found/method errors, liveness, Redis readiness, Prometheus metrics, and docs routes.
- API executable with Redis wiring, embedded public docs, JSON logging, and bounded graceful `SIGINT`/`SIGTERM` shutdown.
- Multi-stage non-root Docker image.
- Internal deployment documentation and public developer documentation reconciled with the shipped runtime variables, operations semantics, embedded docs, local commands, and verification steps.

## Files changed

- `.gitignore`
- `Dockerfile`
- `go.mod`
- `go.sum`
- `cmd/relayhub/main.go`
- `internal/config/config.go`
- `internal/config/config_test.go`
- `internal/httpapi/router.go`
- `internal/httpapi/operations.go`
- `internal/httpapi/operations_test.go`
- `internal/observability/metrics.go`
- `internal/platform/clock.go`
- `internal/store/store.go`
- `internal/store/redisstore/client.go`
- `web/embed.go`
- `docs/developer/deployment-stack.md`
- `public-docs/developer/README.md`
- `.superpowers/sdd/2026-09-11-relayhub-mvp/task-1-report.md`

## Verification evidence

```text
rtk go test ./internal/config ./internal/httpapi -v
Go test: 32 passed in 2 packages

rtk go test ./...
Go test: 32 passed in 8 packages

rtk go vet ./...
Go vet: No issues found

rtk go build ./cmd/relayhub
Go build: Success

rtk ./public-docs/scripts/test-docs.sh
[OK] Docs smoke test passed
endpoints checked: 10
markdown files scanned: 25
link scan errors: 0

rtk docker build -t relayhub:task1 .
exit 0; image relayhub:task1 created
```

Runtime smoke testing used the compiled binary on `127.0.0.1:18080` and verified `200 application/json` from `/healthz`, `200 application/json` from `/readyz` against the available local Redis, `200 text/html` from `/docs/`, and `200 text/plain` from `/docs/llms.txt`. A separate direct-binary run received `SIGTERM` and exited with status 0.

## Self-review

- Compared every Task 1 global constraint, required interface, named file, and prohibited future feature against the diff.
- Confirmed liveness never invokes the injected Redis health checker and readiness does.
- Confirmed readiness hides internal Redis errors and startup errors never contain configured secret values.
- Confirmed all JSON operational errors use the standard envelope.
- Confirmed every current file under `public-docs` is present in the generated embedded filesystem (25 source files and 25 embedded entries).
- Confirmed no application auth, event, WebSocket, worker, or function behavior was added.
- Public OpenAPI/JSON Schema did not change because Task 1 adds only operational and static-doc routes; the assigned human docs and existing `llms.txt` surfaces cover this task's public documentation changes.

## Concern

Go's native `go:embed` cannot reference the sibling `public-docs` directory from `web/embed.go`, and Task 1 restricts ownership to the exact named files. `web/embed.go` is therefore a generated compile-time `fstest.MapFS` snapshot containing every current public docs file. It is embedded in the binary and passes runtime/docs checks, but later public docs edits must regenerate this snapshot until the repository permits either moving the docs beneath `web/` or checking in a generator/embedded asset outside the Task 1 file list.
