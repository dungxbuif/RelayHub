# Task 1 implementation report

## Status

Implementation complete. Fix round 1 resolved the original generated-snapshot concern with a reproducible generator and enforced parity gate.

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

## Fix round 1

The parent review reported two docs-routing defects, stale deployment instructions, the unreproducible embedded snapshot, and an incorrect container entrypoint in internal docs. This round fixed every finding and resolved the concern above.

### HTTP routing RED

Tests were added first for a non-GET docs request and a missing docs file.

```text
rtk go test ./internal/httpapi -run 'TestDocs(RejectsNonGET|MissingFile)' -v
Go test: 0 passed, 2 failed in 1 packages

TestDocsRejectsNonGETWithStandardJSONError:
POST docs status = 200, want 405; body = # Developer guide

TestDocsMissingFileUsesStandardJSONError:
Content-Type = "text/plain; charset=utf-8", want application/json
body = "404 page not found", want standard not_found JSON envelope
```

The router now registers `/docs/*` only for `GET`. The docs handler verifies the requested path in the injected filesystem before using `http.FileServerFS`; absent paths use the same standard JSON `not_found` response as other API routes.

### HTTP routing GREEN

```text
rtk go test ./internal/httpapi -run 'TestDocs(RejectsNonGET|MissingFile|ServesProductionSnapshot)' -v
Go test: 5 passed in 1 packages
```

The covering tests include non-GET, missing docs, and real `web.Public` Markdown/`llms.txt` requests with their production content types.

### Generator RED

The generator contract test was written before generator production code:

```text
rtk go test ./web -run 'TestGeneratedDocsSnapshotIsCurrent' -v
Go test: 0 passed, 1 failed in 1 packages
web/generate_test.go:12:20: undefined: Generate
```

After the deterministic generator was implemented, the deployment docs were corrected while leaving the old snapshot in place. Both drift gates then failed for their intended reason:

```text
rtk go test ./web -run 'Test(GeneratedDocsSnapshotIsCurrent|EmbeddedDocsMatchPublicDocs)' -v
Go test: 0 passed, 2 failed in 1 packages
web/embed.go differs from public-docs; run `go generate ./web`
embedded docs differ from public-docs
```

### Generator GREEN

```text
rtk go generate ./web
rtk go test ./web -run 'Test(GeneratedDocsSnapshotIsCurrent|EmbeddedDocsMatchPublicDocs)' -v
Go test: 2 passed in 1 packages
```

Two consecutive generation runs produced the same `web/embed.go` SHA-256:

```text
7f56dd60d16b5626df8de8f77bb2910e3e0c946b68a6a4e9b6afb2007cbee267
```

`Dockerfile` now runs `go test ./web` after copying the source and before compiling the binary, so a stale checked-in snapshot fails both normal tests and the image build.

### Documentation reconciliation

- `public-docs/deploy/README.md` now describes only Task 1 operations and embedded docs as currently available.
- `public-docs/deploy/docker-compose.relayhub.yml` is a runnable two-service Task 1 stack with the required secrets, Redis URL, AOF persistence, health checks, named volume, and no docs mount or nonexistent worker command.
- The final three-service API/worker/Redis topology remains clearly labeled as planned in the Compose extension and deployment guide.
- References to `Dockerfile.worker`, `/health`, current `/ws`, `RELAYHUB_DOCS_DIR`, docs mounts, fixed container names, and disabled AOF were removed from the two reviewed deployment artifacts.
- `docs/developer/deployment-stack.md` now names the actual `/usr/local/bin/relayhub` entrypoint and documents the generator/parity workflow.
- `public-docs/developer/README.md` documents GET-only docs routes, JSON docs errors, and the source-regeneration command.

### Fix files changed

- `Dockerfile`
- `docs/developer/deployment-stack.md`
- `internal/httpapi/router.go`
- `internal/httpapi/operations_test.go`
- `public-docs/deploy/README.md`
- `public-docs/deploy/docker-compose.relayhub.yml`
- `public-docs/developer/README.md`
- `web/embed.go`
- `web/generate.go`
- `web/generate_test.go`
- `web/cmd/gendocs/main.go`
- `.superpowers/sdd/2026-09-11-relayhub-mvp/task-1-report.md`

### Fix verification evidence

```text
rtk go test ./internal/httpapi ./web -v
Go test: 21 passed in 2 packages

rtk go test ./...
Go test: 39 passed in 9 packages

rtk go vet ./...
Go vet: No issues found

rtk gofmt -l cmd internal web
no output

rtk ./public-docs/scripts/test-docs.sh
[OK] Docs smoke test passed
endpoints checked: 10
markdown files scanned: 25
link scan errors: 0

rtk env RELAYHUB_ADMIN_TOKEN=test-admin RELAYHUB_SIGNING_SECRET=test-signing docker compose -f public-docs/deploy/docker-compose.relayhub.yml config
exit 0; two runnable services plus the explicitly planned three-service extension

rtk docker build -t relayhub:task1-fix1 .
exit 0; `RUN go test ./web` passed and image compiled

rtk git diff --check
no errors
```

### Fix self-review

- Confirmed docs file routes are GET-only and all tested non-GET/missing cases use the standard JSON error envelope.
- Confirmed tests route real production `web.Public` content through `httpapi.NewRouter`.
- Confirmed parity compares generated Go source and every regular embedded file against the real `public-docs` tree.
- Confirmed generator ordering and formatting are deterministic and `go generate ./web` is copyable from internal/public docs.
- Confirmed Docker enforces parity before binary compilation.
- Confirmed the Task 1 Compose file validates and contains no runnable worker service or unsupported current route/config claims.
- No remaining concerns from fix round 1.
