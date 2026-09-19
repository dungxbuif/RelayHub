# RelayHub Foundation and Horizontal Scale Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reorganize RelayHub into the approved monorepo boundaries and introduce
the Redis-backed, multi-replica-safe runtime foundation required by every Admin,
Realtime v2 and Queue v2 feature.

**Architecture:** The backend remains one Go module under `backend/`; the Go SDK
becomes a second module under `sdks/go`, joined by a root `go.work`. PostgreSQL
remains durable truth, NATS remains the private delivery plane, and a
`go-redis/v9` adapter provides bounded shared ephemeral state. Every API and
worker process receives a unique instance identity, includes Redis in readiness,
and can be added or removed without sticky sessions.

**Tech Stack:** Go 1.27.1, chi, pgx, NATS Core/JetStream,
`github.com/redis/go-redis/v9`, Redis 7.4, Docker Compose, Testcontainers, React
and Docusaurus workspace boundaries.

**Spec:** `docs/superpowers/specs/2026-09-20-relayhub-platform-expansion-design.md`

## Global Constraints

- Keep backend module path `github.com/dungxbuif/RelayHub`.
- Use `github.com/dungxbuif/RelayHub/sdks/go` for the standalone Go SDK module.
- PostgreSQL is authoritative for accepted events, deliveries, schedules and
  audit records; Redis must never be required to recover them.
- NATS and Redis stay private; public integrations use HTTPS or RFC 6455
  WebSockets.
- API and worker replicas require no sticky sessions and no singleton process.
- Redis keys are prefixed, TTL-bounded where applicable, and cluster-safe through
  explicit hash tags.
- Redis credentials never appear in URLs, logs, errors or diagnostics.
- Redis outage fails readiness and rejects new session, quota-sensitive and
  realtime-admission operations; already accepted durable work remains in
  PostgreSQL/NATS.
- Root Docker and Compose files remain at repository root; application source
  lives only under `backend/`, `web/` and `sdks/`.
- Python SDK and reverse-proxy implementation are excluded.
- Each task uses TDD for runtime behavior and ends with a focused commit.

## Review Focus

- Malformed or credential-bearing Redis addresses must fail configuration without
  echoing the submitted value; Task 3 pins this with table-driven tests.
- A Redis Cluster request spanning hash slots must be impossible through the
  public key API; Task 4 pins every multi-key primitive to one hash tag.
- A stale API replica must not delete a connection or heartbeat now owned by a
  newer process; Task 4 pins compare-and-delete and fencing behavior.
- Redis loss must make readiness fail without deleting or mutating durable
  PostgreSQL records; Tasks 5 and 6 exercise the outage boundary.
- Two replicas racing on one rate-limit bucket must never grant more than the
  configured limit; Tasks 4 and 6 run the atomic script concurrently.

## File and Responsibility Map

| Path | Responsibility |
| --- | --- |
| `go.work` | Join the backend and Go SDK modules for local development. |
| `backend/go.mod` | Backend dependency graph, including `go-redis/v9`. |
| `backend/cmd/relayhub/main.go` | Construct dependencies, assign instance ID and wire readiness. |
| `backend/internal/config/config.go` | Parse and validate Redis/runtime settings without leaking secrets. |
| `backend/internal/redisstate/client.go` | Redis standalone, Sentinel and Cluster construction plus health. |
| `backend/internal/redisstate/keys.go` | One canonical, cluster-safe key builder. |
| `backend/internal/redisstate/session.go` | TTL-bound Admin session persistence. |
| `backend/internal/redisstate/ratelimit.go` | Atomic distributed fixed-window limiter. |
| `backend/internal/redisstate/ownership.go` | Fenced connection ownership and instance heartbeats. |
| `backend/internal/teststack/stack.go` | Shared PostgreSQL/NATS/Redis integration fixture. |
| `web/admin/` | Reserved React/Vite application boundary; Admin plan owns product code. |
| `web/docs/` | Standalone Docusaurus source and public machine-readable artifacts. |
| `sdks/go/` | Independent official Go SDK module. |
| `sdks/typescript/` | Independent official TypeScript SDK package. |
| `compose.yaml` | Local API, worker, PostgreSQL, NATS and Redis stack. |

---

### Task 1: Establish monorepo boundaries without changing behavior

**Files:**

- Create: `go.work`
- Create: `sdks/go/go.mod`
- Move: `cmd/`, `internal/`, `web/`, `deploy/`, `scripts/`, `go.mod`, `go.sum`
  into `backend/`
- Move: `sdk/go/*` into `sdks/go/`
- Move: `sdk/typescript/*` into `sdks/typescript/`
- Move: `docs-site/*` into `web/docs/`
- Move: `public-docs/*` into `web/docs/static/`
- Modify: `README.md`
- Modify: `.gitignore`

**Interfaces:**

- Consumes: current backend module path and public v1 HTTP/WebSocket contracts.
- Produces: `go work` workspace with `./backend` and `./sdks/go`; canonical
  commands `go -C backend test ./...`, `go -C sdks/go test ./...`, and
  `npm --prefix sdks/typescript test`.

- [ ] **Step 1: Record the pre-move verification result**

Run:

```bash
go test ./... -count=1
go test ./sdk/go -count=1
npm --prefix sdk/typescript test
```

Expected: all three commands exit 0. Save the command and result in the commit
message body if an existing test is skipped by a build tag.

- [ ] **Step 2: Move tracked source with Git-aware operations**

Run:

```bash
mkdir -p backend web sdks/go sdks/typescript
git mv cmd internal deploy scripts backend/
git mv go.mod go.sum backend/
git mv web backend/web
git mv sdk/go/* sdks/go/
git mv sdk/typescript/* sdks/typescript/
git mv docs-site web/docs
git mv public-docs web/docs/static
rmdir sdk/go sdk/typescript sdk
```

Expected: `git status --short` reports renames rather than duplicate source
trees; root has no `cmd/`, `internal/`, `sdk/`, `docs-site/` or `public-docs/`.

- [ ] **Step 3: Create the workspace and Go SDK module**

Create `go.work`:

```go
go 1.27.1

use (
	./backend
	./sdks/go
)
```

Create `sdks/go/go.mod`:

```go
module github.com/dungxbuif/RelayHub/sdks/go

go 1.27.1

require github.com/gorilla/websocket v1.5.3
```

Run:

```bash
go work sync
go -C sdks/go mod tidy
```

Expected: both commands exit 0 and `sdks/go/go.sum` contains the WebSocket
dependency checksum.

- [ ] **Step 4: Update repository-path references mechanically**

Apply these path mappings to Markdown, shell scripts and package manifests:

```text
go test ./...                         -> go -C backend test ./...
go generate ./web                    -> go -C backend generate ./web
sdk/go                               -> sdks/go
sdk/typescript                       -> sdks/typescript
docs-site                            -> web/docs
public-docs                          -> web/docs/static
./scripts/                           -> ./backend/scripts/
```

Do not rewrite historical evidence under `docs/reviews/` or the superseded
2026-09-11 and 2026-09-12 specs/plans; those files describe the repository state
at their recorded dates.

- [ ] **Step 5: Verify the moved modules**

Run:

```bash
go -C backend test ./... -count=1
go -C sdks/go test ./... -count=1
npm --prefix sdks/typescript test
npm --prefix web/docs run build
```

Expected: all commands exit 0. Runtime route tests must produce the same status,
content type and body assertions as before the move.

- [ ] **Step 6: Commit the repository boundary**

```bash
git add go.work backend web sdks README.md .gitignore docs
git commit -m "refactor: establish backend web and sdk boundaries"
```

---

### Task 2: Rebuild deterministic generation and container paths

**Files:**

- Modify: `backend/web/generate.go`
- Modify: `backend/web/generate_test.go`
- Modify: `backend/web/cmd/gendocs/main.go`
- Modify: `backend/internal/httpapi/router.go`
- Modify: `backend/internal/httpapi/routes.go`
- Modify: `backend/internal/httpapi/routes_test.go`
- Modify: `backend/scripts/build-llms.sh`
- Modify: `backend/scripts/build-skill.sh`
- Modify: `backend/scripts/check-contracts.sh`
- Modify: `backend/scripts/check-docs.py`
- Modify: `Dockerfile`
- Modify: `.dockerignore`
- Modify: `web/docs/package.json`
- Regenerate: `backend/web/embed.go`

**Interfaces:**

- Consumes: Docusaurus/public artifacts under `web/docs/`.
- Produces: `go -C backend generate ./web`, a backend binary embedding only
  Admin assets at `/admin/`, and no backend-owned `/docs/` route. During this
  foundation plan, the existing console remains the embedded Admin payload; the
  Admin plan replaces it with the Vite build.

- [ ] **Step 1: Write failing generator path tests**

Update `backend/web/generate_test.go` so its source fixture resolves from
`../../web/docs/static`, and add:

```go
func TestPublicDocsAreNotEmbeddedAfterBoundarySplit(t *testing.T) {
	for name := range Admin {
		if strings.HasSuffix(name, ".md") || name == "openapi.json" || name == "asyncapi.yaml" {
			t.Fatalf("public documentation embedded as %q", name)
		}
	}
}
```

Run:

```bash
go -C backend test ./web -run 'TestGenerate|TestPublicDocsAreNotEmbedded' -count=1
```

Expected: FAIL because the existing generated map still contains public docs.

- [ ] **Step 2: Change the generator input to the legacy Admin entrypoint only**

Create `web/admin/legacy/` and move these files from `web/docs/static/`:

```text
console.html
assets/console.js
assets/docs.css
assets/docs.js
```

Change the generated variable from `Public` to `Admin` and the generation
directive to:

```go
//go:generate go run ./cmd/gendocs -source ../../web/admin/legacy -output embed.go
```

Keep `Generate(fs.FS)` deterministic. Change `httpapi.Dependencies.Docs` to
`Admin fs.FS`, mount the generated filesystem at `/admin/`, redirect `/admin` to
`/admin/`, and remove the backend `/docs/` mount. Add route tests proving
`/admin/console.html` is available, `/admin/missing` does not escape the
filesystem, and `/docs/intro` returns the normal API 404. The later Admin plan
adds SPA fallback only for paths emitted by React Router.

- [ ] **Step 3: Make documentation scripts repository-root independent**

Every shell script must derive the repository root from its own location:

```sh
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(CDPATH= cd -- "$script_dir/../.." && pwd)
docs_dir="$repo_dir/web/docs"
```

Use `docs_dir` for OpenAPI, schemas, `llms.txt`, `llms-full.txt` and Skill input;
never depend on the caller's current directory.

- [ ] **Step 4: Update the multi-stage Docker build**

The build stage must copy only explicit inputs and run:

```dockerfile
WORKDIR /src/backend
COPY backend/go.mod backend/go.sum ./
RUN go mod download
COPY backend/cmd ./cmd
COPY backend/internal ./internal
COPY backend/web ./web
COPY backend/scripts ./scripts
COPY web/docs /src/web/docs
RUN go generate ./web \
 && sh scripts/build-skill.sh --check \
 && sh scripts/build-llms.sh --check \
 && sh scripts/check-contracts.sh --static
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/relayhub ./cmd/relayhub
```

The final image remains distroless and contains only `/relayhub` plus CA roots.

- [ ] **Step 5: Regenerate and verify byte stability**

Run twice:

```bash
go -C backend generate ./web
sha256sum backend/web/embed.go web/docs/static/llms.txt web/docs/static/llms-full.txt
go -C backend generate ./web
sha256sum backend/web/embed.go web/docs/static/llms.txt web/docs/static/llms-full.txt
```

Expected: corresponding hashes from both runs are identical. Then run:

```bash
go -C backend test ./web ./internal/httpapi -count=1
docker build -t relayhub:foundation .
```

Expected: both commands exit 0.

- [ ] **Step 6: Commit deterministic build paths**

```bash
git add backend/web backend/scripts backend/internal/httpapi web/admin web/docs Dockerfile .dockerignore
git commit -m "build: restore generation across monorepo boundaries"
```

---

### Task 3: Add validated Redis configuration and client lifecycle

**Files:**

- Modify: `backend/go.mod`
- Modify: `backend/go.sum`
- Modify: `backend/internal/config/config.go`
- Modify: `backend/internal/config/config_test.go`
- Create: `backend/internal/redisstate/client.go`
- Create: `backend/internal/redisstate/client_test.go`
- Create: `backend/internal/redisstate/client_integration_test.go`

**Interfaces:**

- Consumes: `RELAYHUB_REDIS_*` settings from the design spec.
- Produces:

```go
type Mode string

const (
	ModeStandalone Mode = "standalone"
	ModeSentinel   Mode = "sentinel"
	ModeCluster    Mode = "cluster"
)

type Config struct {
	Mode           Mode
	Addrs          []string
	Username       string
	Password       string
	SentinelMaster string
	DB             int
	TLS            bool
	KeyPrefix      string
	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	PoolSize       int
}

func New(ctx context.Context, cfg Config) (*Client, error)
func (c *Client) Ping(ctx context.Context) error
func (c *Client) Close() error
func (c *Client) Universal() redis.UniversalClient
```

- [ ] **Step 1: Write failing configuration tests**

Add table-driven tests covering:

```go
func TestLoadParsesRedisModes(t *testing.T)
func TestLoadRejectsRedisCredentialsInAddresses(t *testing.T)
func TestLoadRequiresSentinelMasterOnlyForSentinel(t *testing.T)
func TestLoadRejectsClusterDatabaseSelection(t *testing.T)
func TestLoadDoesNotExposeRedisSecrets(t *testing.T)
func TestLoadRejectsUnboundedRedisPool(t *testing.T)
```

Use valid examples `redis.internal:6379`, `redis-a:6379,redis-b:6379`; reject
schemes, userinfo, paths, empty hosts, duplicate addresses, pool sizes outside
`1..4096`, standalone/Sentinel database numbers outside `0..15`, non-zero Cluster
database numbers, and key prefixes outside `[a-z0-9:_-]{1,32}`.

Run:

```bash
go -C backend test ./internal/config -run Redis -count=1
```

Expected: FAIL because `Config.Redis` and its parser do not exist.

- [ ] **Step 2: Implement the configuration contract**

Add `Redis redisstate.Config`-equivalent fields to the application config without
importing the adapter package into `internal/config`; use this local value type:

```go
type RedisConfig struct {
	Mode, Username, Password, SentinelMaster, KeyPrefix string
	Addrs []string
	TLS bool
	ConnectTimeout, ReadTimeout, WriteTimeout time.Duration
	PoolSize int
}
```

Defaults are `standalone`, `localhost:6379`, key prefix `rh`, 2-second connect,
1-second read/write timeouts, database zero and pool size 32. Empty credentials
remain valid for mTLS/private development deployments; Compose requires a
password. Credentials are never included in returned validation errors.

- [ ] **Step 3: Write failing client-construction tests**

Test exact adapter selection through an unexported option-builder:

```go
func TestBuildOptionsSelectsStandaloneSentinelAndCluster(t *testing.T)
func TestSafeAddressesRemoveCredentialsAndQueries(t *testing.T)
func TestNewFailsWhenInitialPingFails(t *testing.T)
```

Run:

```bash
go -C backend test ./internal/redisstate -count=1
```

Expected: FAIL because the package does not exist.

- [ ] **Step 4: Implement the Redis client**

Add `github.com/redis/go-redis/v9` and map the three modes to
`redis.NewClient`, `redis.NewFailoverClient` and `redis.NewClusterClient`.
Configure TLS 1.3 minimum when enabled, disable protocol retries during the
initial bounded ping, and normalize all connection errors to
`errors.New("Redis unavailable")` before they reach runtime logs.

- [ ] **Step 5: Prove lifecycle against a real Redis process**

Add an integration test using `redis:7.4-alpine` and Testcontainers:

```go
func TestClientPingAndClose(t *testing.T)
func TestClientAuthenticationFailureDoesNotLeakPassword(t *testing.T)
```

Run:

```bash
go -C backend test ./internal/config ./internal/redisstate -count=1
go -C backend test -tags=integration ./internal/redisstate -count=1 -timeout=90s
```

Expected: both commands exit 0 and test output contains no configured password.

- [ ] **Step 6: Commit Redis connectivity**

```bash
git add backend/go.mod backend/go.sum backend/internal/config backend/internal/redisstate
git commit -m "feat: add validated Redis connectivity"
```

---

### Task 4: Implement cluster-safe shared state primitives

**Files:**

- Create: `backend/internal/redisstate/keys.go`
- Create: `backend/internal/redisstate/keys_test.go`
- Create: `backend/internal/redisstate/session.go`
- Create: `backend/internal/redisstate/session_integration_test.go`
- Create: `backend/internal/redisstate/ratelimit.go`
- Create: `backend/internal/redisstate/ratelimit_integration_test.go`
- Create: `backend/internal/redisstate/ownership.go`
- Create: `backend/internal/redisstate/ownership_integration_test.go`

**Interfaces:**

- Consumes: `*redisstate.Client` from Task 3.
- Produces:

```go
type Keyspace struct{ Prefix string }
func (k Keyspace) AdminSession(id string) (string, error)
func (k Keyspace) RateLimit(appID, dimension, window string) (string, error)
func (k Keyspace) ConnectionOwner(connectionID string) (string, error)
func (k Keyspace) InstanceMember(instanceID string) (string, error)
func (k Keyspace) InstanceIndex() string

type AdminSession struct {
	ID, CSRFHash string
	IssuedAt, ExpiresAt time.Time
}
type SessionStore interface {
	Put(context.Context, AdminSession) error
	Get(context.Context, string) (AdminSession, error)
	Delete(context.Context, string) error
}

type RateDecision struct {
	Allowed bool
	Remaining float64
	ResetAt time.Time
	RetryAfter time.Duration
}
func (l *RateLimiter) Allow(context.Context, string, int64, time.Duration, int64) (RateDecision, error)

type ConnectionOwner struct { InstanceID string; Generation uint64 }
func (o *OwnershipStore) Claim(context.Context, string, string, string, uint64, time.Duration) error
func (o *OwnershipStore) Refresh(context.Context, string, string, string, uint64, time.Duration) error
func (o *OwnershipStore) Owner(context.Context, string, string) (ConnectionOwner, error)
func (o *OwnershipStore) Release(context.Context, string, string, string, uint64) error
func (o *OwnershipStore) Heartbeat(context.Context, Instance, time.Duration) error
func (o *OwnershipStore) LiveInstances(context.Context, time.Time, int64) ([]Instance, error)
```

- [ ] **Step 1: Write failing key validation tests**

Assert that empty identifiers, identifiers over 128 bytes, control characters,
braces and whitespace are rejected. Assert exact keys:

```text
rh:{session:sess_1}:admin
rh:{app:app_1}:rate:publish:1789920000
rh:{connection:conn_1}:owner
rh:{instances}:member:api_1
rh:{instances}:live
```

Run:

```bash
go -C backend test ./internal/redisstate -run Keyspace -count=1
```

Expected: FAIL because `Keyspace` does not exist.

- [ ] **Step 2: Implement the only permitted key builder**

Keep all raw key formatting private to `keys.go`. Return `ErrInvalidKeyPart`
instead of interpolating rejected input into errors. No runtime package may call
`fmt.Sprintf("rh:` directly; enforce this later with a repository search.

- [ ] **Step 3: Write failing session and ownership integration tests**

Cover session expiry, malformed JSON deletion, compare-and-delete, stale
generation refresh, newer-owner fencing, heartbeat expiry and bounded live
instance listing. Use two independent Redis clients to represent two API
replicas.

Run:

```bash
go -C backend test -tags=integration ./internal/redisstate \
  -run 'Session|Ownership|Heartbeat' -count=1 -timeout=90s
```

Expected: FAIL because the stores do not exist.

- [ ] **Step 4: Implement sessions and fenced ownership with atomic scripts**

Serialize sessions and instance records as versioned JSON. Use Lua scripts for
refresh and release so `instance_id` plus `generation` must match atomically.
Heartbeat writes the member key and updates the `{instances}` sorted set in one
hash slot; `LiveInstances` removes expired scores, caps reads at the requested
limit and never uses `KEYS` or unbounded `SCAN`.

- [ ] **Step 5: Write the concurrent limiter test before its script**

Start 64 goroutines split across two clients, each calling:

```go
decision, err := limiter.Allow(ctx, "rh:{app:app_1}:rate:publish", 17, time.Minute, 1)
```

Assert exactly 17 allowed decisions, no negative `Remaining`, and bounded shared
reset timestamps. Advance the injected clock/refill state and prove capacity is
restored at 17 tokens per minute without exceeding the burst. Add cases for
zero/negative limits or cost, windows outside `1s..24h`, cost above burst,
cancelled contexts and Redis outage.

Run:

```bash
go -C backend test -tags=integration ./internal/redisstate -run RateLimiter -count=1 -timeout=90s
```

Expected: FAIL because `RateLimiter.Allow` does not exist.

- [ ] **Step 6: Implement the limiter atomically**

Use one Lua token-bucket script with Redis `TIME`, hash fields `tokens` and
`last_ms`, floating-point refill capped at the configured burst, and `PEXPIRE`
long enough to remove an idle full bucket. The script returns allowed,
remaining, reset and retry-after values derived from server time. Any Redis error
returns no permissive decision, so callers fail closed.

- [ ] **Step 7: Verify all primitives and key discipline**

Run:

```bash
go -C backend test ./internal/redisstate -count=1
go -C backend test -race -tags=integration ./internal/redisstate -count=1 -timeout=120s
rg -n 'fmt\.Sprintf\("rh:|"rh:\{' backend --glob '*.go' --glob '!internal/redisstate/keys.go'
```

Expected: tests exit 0 and `rg` returns no matches.

- [ ] **Step 8: Commit shared ephemeral primitives**

```bash
git add backend/internal/redisstate
git commit -m "feat: add cluster-safe shared Redis state"
```

---

### Task 5: Wire instance identity, readiness and local Redis deployment

**Files:**

- Modify: `backend/cmd/relayhub/main.go`
- Modify: `backend/cmd/relayhub/main_test.go`
- Create: `backend/internal/platform/instance.go`
- Create: `backend/internal/platform/instance_test.go`
- Modify: `compose.yaml`
- Modify: `.env.example`
- Modify: `README.md`
- Modify: `docs/operations/runbook.md`

**Interfaces:**

- Consumes: Redis client, ownership store and existing `broker.CompositeHealth`.
- Produces:

```go
type Instance struct {
	ID string
	Role string
	Generation uint64
	StartedAt time.Time
}

func NewInstance(role, configuredID string, now time.Time) (Instance, error)
```

`RELAYHUB_INSTANCE_ID` is optional; generated IDs use
`<role>_<lowercase-uuid>`. Generation is a cryptographically random non-zero
`uint64`, not a process-local increment.

- [ ] **Step 1: Replace the legacy no-Redis assertions with failing scale tests**

Delete the `cmd/relayhub/main_test.go` assertion that Redis must be absent. Add:

```go
func TestRuntimeConstructsRedisBeforeServing(t *testing.T)
func TestRuntimeReadinessIncludesRedis(t *testing.T)
func TestRuntimeUsesDistinctInstanceGenerationAfterRestart(t *testing.T)
func TestWorkerKeepsDurableLoopRunningWhenRedisReadinessFails(t *testing.T)
```

Use dependency factories in the test rather than a live Redis process. Run:

```bash
go -C backend test ./cmd/relayhub -run 'Redis|Instance|WorkerKeeps' -count=1
```

Expected: FAIL against the NATS/PostgreSQL-only runtime.

- [ ] **Step 2: Implement instance identity and lifecycle wiring**

Construct Redis after configuration and before role-specific runtime wiring.
Append it to readiness for both API and worker. Start a heartbeat loop at one
third of its 30-second TTL; stop the loop and release only matching-generation
records during graceful shutdown. Do not stop the worker callback/outbox loop
merely because a later Redis ping fails.

- [ ] **Step 3: Add the local Redis service**

Add `relayhub-redis` with:

```yaml
image: redis:7.4-alpine
restart: unless-stopped
command: [redis-server, --save, "", --appendonly, "no", --requirepass, "$${REDIS_PASSWORD}"]
environment:
  REDIS_PASSWORD: "${RELAYHUB_REDIS_PASSWORD:?generate RELAYHUB_REDIS_PASSWORD}"
  REDISCLI_AUTH: "${RELAYHUB_REDIS_PASSWORD:?generate RELAYHUB_REDIS_PASSWORD}"
healthcheck:
  test: [CMD, redis-cli, ping]
  interval: 5s
  timeout: 3s
  retries: 12
networks: [relayhub]
```

Add it to the shared runtime `depends_on` with `condition: service_healthy`. Pass
standalone mode, `relayhub-redis:6379`, password, key prefix and timeout/pool
settings to API and worker. Do not publish Redis ports or add a persistent
volume; local Redis owns ephemeral state only.

- [ ] **Step 4: Update environment documentation without exposing credentials**

Document every setting from spec section 6.2. `.env.example` contains an empty
`RELAYHUB_REDIS_PASSWORD=` and non-secret defaults; README uses
`openssl rand -hex 32` and never places the password in an address.

- [ ] **Step 5: Verify role behavior and Compose**

Run:

```bash
go -C backend test ./cmd/relayhub ./internal/platform ./internal/config -count=1
docker compose config --quiet
```

Expected: both exit 0. Then run the stack and outage probe:

```bash
docker compose up --build -d --wait --wait-timeout 120
curl --fail http://localhost:8080/readyz
docker compose stop relayhub-redis
test "$(curl -sS -o /dev/null -w '%{http_code}' http://localhost:8080/readyz)" = 503
docker compose start relayhub-redis
docker compose up -d --wait --wait-timeout 60
curl --fail http://localhost:8080/readyz
```

Expected: readiness transitions 200 -> 503 -> 200 without restarting PostgreSQL,
NATS, API or worker.

- [ ] **Step 6: Commit runtime wiring**

```bash
git add backend/cmd backend/internal/platform compose.yaml .env.example README.md docs/operations/runbook.md
git commit -m "feat: wire Redis into horizontally scalable runtime"
```

---

### Task 6: Prove two-replica behavior and outage boundaries

**Files:**

- Create: `backend/internal/teststack/stack.go`
- Create: `backend/internal/teststack/stack_test.go`
- Create: `backend/internal/httpapi/horizontal_scale_integration_test.go`
- Modify: `backend/internal/realtime/nats_bridge_integration_test.go`
- Modify: `backend/internal/streamgateway/gateway_integration_test.go`

**Interfaces:**

- Consumes: real PostgreSQL, NATS and Redis containers plus two API dependency
  graphs and two worker dependency graphs.
- Produces:

```go
type Stack struct {
	PostgresURL string
	NATSURL string
	RedisAddr string
	RedisPassword string
}

func Start(t *testing.T) *Stack
func (s *Stack) StopRedis(t *testing.T)
func (s *Stack) StartRedis(t *testing.T)
```

The helper registers cleanup with `t.Cleanup`, redacts credentials in errors and
uses fixed container image tags matching Compose.

- [ ] **Step 1: Write a failing fixture self-test**

Add `TestStackStartsAllPrivateDependencies`, which opens clients, pings each
dependency, stops Redis, verifies only Redis ping fails, restarts Redis and
verifies recovery.

Run:

```bash
go -C backend test -tags=integration ./internal/teststack -count=1 -timeout=120s
```

Expected: FAIL because `teststack.Start` does not exist.

- [ ] **Step 2: Implement the shared integration stack**

Start `postgres:17-alpine`, `nats:2.14.5-alpine` with JetStream and
`redis:7.4-alpine`. Generate per-test credentials, map ports dynamically and
wait for protocol-level readiness. Never print container environment maps in
test failures.

- [ ] **Step 3: Write the two-replica acceptance test**

`TestHorizontalScaleFoundation` must:

1. construct API instance A and B against one stack;
2. create 40 Admin sessions alternately and read every session through the other
   instance;
3. make 200 concurrent limit requests through both replicas and assert the
   configured grant count exactly;
4. claim a connection on A, transfer it with a higher generation to B, and prove
   A cannot refresh or delete B's claim;
5. stop Redis and prove both readiness checks fail while a PostgreSQL event
   accepted before the outage remains queryable;
6. restart Redis and prove readiness recovers without recreating either process;
7. cancel instance A and prove B stays live and no durable record disappears.

Run:

```bash
go -C backend test -race -tags=integration ./internal/httpapi \
  -run TestHorizontalScaleFoundation -count=1 -timeout=180s
```

Expected: FAIL until the full runtime harness exposes injectable constructors.

- [ ] **Step 4: Extract a testable runtime constructor**

Move dependency assembly from `cmd/relayhub/main.go` into
`backend/internal/runtime/runtime.go` with:

```go
type Dependencies struct {
	Postgres *postgres.Client
	NATS *natsbroker.Client
	Redis *redisstate.Client
	Instance platform.Instance
}

func NewAPI(ctx context.Context, cfg config.Config, deps Dependencies, logger *slog.Logger) (*API, error)
func NewWorker(ctx context.Context, cfg config.Config, deps Dependencies, logger *slog.Logger) (*Worker, error)
```

`cmd/relayhub` remains process and signal orchestration only. `API.Ready` and
`Worker.Ready` use the same composite dependency check as HTTP `/readyz`.

- [ ] **Step 5: Run focused and regression suites**

Run:

```bash
go -C backend test ./internal/runtime ./cmd/relayhub -count=1
go -C backend test -race -tags=integration ./internal/teststack ./internal/httpapi ./internal/realtime ./internal/streamgateway -count=1 -timeout=240s
go -C backend test ./... -count=1
```

Expected: all commands exit 0. No test may depend on request affinity or sleep
longer than a configured lease/heartbeat deadline.

- [ ] **Step 6: Commit the scale proof**

```bash
git add backend/internal/teststack backend/internal/runtime backend/internal/httpapi backend/internal/realtime backend/internal/streamgateway backend/cmd
git commit -m "test: prove multi-replica scale foundation"
```

---

### Task 7: Close the foundation release gate

**Files:**

- Modify: `docs/architecture/overview.md`
- Create: `docs/architecture/adr/0003-redis-ephemeral-state.md`
- Modify: `docs/operations/runbook.md`
- Modify: `README.md`
- Modify: `web/docs/docs/intro.md`
- Modify: `web/docs/static/llms.txt`
- Modify: `web/docs/static/llms-full.txt`
- Modify: `web/docs/static/skills/relayhub-integration/SKILL.md`

**Interfaces:**

- Consumes: verified monorepo and multi-replica runtime.
- Produces: operator and integrator documentation that describes the shipped
  five-service development stack and durable/ephemeral ownership boundary.

- [ ] **Step 1: Write the ADR with explicit failure semantics**

Record:

```text
PostgreSQL: durable system of record.
NATS JetStream: durable work transport and wakeups.
Core NATS: ephemeral replica fan-out and control.
Redis: shared sessions, fences, ownership, rate limits and bounded live state.
Redis loss: no loss of accepted durable records; readiness fails and new
quota/session/realtime admission fails closed.
```

Also record why Redis Streams are reserved for bounded Realtime history and are
not Queue v2's durable settlement mechanism.

- [ ] **Step 2: Update docs and generated AI artifacts**

Document standalone, Sentinel and Cluster modes; key-prefix isolation; TLS/ACL
expectations; password rotation; readiness behavior; and the no-sticky-session
load-balancer contract. Remove statements that the product has no Redis path or
that only four services are canonical.

- [ ] **Step 3: Regenerate and run the complete foundation gate**

Run:

```bash
go -C backend generate ./web
go -C backend test ./... -count=1
go -C backend test -race ./... -count=1
go -C backend test -race -tags=integration ./... -count=1 -timeout=300s
go -C sdks/go test ./... -count=1
npm --prefix sdks/typescript ci
npm --prefix sdks/typescript test
npm --prefix web/docs ci
npm --prefix web/docs run build
docker compose config --quiet
docker build -t relayhub:foundation .
git diff --exit-code
```

Expected: every command exits 0 and generated artifacts leave no worktree diff.

- [ ] **Step 4: Commit foundation documentation**

```bash
git add README.md docs web/docs backend/web
git commit -m "docs: document Redis scale foundation"
```

## Plan Completion Evidence

This plan is complete only when all seven task commits exist, the Task 7 release
gate has fresh passing output, root source boundaries match the target layout,
and the two-replica acceptance test proves Redis outage and replica termination
do not lose durable data. Passing unit tests alone is insufficient.
