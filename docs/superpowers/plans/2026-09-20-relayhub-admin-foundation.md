# RelayHub Admin Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the legacy static console with an embedded React/Vite Admin shell and add cluster-safe browser Admin sessions with CSRF protection.

**Architecture:** `web/admin` builds a deterministic SPA under `/admin/`; the backend generator validates and embeds only `web/admin/dist`. The bootstrap Admin bearer credential is exchanged for a Secure HttpOnly cookie while Redis owns revocable idle/absolute session state and the browser holds only the matching CSRF token in memory.

**Tech Stack:** Go 1.27, Redis 7.4, React 18, TypeScript 5, Vite 7, React Router 7, TanStack Query 5, Vitest, Testing Library, Playwright.

**Spec:** `docs/superpowers/specs/2026-09-20-relayhub-platform-expansion-design.md` sections 6-8, 19, 21-22, and delivery-sequence item 4.

## Global Constraints

- No Python SDK, Python package, or reverse-proxy implementation.
- PostgreSQL/NATS remain durable truth; Admin sessions and CSRF state are TTL-bound Redis data.
- Bootstrap Admin credentials never enter local storage, session storage, URLs, logs, analytics, or exported UI state.
- The cookie is `Secure`, `HttpOnly`, `SameSite=Strict`, `Path=/`, and has no `Domain` attribute.
- State-changing cookie-authenticated requests require `X-RelayHub-CSRF`; bearer automation remains backward compatible and does not require browser CSRF.
- Admin source has no runtime CDN dependency and the backend embeds no public Docusaurus content.
- All list/read feature pages beyond the shell remain owned by later Admin read/API plans; this plan ships honest unavailable/coming-next states, not mock production data.

## Review Focus

- A replayed or guessed CSRF value must not authorize a different session; Task 3 hashes and constant-time compares the token against the session-bound record.
- Concurrent requests around idle expiry must not resurrect an expired session; Task 2 uses one atomic Redis script for validate-and-touch.
- A stale generated Admin bundle must fail tests/build instead of silently shipping old assets; Tasks 1 and 4 pin manifest and generation determinism.
- React Router deep links such as `/admin/events/evt_1` must return the SPA entry while real missing assets return 404; Task 4 tests both classes.
- Secrets must remain absent from storage, logs, URL, DOM after login, and Playwright traces; Tasks 3, 5 and 6 test redaction and browser storage.

---

### Task 1: Scaffold the deterministic React/Vite Admin application

**Files:**

- Delete: `web/admin/legacy/`
- Create: `web/admin/package.json`
- Create: `web/admin/package-lock.json`
- Create: `web/admin/tsconfig.json`
- Create: `web/admin/vite.config.ts`
- Create: `web/admin/index.html`
- Create: `web/admin/src/main.tsx`
- Create: `web/admin/src/app/App.tsx`
- Create: `web/admin/src/app/providers.tsx`
- Create: `web/admin/src/styles/tokens.css`
- Create: `web/admin/src/styles/global.css`
- Create: `web/admin/tests/build-contract.test.ts`

**Interfaces:**

- Consumes: backend route prefix `/admin/` and Node 20+.
- Produces: `npm run typecheck`, `npm test`, and deterministic `npm run build`; `dist/index.html`, hashed `dist/assets/*`, and `dist/.vite/manifest.json` use base `/admin/`.

- [ ] **Step 1: Write the failing build-contract test**

Create a Vitest test that runs the production build in a temporary output directory and asserts:

```ts
expect(index).toContain('src="/admin/assets/')
expect(index).toContain('href="/admin/assets/')
expect(Object.keys(manifest)).toContain('index.html')
expect(index).not.toMatch(/https?:\/\//)
```

Also build twice and compare sorted relative paths plus SHA-256 hashes.

- [ ] **Step 2: Run the test and verify RED**

Run: `npm --prefix web/admin test -- --run tests/build-contract.test.ts`

Expected: FAIL because the Admin package and Vite build do not exist.

- [ ] **Step 3: Implement the minimal package and shell**

Use scripts:

```json
{
  "dev": "vite",
  "typecheck": "tsc --noEmit",
  "test": "vitest run",
  "build": "vite build --emptyOutDir"
}
```

Configure `base: "/admin/"`, emit a manifest, target modern evergreen browsers, and disable source maps in production. Mount a semantic `<main>` with the text `RelayHub Admin` and no network request yet.

- [ ] **Step 4: Install and verify GREEN**

Run:

```bash
npm --prefix web/admin install
npm --prefix web/admin run typecheck
npm --prefix web/admin test
npm --prefix web/admin run build
```

Expected: all commands pass and `dist` contains only local assets.

- [ ] **Step 5: Commit**

```bash
git add web/admin
git commit -m "feat: scaffold React Admin application"
```

---

### Task 2: Add atomic idle and absolute Redis Admin sessions

**Files:**

- Modify: `backend/internal/redisstate/session.go`
- Modify: `backend/internal/redisstate/session_integration_test.go`
- Modify: `backend/internal/httpapi/horizontal_scale_integration_test.go`

**Interfaces:**

- Consumes: `RedisSessionStore` and `Keyspace.AdminSession` from the scale foundation.
- Produces:

```go
type AdminSession struct {
    ID string
    CSRFHash string
    IssuedAt time.Time
    LastSeenAt time.Time
    IdleExpiresAt time.Time
    ExpiresAt time.Time
}

func (s *RedisSessionStore) Put(context.Context, AdminSession) error
func (s *RedisSessionStore) ValidateAndTouch(context.Context, string, time.Time, time.Duration) (AdminSession, error)
func (s *RedisSessionStore) Delete(context.Context, string) error
```

Extend `SessionStore` with the same `ValidateAndTouch` signature so runtime and
service code depend on the interface, not the Redis concrete type.

- [ ] **Step 1: Write failing Redis integration tests**

Cover: cross-client read/touch, idle extension capped at absolute expiry, no resurrection after idle expiry, deletion/revocation, corrupt-record removal, and 50 concurrent touches producing one valid bounded record. Use a real Redis container and Redis server time semantics.

- [ ] **Step 2: Run and verify RED**

Run: `go -C backend test -race -tags=integration ./internal/redisstate -run AdminSession -count=1`

Expected: FAIL because `ValidateAndTouch` and the new timestamps do not exist.

- [ ] **Step 3: Implement versioned atomic validation/touch**

Use one Lua script to read, decode, reject/delete an idle- or absolute-expired record, derive server time via `TIME`, set `last_seen_at_us`, set `idle_expires_at_us=min(now+idle, absolute)`, and update key TTL to the earlier bound. Return normalized `ErrNotFound`, `ErrCorruptRecord`, `ErrInvalidRecord`, or `ErrUnavailable`; never return Redis error text.

- [ ] **Step 4: Update multi-replica fixtures and verify GREEN**

Run:

```bash
go -C backend test ./internal/redisstate ./internal/httpapi -count=1
go -C backend test -race -tags=integration ./internal/redisstate ./internal/httpapi -run 'AdminSession|HorizontalScaleFoundation' -count=1 -timeout=180s
```

Expected: all tests pass across two Redis clients/API graphs.

- [ ] **Step 5: Commit**

```bash
git add backend/internal/redisstate backend/internal/httpapi/horizontal_scale_integration_test.go
git commit -m "feat: add revocable Redis Admin sessions"
```

---

### Task 3: Implement session exchange, cookie authentication and CSRF

**Files:**

- Create: `backend/internal/service/admin_sessions.go`
- Create: `backend/internal/service/admin_sessions_test.go`
- Create: `backend/internal/httpapi/admin_session.go`
- Create: `backend/internal/httpapi/admin_session_test.go`
- Modify: `backend/internal/httpapi/auth.go`
- Modify: `backend/internal/httpapi/router.go`
- Modify: `backend/internal/runtime/runtime.go`
- Modify: `backend/cmd/relayhub/main.go`
- Modify: `web/docs/static/openapi.json`

**Interfaces:**

- Consumes: `redisstate.SessionStore`, bootstrap `RELAYHUB_ADMIN_TOKEN`, and runtime clock/random sources.
- Produces:

```go
const AdminCookieName = "__Host-relayhub_admin"

type AdminSessionService struct {
    store redisstate.SessionStore
    bootstrapHash [32]byte
    now func() time.Time
    random io.Reader
    idleTTL time.Duration
    absoluteTTL time.Duration
}
func NewAdminSessionService(store redisstate.SessionStore, bootstrapToken string, now func() time.Time, random io.Reader) (*AdminSessionService, error)
func (s *AdminSessionService) Exchange(context.Context, string) (sessionID, csrfToken string, expiresAt time.Time, err error)
func (s *AdminSessionService) Authenticate(context.Context, sessionID, csrfToken string, mutate bool) error
func (s *AdminSessionService) Logout(context.Context, sessionID string) error
```

HTTP contract:

```text
POST   /api/v1/admin/session  Authorization: Bearer <bootstrap>
200 {"csrf_token":"...","expires_at":"..."} + Secure HttpOnly cookie
DELETE /api/v1/admin/session  cookie + X-RelayHub-CSRF
204 + expired cookie
```

- [ ] **Step 1: Write failing service and HTTP tests**

Test valid exchange, invalid bootstrap token, random failure, idle/absolute expiry, logout revocation, cookie attributes, malformed/missing cookie, missing/wrong CSRF, safe GET without CSRF, mutating request with CSRF, bearer compatibility, generic 401/403 bodies, and request-log redaction.

- [ ] **Step 2: Run and verify RED**

Run: `go -C backend test ./internal/service ./internal/httpapi -run 'AdminSession|AdminAuthentication|CSRF' -count=1`

Expected: FAIL because the service, routes and combined middleware are absent.

- [ ] **Step 3: Implement the service and middleware**

Generate independent 256-bit base64url session and CSRF values, persist only SHA-256 of CSRF, use constant-time comparisons, set a 30-minute idle and 12-hour absolute lifetime, and cap cookies to absolute expiry. `adminAuthentication` accepts either exact bearer bootstrap authentication or the session cookie. Require CSRF only when cookie auth handles a non-GET/HEAD/OPTIONS request.

- [ ] **Step 4: Wire runtime and contracts**

Expose the runtime `Sessions` store to `serveAPI`, construct `AdminSessionService`, register session exchange/logout outside and inside the proper middleware boundary, and add OpenAPI schemas/security descriptions. Never expose bootstrap or CSRF values to logs.

- [ ] **Step 5: Verify GREEN**

Run:

```bash
go -C backend test -race ./internal/service ./internal/httpapi ./internal/runtime ./cmd/relayhub -count=1
sh backend/scripts/check-contracts.sh --static
```

Expected: tests and contract parity pass.

- [ ] **Step 6: Commit**

```bash
git add backend/internal/service backend/internal/httpapi backend/internal/runtime backend/cmd web/docs/static/openapi.json
git commit -m "feat: secure browser Admin sessions"
```

---

### Task 4: Embed the Vite build with strict SPA fallback

**Files:**

- Modify: `backend/web/generate.go`
- Modify: `backend/web/generate_test.go`
- Modify: `backend/web/cmd/gendocs/main.go`
- Modify: `backend/internal/httpapi/router.go`
- Modify: `backend/internal/httpapi/routes_test.go`
- Modify: `Dockerfile`
- Modify: `.dockerignore`

**Interfaces:**

- Consumes: Task 1 `web/admin/dist` including `.vite/manifest.json`.
- Produces: deterministic `backend/web/embed.go`; `/admin/` and route-like deep links serve `index.html`, while missing filenames/extensions return JSON 404.

- [ ] **Step 1: Write failing generator/router tests**

Require `index.html`, at least one manifest entry and every referenced hashed asset. Reject symlinks, absolute paths, `..`, missing assets, external script/style URLs, source maps and files over 5 MiB. Test `/admin/events/evt_1` => HTML 200, `/admin/assets/missing.js` => 404, `/admin/openapi.json` => 404, and `/docs/*` => API 404.

- [ ] **Step 2: Run and verify RED**

Run: `go -C backend test ./web ./internal/httpapi -run 'Admin|Generate|SPA' -count=1`

Expected: FAIL because generation still consumes `legacy` and router has no deep-link fallback.

- [ ] **Step 3: Implement validated dist generation and fallback**

Point generation to `../../web/admin/dist`. Keep sorted deterministic output. The handler serves exact files first; only extensionless GET/HEAD paths with an HTML `Accept` header fall back to `index.html`. Never fall back for `/assets/`, a path containing an extension, or a non-GET/HEAD method.

- [ ] **Step 4: Update Docker build order**

Add a Node build stage using the lockfile:

```dockerfile
RUN npm ci && npm run typecheck && npm test && npm run build
```

Copy only `web/admin/dist` into the Go build stage before `go generate ./web`; do not copy `node_modules` or public docs into the runtime image.

- [ ] **Step 5: Verify determinism and image**

Run:

```bash
npm --prefix web/admin ci
npm --prefix web/admin run build
go -C backend generate ./web
sha256sum backend/web/embed.go
go -C backend generate ./web
sha256sum backend/web/embed.go
go -C backend test ./web ./internal/httpapi -count=1
docker build -t relayhub:admin-foundation .
```

Expected: hashes match; tests and image build pass.

- [ ] **Step 6: Commit**

```bash
git add backend/web backend/internal/httpapi Dockerfile .dockerignore
git commit -m "build: embed deterministic Admin SPA"
```

---

### Task 5: Build the authenticated Admin shell and resilient API client

**Files:**

- Create: `web/admin/src/api/client.ts`
- Create: `web/admin/src/api/types.ts`
- Create: `web/admin/src/auth/AuthProvider.tsx`
- Create: `web/admin/src/auth/LoginPage.tsx`
- Create: `web/admin/src/layout/AdminLayout.tsx`
- Create: `web/admin/src/layout/Navigation.tsx`
- Create: `web/admin/src/components/AsyncState.tsx`
- Create: `web/admin/src/pages/OverviewPage.tsx`
- Create: `web/admin/src/pages/FeatureBoundaryPage.tsx`
- Create: `web/admin/src/pages/NotFoundPage.tsx`
- Modify: `web/admin/src/app/App.tsx`
- Create: `web/admin/tests/auth.test.tsx`
- Create: `web/admin/tests/navigation.test.tsx`
- Create: `web/admin/tests/redaction.test.ts`

**Interfaces:**

- Consumes: Task 3 session endpoints.
- Produces: `AdminApiClient.request<T>()`, in-memory CSRF state, protected React Router routes for Overview, Events, Dead Letters, Apps, Routing Rules, Realtime Studio, Audit Logs and System.

- [ ] **Step 1: Write failing UI tests**

Test login loading/error/success, raw token cleared from the input and absent from Web Storage, cookie-authenticated request with `credentials:"same-origin"`, CSRF only on mutation, automatic transition to login on 401, explicit logout, keyboard navigation, focus restoration, reduced motion, mobile navigation and redaction of token-like values from displayed/exported errors.

- [ ] **Step 2: Run and verify RED**

Run: `npm --prefix web/admin test -- --run tests/auth.test.tsx tests/navigation.test.tsx tests/redaction.test.ts`

Expected: FAIL because the auth provider, client and route shell do not exist.

- [ ] **Step 3: Implement API/auth state**

Keep the bootstrap token only in the submit handler local variable; immediately clear the controlled field after the exchange settles. Keep CSRF in React memory only. Use an `AbortController`, structured `ApiError`, JSON content-type checks, bounded error text and central redaction for bearer values, signatures, API keys, socket tokens and cookie-like strings.

- [ ] **Step 4: Implement the responsive shell**

Use semantic landmarks, skip link, visible focus, 44px touch targets, high-contrast design tokens, `prefers-reduced-motion`, desktop sidebar and mobile drawer. Feature routes explain which backend plan enables live data and never invent metrics/events.

- [ ] **Step 5: Verify GREEN**

Run:

```bash
npm --prefix web/admin run typecheck
npm --prefix web/admin test
npm --prefix web/admin run build
```

Expected: all checks pass.

- [ ] **Step 6: Commit**

```bash
git add web/admin
git commit -m "feat: add authenticated Admin shell"
```

---

### Task 6: Add browser acceptance, docs and the Admin foundation release gate

**Files:**

- Create: `web/admin/playwright.config.ts`
- Create: `web/admin/e2e/admin-auth.spec.ts`
- Modify: `web/admin/package.json`
- Modify: `README.md`
- Modify: `docs/operations/runbook.md`
- Modify: `web/docs/docs/control-panel/overview.md`
- Modify: `web/docs/static/security.md`
- Modify: `web/docs/static/llms-full.txt`
- Create: `.github/workflows/admin-ci.yml`

**Interfaces:**

- Consumes: embedded SPA and session API from Tasks 1-5.
- Produces: desktop/mobile browser proof, Admin CI, and operator documentation for cookie/session behavior.

- [ ] **Step 1: Write Playwright acceptance tests**

Against an ephemeral real backend/Redis/PostgreSQL/NATS stack, test desktop and mobile login, deep-link reload, logout cluster revocation, wrong CSRF rejection, 401 recovery, keyboard-only navigation, no token in local/session storage or URL, and no secret in captured console/network failure text. Redact traces and retain them only on failure.

- [ ] **Step 2: Add Admin CI**

Use Node 20, `npm ci`, typecheck, Vitest, build, Go generation drift check and Playwright Chromium. Cache only npm downloads; upload build/test artifacts without `.env`, traces from passing tests, credentials or backend data volumes.

- [ ] **Step 3: Update docs and generated artifacts**

Document bootstrap-token exchange, cookie/CSRF properties, idle/absolute expiry, logout, `/admin/*` deep-link proxy requirement, `/docs/*` separation and the fact that feature data pages land in subsequent plans. Regenerate `llms-full.txt` and the integration Skill if its source changes.

- [ ] **Step 4: Run the complete gate**

Run:

```bash
npm --prefix web/admin ci
npm --prefix web/admin run typecheck
npm --prefix web/admin test
npm --prefix web/admin run build
go -C backend generate ./web
go -C backend test ./... -count=1
go -C backend test -race ./... -count=1
go -C backend test -race -tags=integration ./... -count=1 -timeout=300s
npm --prefix web/docs ci
npm --prefix web/docs run build
sh backend/scripts/build-llms.sh --check
sh backend/scripts/build-skill.sh --check
docker build -t relayhub:admin-foundation .
```

Expected: every command exits 0 and a second Admin build/generation produces no tracked diff.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/admin-ci.yml README.md docs/operations/runbook.md web/admin web/docs backend/web
git commit -m "test: close Admin foundation release gate"
```

## Plan Completion Evidence

This plan is complete only when all six task commits exist, the raw bootstrap token is absent from browser storage and logs, cookie authentication works across independent API/Redis clients, CSRF and idle/absolute expiry tests pass under race/integration modes, deep links work without masking missing assets, generated Admin bytes are deterministic, and the full release gate has fresh passing output.
