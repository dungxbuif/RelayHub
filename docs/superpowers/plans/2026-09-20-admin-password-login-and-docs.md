# Admin Password Login And Docs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace bootstrap Admin bearer-token login with seeded username/password Admin users, preserve cookie/CSRF sessions across UI navigation, and serve `/docs/` from the RelayHub binary.

**Architecture:** Add a PostgreSQL-backed `admin_users` table with PBKDF2-SHA256 password hashes and role `admin`. The Admin session service authenticates email/password, stores Redis sessions with an actor ID, and all Admin routes require session cookies rather than bearer tokens. The Go binary embeds a static docs filesystem and mounts it at `/docs/`.

**Tech Stack:** Go, PostgreSQL migrations, Redis sessions, PBKDF2-SHA256 via `golang.org/x/crypto/pbkdf2`, React Admin, Docusaurus/static docs assets.

**Spec:** User request in this thread on 2026-09-20.

## Global Constraints

- Seed exactly one Admin account email: `dungbui.dungbui.00@gmail.com`.
- Only Admin-authenticated sessions may create users.
- Remove bootstrap Admin token authentication for Admin APIs and UI login.
- Do not print generated passwords, session cookies, CSRF tokens, API keys, or password hashes.
- `/docs/` must return a public HTML page; machine-readable docs under `/docs/openapi.json`, `/docs/llms.txt`, and `/docs/llms-full.txt` must be fetchable.
- Keep app API-key/HMAC auth unchanged.

## Review Focus

- Existing bearer `RELAYHUB_ADMIN_TOKEN` no longer authorizes Admin routes.
- Login rejects unknown email and wrong password with the same 401 envelope.
- Admin cookie session survives navigation/reload by allowing UI to refresh CSRF from the cookie.
- User creation requires an authenticated Admin cookie + CSRF.
- `/docs/` and `/docs/openapi.json` work in the deployable binary.

---

### Task 1: Admin User Store And Password Verifier

**Files:**
- Create: `backend/internal/auth/password.go`
- Create: `backend/internal/auth/password_test.go`
- Create: `backend/internal/store/postgres/migrations/020_admin_users.sql`
- Create: `backend/internal/store/postgres/admin_users.go`
- Create: `backend/internal/store/postgres/admin_users_integration_test.go`
- Modify: `backend/internal/store/store.go`

**Interfaces:**
- Produces: `store.AdminUserStore` with `AuthenticateAdminUser`, `CreateAdminUser`, and `EnsureSeedAdminUser`.
- Produces: password hashes in format `pbkdf2-sha256$iterations$salt$hash`.

- [ ] Write failing password hash tests.
- [ ] Implement password hash/verify.
- [ ] Add migration and Postgres store methods.
- [ ] Add integration tests for seed idempotency, auth success/failure, and duplicate user conflict.
- [ ] Run targeted tests.

### Task 2: Session Login Without Bootstrap Token

**Files:**
- Modify: `backend/internal/service/admin_sessions.go`
- Modify: `backend/internal/service/admin_sessions_test.go`
- Modify: `backend/internal/redisstate/session.go`
- Modify: `backend/internal/httpapi/auth.go`
- Modify: `backend/internal/httpapi/admin_session.go`
- Modify: `backend/internal/httpapi/admin_session_test.go`
- Modify: `backend/internal/httpapi/router.go`
- Modify: `backend/internal/httpapi/routes.go`
- Modify: `backend/cmd/relayhub/main.go`

**Interfaces:**
- Consumes: `store.AdminUserStore`.
- Produces: `POST /api/v1/admin/session` body `{email,password}`.
- Produces: `GET /api/v1/admin/session` session refresh endpoint returning `csrf_token`, `expires_at`, and `user`.

- [ ] Write failing service and HTTP tests for password login, bearer rejection, CSRF refresh, and user creation auth.
- [ ] Update session records to include `UserID` and `Email`.
- [ ] Refactor middleware to cookie-only Admin auth.
- [ ] Wire Postgres user store into API runtime and seed Admin account on startup.
- [ ] Run targeted backend tests.

### Task 3: Admin UI Login Form

**Files:**
- Modify: `web/admin/src/auth/AuthProvider.tsx`
- Modify: `web/admin/src/auth/LoginPage.tsx`
- Modify: `web/admin/src/api/types.ts`
- Modify: relevant admin tests under `web/admin/tests`

**Interfaces:**
- Consumes: `POST /api/v1/admin/session` JSON login.
- Consumes: `GET /api/v1/admin/session` refresh.

- [ ] Write failing UI tests for email/password login and session refresh.
- [ ] Update provider and login page.
- [ ] Run `npm --prefix web/admin test` and build.

### Task 4: Public Docs Route

**Files:**
- Modify: `backend/web/generate.go`
- Modify: `backend/web/cmd/gendocs/main.go`
- Modify: `backend/web/generate_test.go`
- Modify: `backend/internal/httpapi/router.go`
- Modify: `backend/internal/httpapi/operations_test.go`
- Modify: `Dockerfile`

**Interfaces:**
- Produces: embedded `web.Docs fs.FS`.
- Produces: public `/docs/` route and static files.

- [ ] Write failing route test for `/docs/` and `/docs/openapi.json`.
- [ ] Generate embedded docs assets from `web/docs/static`.
- [ ] Mount docs handler.
- [ ] Run generation and docs/contracts checks.

### Task 5: Docs, Staging Deploy, And Verification

**Files:**
- Modify: `docs/operations/runbook.md`
- Modify: `docs/developer/auth.md`
- Modify: `docs/developer/registration-flow.md`
- Modify: `web/docs/static/developer/auth.md`
- Modify: `web/docs/static/developer/registration-flow.md`
- Modify: homelab docs after deploy.

**Interfaces:**
- Produces: staging image and VM100 rollout.

- [ ] Update docs for password Admin login and removed bootstrap token.
- [ ] Build image, run test gates, transfer to VM100.
- [ ] Update VM100 env to remove Admin token requirement and seed admin password securely.
- [ ] Deploy to staging and verify login, app creation, `/docs/`, and no bearer access.
