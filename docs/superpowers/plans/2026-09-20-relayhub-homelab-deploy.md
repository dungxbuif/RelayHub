# RelayHub Homelab Deploy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the old RelayHub bootstrap container with the finished RelayHub production runtime on VM100.

**Architecture:** Build an immutable RelayHub image from `/Users/dungxbuif/workspace/RelayHub`, deploy API and worker on VM100, keep Pi5 as the PostgreSQL service-DB host, and run dedicated private NATS/Redis services for RelayHub runtime state. The API service joins both `relayhub-net` and `homelab-net`; Pi5 Traefik should route `relayhub.dungxbuif.com` to `relayhub-api:8080` for the staged Compose deployment, or to `tasks.relayhub-api:8080` after a later Swarm manager-side redeploy.

**Tech Stack:** Go RelayHub binary, embedded React Admin, Docker Swarm, Pi5 PostgreSQL/PgBouncer, NATS JetStream, Redis 7.4, Pi5 Traefik file provider.

**Spec:** `/Users/dungxbuif/workspace/RelayHub/docs/operations/runbook.md`

## Global Constraints

- Do not publish VM100 host ports for RelayHub HTTP.
- Keep PostgreSQL on Pi5; create a dedicated `relayhub` database and role if missing.
- Keep RelayHub NATS and Redis private to `relayhub-net`.
- Preserve rollback: do not delete the old bootstrap image until the new public route verifies.
- Never print generated secrets or interpolated Compose/Stack configs.

## Review Focus

- Public route must go to the new API service, not the old bootstrap `relayhub`.
- `/readyz` must check PostgreSQL, NATS and Redis; `/healthz` alone is not enough.
- Admin UI must load from `/admin/` through the same API route.
- NATS JetStream data must persist across container restart.
- Redis is ephemeral and private; no Redis host port should be opened.

---

### Task 1: Preflight And Candidate Build

**Files:**
- Read: `Dockerfile`
- Read: `compose.yaml`
- Read: `docs/operations/runbook.md`

**Interfaces:**
- Consumes: source checkout at `/Users/dungxbuif/workspace/RelayHub`
- Produces: image tag `homelab/relayhub:<date>-<shortsha>` loaded on VM100

- [x] Run RelayHub release smoke checks that fit the local environment: Go backend tests, TypeScript admin checks through Docker build, and Docker image build for `linux/amd64`.
- [x] Record the short git SHA and image tag.
- [x] Transfer the image to VM100 with `docker save | ssh vm100 docker load`.

### Task 2: Datastore And Secrets

**Files:**
- Create on VM100 while Pi5 SSH is unavailable: `/opt/apps/relayhub/prod/relayhub.env` with mode `0600`

**Interfaces:**
- Consumes: Pi5 PostgreSQL/PgBouncer endpoint
- Produces: private env file values for API and worker

- [x] Create or verify PostgreSQL role/database `relayhub` on Pi5.
- [x] Generate `RELAYHUB_ADMIN_TOKEN`, `RELAYHUB_SIGNING_SECRET`, `RELAYHUB_SECRET_ENCRYPTION_KEY`, `RELAYHUB_NATS_USERNAME`, `RELAYHUB_NATS_PASSWORD`, and `RELAYHUB_REDIS_PASSWORD` if no existing production values are present.
- [x] Store secrets on VM100 staged env file only, do not commit them. Pi5 secret placement is still pending because Pi5 SSH is blocked.

### Task 3: VM100 Runtime

**Files:**
- Create on VM100: `/opt/apps/relayhub/prod/compose.yml`
- Create on VM100: `/opt/apps/relayhub/prod/nats.conf`

**Interfaces:**
- Consumes: image tag from Task 1 and env from Task 2
- Produces: Compose containers `relayhub-api`, `relayhub-worker`, `relayhub-nats`, `relayhub-redis`

- [x] Create private Docker network `relayhub-net` on VM100.
- [x] Deploy private NATS and Redis services with persistent NATS volume and no host ports.
- [x] Deploy API and worker with healthchecks, read-only filesystem, dropped capabilities, and private env.
- [x] Scale down the old bootstrap service only after new services pass internal readiness and Pi5 manager access is available.

### Task 4: Edge Route And Verification

**Files:**
- Modify on Pi5: `/home/dungxbuif/edge/traefik/dynamic.yml`
- Modify docs after verification: homelab RelayHub deployment notes

**Interfaces:**
- Consumes: staged API container DNS `relayhub-api:8080`
- Produces: public route `https://relayhub.dungxbuif.com`

- [x] Back up Pi5 Traefik dynamic config.
- [x] Change RelayHub upstream to `http://relayhub-api:8080` for staged Compose, or `http://tasks.relayhub-api:8080` after Swarm redeploy.
- [x] Verify internal `/healthz` and `/readyz` for API and worker on VM100.
- [x] Verify public `/healthz`, `/readyz`, `/admin/`, `/metrics` after Pi5 edge cutover.
- [x] Update homelab and RelayHub docs with the staged runtime, rollback path, and verification evidence.

## 2026-09-20 Staging Evidence

- Candidate source: `main` at `48c37f0784c6`.
- Candidate image loaded on VM100: `homelab/relayhub:prod-20260920-48c37f0784c6`.
- Local gates passed: `npm run typecheck`, `npm test`, `npm run build`, `go -C backend generate ./web`, `go -C backend test ./...`, and Docker build.
- VM100 staged runtime: `/opt/apps/relayhub/prod/compose.yml` plus private `relayhub.env`, with containers `relayhub-api`, `relayhub-worker`, `relayhub-nats`, and `relayhub-redis`.
- Internal verification passed from VM100: API `/healthz`, API `/readyz`, worker `/readyz`, and `homelab-net` DNS access to `http://relayhub-api:8080/healthz`.
- Public `https://relayhub.dungxbuif.com/healthz`, `/readyz`, `/admin/` and `/metrics` verified after Pi5 Traefik cutover.
- Old Swarm bootstrap service `relayhub` scaled to `0/0`; Pi5's older local Compose `relayhub-*` containers are retained as a short-term rollback path and are no longer on the public route.
