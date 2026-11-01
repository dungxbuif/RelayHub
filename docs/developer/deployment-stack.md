# Deployment stack

RelayHub v1 release deployment runs one API container, one worker container, one
private PostgreSQL endpoint, one private NATS JetStream container and one
private Redis container. Local Compose places all services on the `relayhub`
network; the homelab deployment keeps PostgreSQL on the Pi5 service-DB host and
runs API, worker, NATS and Redis on VM100.

PostgreSQL is the authoritative store for applications, credentials, routing
rules, events, deliveries, idempotency and outbox rows. NATS JetStream is the
private broker for stream delivery, callback work, dead letters, realtime hints
and function routing. Redis is ephemeral runtime state for admin sessions,
rolling metrics and instance heartbeat/readiness surfaces; PostgreSQL and NATS
remain the recovery sources. Only the API publishes an HTTP port in local
Compose. In homelab production, no VM100 host port is published; the API joins
the shared edge network and Pi5 Traefik routes to the API container DNS name.

Required runtime secrets are `RELAYHUB_SIGNING_SECRET`, `RELAYHUB_POSTGRES_PASSWORD`,
`RELAYHUB_SECRET_ENCRYPTION_KEY`, `RELAYHUB_NATS_USERNAME` and
`RELAYHUB_NATS_PASSWORD`; deployments with password-authenticated Redis also
set `RELAYHUB_REDIS_PASSWORD`. The first Admin account is seeded with a one-shot
database job and is not stored in environment variables. Public docs and
`.env.example` expose the same configuration surface.

File messaging is enabled only when the complete `RELAYHUB_OBJECT_STORAGE_*`
group is present. Mobile push providers are independently optional. APNs requires
`RELAYHUB_APNS_ENDPOINT`, `RELAYHUB_APNS_AUTHORIZATION`, and
`RELAYHUB_APNS_TOPIC`; FCM requires `RELAYHUB_FCM_ENDPOINT`,
`RELAYHUB_FCM_AUTHORIZATION`, and `RELAYHUB_FCM_PROJECT`. Enabled groups must use
HTTPS and be complete. Provider authorization values are server-only rotating
secrets; never expose them to SDK clients or notification data.

## GitLab delivery

`.gitlab-ci.yml` mirrors the repository validation gates for the backend, Rust v1
runtime, SDKs, Admin, docs and generated contracts. Release tags are accepted
only when their commit is reachable from the protected `main` branch. The image
is pushed by digest to the project Registry; no tag pipeline deploys by itself.

The protected `deploy_prod` job is manual and deploys only the Rust v1 API image
to the existing Swarm stack on the Pi5 manager. It copies the versioned stack
file into a release directory, logs the manager into the Registry, deploys with
`--with-registry-auth`, then verifies the public API and Admin routes. Configure
these masked/protected GitLab variables before enabling the job:

- `RELAYHUB_SWARM_MANAGER_HOST`, `RELAYHUB_SWARM_MANAGER_USER`
- `RELAYHUB_SSH_PRIVATE_KEY`, `RELAYHUB_SSH_KNOWN_HOSTS`

The stack keeps the existing external PostgreSQL secret, networks and Admin
image. The API image is selected with `RELAYHUB_IMAGE` and is immutable in the
release job. A production deployment still requires a reviewed merge to the
protected `main` branch and an owner-approved `prod-*` tag.
