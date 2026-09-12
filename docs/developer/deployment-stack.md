# Deployment stack

RelayHub v1 release deployment runs one API container, one worker container, one
private PostgreSQL container and one private NATS JetStream container on the
`relayhub` network. Redis was part of the prototype path and is not in the v1
release runtime.

PostgreSQL is the authoritative store for applications, credentials, routing
rules, events, deliveries, idempotency and outbox rows. NATS JetStream is the
private broker for stream delivery, callback work, dead letters, realtime hints
and function routing. Only the API publishes an HTTP port; PostgreSQL, NATS and
the worker listener stay internal to the Compose project.

Required runtime secrets are `RELAYHUB_ADMIN_TOKEN`,
`RELAYHUB_SIGNING_SECRET`, `RELAYHUB_POSTGRES_PASSWORD`,
`RELAYHUB_SECRET_ENCRYPTION_KEY`, `RELAYHUB_NATS_USERNAME` and
`RELAYHUB_NATS_PASSWORD`. Public docs and `.env.example` expose the same
configuration surface.
