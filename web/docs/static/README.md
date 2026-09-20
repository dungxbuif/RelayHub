# RelayHub Docs

RelayHub connects applications through durable events, Queue workers pull workers,
signed callbacks, standard WebSocket notifications, and remote function calls. Start with the
[documentation home](/) and [management console](/admin/). The legacy
`/docs/` path remains available for old bookmarks, but the public Docusaurus UI
is now served from the root domain.

- [User guide](user.md): setup, delivery choices and daily operation.
- [Developer guide](developer.md): signing, routing, Queue workers, realtime channels, callbacks, durable stream and RPC.
- [API reference](api.md): OpenAPI, schemas and stable endpoints.
- [Skills](skills.md): copy or download the integration Skill.
- [Deployment](deploy/README.md), [security](security.md), [troubleshooting](troubleshooting.md).
- [PostgreSQL](deploy/postgresql.md): private database, encryption key and upgrade
  requirements.
- [NATS and JetStream](deploy/nats.md): private broker, stream contract, readiness
  and persistence.

Markdown, Docusaurus UI assets and machine-readable artifacts live with the
standalone Docusaurus source under `web/docs/` and are embedded in the Go API
binary for deployment.
Agents can use [llms.txt](llms.txt), [OpenAPI](openapi.json) or
[the complete reference](llms-full.txt) without loading the Admin application.

The root repository README starts a verified four-service Docker stack and walks
through a first signed event. The [deployment guide](deploy/README.md) lists every
setting, secret-generation instructions, private health probes, operator-owned
routing and backup/restore guidance. The same content is included
in the complete agent reference.
