# RelayHub Docs

RelayHub connects applications through durable events, signed callbacks, standard
WebSocket notifications, and remote function calls. Start with the
[documentation index](/docs/) and [management console](/admin/).

- [User guide](user.md): setup, delivery choices and daily operation.
- [Developer guide](developer.md): signing, routing, realtime channels, callbacks, durable stream and RPC.
- [API reference](api.md): OpenAPI, schemas and stable endpoints.
- [Skills](skills.md): copy or download the integration Skill.
- [Deployment](deploy/README.md), [security](security.md), [troubleshooting](troubleshooting.md).
- [PostgreSQL](deploy/postgresql.md): private database, encryption key and upgrade
  requirements.
- [NATS and JetStream](deploy/nats.md): private broker, stream contract, readiness
  and persistence.

Markdown and machine-readable artifacts live with the standalone Docusaurus
application under `web/docs/`. The operator routes `/docs/*` to that deployment;
the Go API does not embed or serve this tree. Agents can use [llms.txt](llms.txt)
or [the complete reference](llms-full.txt) without loading the Admin application.

The root repository README starts a verified four-service Docker stack and walks
through a first signed event. The [deployment guide](deploy/README.md) lists every
setting, secret-generation instructions, private health probes, operator-owned
routing and backup/restore guidance. The same content is included
in the complete agent reference.
