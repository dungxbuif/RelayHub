# RelayHub Docs

RelayHub connects applications through durable events, signed callbacks, standard
WebSocket notifications, and remote function calls. Start with the
[documentation index](index.html) and [management console](console.html).

- [User guide](user.md): setup, delivery choices and daily operation.
- [Developer guide](developer.md): signing, routing, realtime channels, callbacks, durable stream and RPC.
- [API reference](api.md): OpenAPI, schemas and stable endpoints.
- [Skills](skills.md): copy or download the integration Skill.
- [Deployment](deploy/README.md), [security](security.md), [troubleshooting](troubleshooting.md).
- [PostgreSQL](deploy/postgresql.md): private database, encryption key and upgrade
  requirements.
- [NATS and JetStream](deploy/nats.md): private broker, stream contract, readiness
  and persistence.

Markdown is the canonical content. All links are fetchable from the Go API below
`https://relayhub.dungxbuif.com/docs/`. Agents can use [llms.txt](llms.txt) or
[the complete reference](llms-full.txt). No JavaScript is required to read or
download the reference; the console adds optional copy buttons.

Optional: tài liệu trình bày theo kiểu Docusaurus (UX tốt cho đọc/lướt) nằm ở
`/docs-site`, dùng để hỗ trợ user/developer/operator, nhưng canonical vẫn là
`public-docs/` embedded trong API.

The root repository README starts a verified four-service Docker stack and walks
through a first signed event. The [deployment guide](deploy/README.md) lists every
setting, secret-generation instructions, private health probes, external
Traefik/Cloudflare routing and backup/restore guidance. The same content is included
in the complete agent reference.
