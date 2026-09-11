# Developer Guide

Start with [registration](developer/registration-flow.md) and
[exact signing](developer/auth.md). API examples use the deployed
`https://relayhub.dungxbuif.com` origin; replace the origin for local development.

- [HTTP API and signed publish/queue loop](developer/api-overview.md).
- [Callbacks, retries, leases and dead letter](developer/reliability.md).
- [Standard WebSocket and reconnect](developer/websocket.md).
- [Remote functions](developer/functions.md).
- [OpenAPI and schemas](api.md), [integration Skill](skills.md).
- [Deployment](deploy/README.md), [security](security.md), [troubleshooting](troubleshooting.md).

Application credentials belong on the backend. Browser clients receive only
short-lived WebSocket tokens through their authenticated backend.
