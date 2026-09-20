# RelayHub Apps, Routing Rules and Realtime Studio Plan

**Goal:** Replace the remaining Admin placeholders with safe application/routing management and an interactive, credential-redacted WebSocket studio.

**Architecture:** PostgreSQL remains authoritative for Apps and rules. New Admin-only control endpoints wrap existing services and audit every mutation; application HMAC/API credentials never enter the browser except the existing one-time create/rotate response. Studio token minting happens server-side under Admin authentication and returns only a short-lived scoped socket token. The browser uses standard WebSocket APIs and keeps redacted, bounded in-memory frame logs.

**Constraints:** No Python, no reverse-proxy implementation, no Socket.IO, no fake UI behavior. Cookie mutations require CSRF. One-time credentials require explicit acknowledgement and are never persisted in browser storage.

## Task 1: Admin control contracts

- Add safe Admin application detail/update/disable/rotate operations and explicit audit rows.
- Reuse validation and credential rotation invariants from the existing application service.
- Add an Admin-only short-lived studio token endpoint scoped to a selected enabled app.
- Add service/router/OpenAPI/auth/redaction tests.

## Task 2: Apps management UI

- Add responsive table/card list, create/edit/disable and rotate workflows.
- Present new credentials once with copy buttons, environment snippets and an acknowledgement gate.
- Link Go and TypeScript integration docs; test keyboard, error and secret-lifetime behavior.

## Task 3: Routing Rules management UI

- Add list/filter/create/edit, enable/disable and delete workflows.
- Validate source app, event type and target app inline; use deterministic cache invalidation and confirmations.
- Test empty/loading/error and conflict rollback behavior.

## Task 4: Realtime Studio v1 and durable stream

- Select an app and protocol, request a short-lived scoped token, connect/close and reconnect with bounded exponential backoff plus jitter.
- Support subscribe/unsubscribe and authorized channel publish for current v1; support durable stream frames without inventing v2 behavior before the v2 phase.
- Maintain bounded timestamped inbound/outbound logs with pause, clear, filter and redacted export.
- Never render/export socket query tokens, Admin credentials, API keys or HMAC secrets.

## Task 5: Verification and docs

- Add Vitest and Playwright coverage, OpenAPI/Skill/docs updates and regenerate the embedded Admin.
- Run frontend build/audit, Go unit/race/integration, contract checks and relevant multi-replica/browser gates.
