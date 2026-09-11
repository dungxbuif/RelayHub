# Security and credential handling

## Keep credentials on trusted systems

Set separate strong admin and server signing secrets using the deployment's secret
mechanism. Store one-time app credentials securely. Never commit secrets or put
app HMAC keys in browser code. Admin routes grant application lifecycle and job
control authority; app keys do not. Protect Redis and backups because the server
must recover app signing material to verify requests and sign callbacks.

Use HTTPS externally and secure Redis transport where needed. HMAC signs exact
body/method/request-target with a timestamp; it does not encrypt data and does not
by itself prevent replay within the clock-skew window. Use idempotency keys for
publication/invocation and event-ID deduplication for consumer effects.

## Rotation and disable

Admin rotation atomically replaces API key and HMAC secret. Coordinate receiver
secret changes for callbacks. Disable makes later signed app authentication fail.
Already-issued socket tokens and established connections are not automatically
revoked by app rotation/disable; expiry is checked only at handshake. For urgent
session revocation, operators must close connections/restart API instances and
manage server token signing-secret rotation across instances.

## Exposure and permissions

Only the API should be externally reachable through TLS. Keep Redis, worker
metrics and administrative credentials private. The operations endpoints have no
built-in auth; restrict their network exposure at your proxy/firewall. There is
no per-function ACL: any authenticated app knowing an enabled function ID can
invoke it. Function list/delete remain owner-scoped. Use a separate deployment
when stronger isolation is required.

Callbacks are outbound requests. HTTPS URL validation is not a complete SSRF
boundary: private HTTPS destinations may be valid. Permit app configuration only
for trusted clients and enforce worker egress restrictions appropriate to your
network. HTTP callbacks are opt-in for controlled local development. Redirects
are not followed by the callback worker.

## Browser and logs

Allow exact browser Origins in `RELAYHUB_ALLOWED_ORIGINS`; `*` is invalid.
Absent/empty Origin is accepted for native clients and does not replace token
authentication. Backend endpoints giving browsers socket tokens must authenticate
the user and select the correct app. Do not log token query strings, Authorization,
API keys, signatures, HMAC secrets or event payloads. Reverse-proxy `/ws` access
logs must omit the query. Use bounded metrics, status codes and opaque IDs for
troubleshooting. Follow [deployment](deploy/README.md) for persistence and recovery.

## Production stack and observable data

Root Compose requires independently generated admin, signing and Redis passwords.
The committed example leaves all three empty. API/worker run non-root in a
read-only distroless image with trusted CA roots; Redis runs non-root with a private
AOF volume. Every container drops capabilities and enables no-new-privileges.
Only API publishes a host port. Keep Docker access and `.env` private: container
inspection can reveal environment credentials. Protect and encrypt Redis backups.
See [deployment](deploy/README.md) for every setting and persistence tradeoff.

API structured logs contain generated request IDs, method, route templates, status,
latency and bounded outcomes. Caller-provided request IDs, raw queries/paths,
credentials/signatures, headers, bodies, callback URLs and function input/result
are excluded. Callback/function operation logs record bounded outcomes. Metrics
use bounded labels. Proxy/WAF/access logs are separate: omit `/ws` query tokens,
authentication headers and body capture there too. Restrict unauthenticated metrics
and readiness routes with proxy/firewall rules.

Cloudflare Cache Rules must bypass `/api/*`, `/ws` and operations routes, including
function invoke/replay. Preserve signed request targets and WebSocket Upgrade
headers through external Traefik. Use TLS to the API and HTTPS callbacks; do not
turn on local insecure callback exceptions in production. URL validation does not
replace worker egress controls.

Successful authenticated request logs also include the persisted app ID. Application
operation logs record generated event/job IDs for publish/lease/admin transitions,
the validated event ID for acknowledgement, a persisted function ID at registration,
and the persisted invocation ID for a completed call or replay. Worker logs record
persisted target app/event/job IDs, callback attempt and outcome only after the
transition commits. Function names, callback URLs and handler error details remain
excluded. A function replay may change its URL, so its unchecked path is never
logged as the original function ID. Capture tests and acceptance assert these
specific IDs and outcomes while checking every sensitive sentinel remains absent.
