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
