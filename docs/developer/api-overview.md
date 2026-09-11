# Application and event API overview

The API exposes application lifecycle, socket tokens, durable events, managed queues, callback delivery controls and remote functions. All request and response bodies are JSON. Errors use `{"error":{"code":"...","message":"..."}}`. Request bodies are limited to 1 MiB.

## Application model

```json
{
  "id": "app_...",
  "name": "orders-web",
  "callback_url": "https://orders.internal/events",
  "delivery_mode": "queue",
  "enabled": true,
  "created_at": "2026-09-11T10:00:00Z",
  "updated_at": "2026-09-11T10:00:00Z"
}
```

`delivery_mode` is one of `queue`, `websocket`, `callback`, or `all`. A callback URL must be an absolute HTTP(S) URL with a nonempty hostname. HTTPS is required by default. The local HTTP exception includes loopback and private IP addresses and recognized local hostnames; see the application validation rules below. Link-local HTTP destinations are rejected.

## Routes

| Method and path | Authentication | Result |
| --- | --- | --- |
| `POST /api/v1/apps` | Admin bearer | Create an application; returns `app_id`, `api_key`, and `hmac_secret` once. |
| `GET /api/v1/apps` | Admin bearer | List applications without credentials. |
| `GET /api/v1/apps/{appID}` | Signed app request | Read the authenticated application. |
| `PATCH /api/v1/apps/{appID}` | Signed app request | Update `name`, `callback_url`, or `delivery_mode`. |
| `DELETE /api/v1/apps/{appID}` | Admin bearer | Disable the application; subsequent signed requests return `401`. |
| `POST /api/v1/apps/{appID}/rotate-secret` | Admin bearer | Atomically replace credentials and return the new `app_id`, `api_key`, and `hmac_secret` once. |
| `POST /api/v1/socket/token` | Signed app request | Issue an app-scoped token for explicit WebSocket scopes, for at most 900 seconds. |

Create body:

```json
{"name":"orders-web","callback_url":"https://orders.internal/events","delivery_mode":"queue"}
```

Create and rotate responses contain credentials:

```json
{"app_id":"app_...","api_key":"rhk_...","hmac_secret":"rhs_..."}
```

List, get, update, and disable responses never include `api_key` or `hmac_secret`. Rotation invalidates the previous API key and HMAC secret in the same store operation.

Patch fields are optional. Send `callback_url: null` to remove an existing callback URL. An empty patch or unknown JSON field returns `400 invalid_request`.

Socket-token request and response:

```json
{"scopes":["ws:connect","ws:subscribe"],"ttl_seconds":600}
```

```json
{"token":"<signed-token>","expires_at":"2026-09-11T10:10:00Z"}
```

The service uses `400 invalid_request` for malformed JSON or invalid fields, `401 unauthorized` for failed authentication, `403 forbidden` when a signed app targets another app ID, `404 app_not_found` for an absent application, and `409 conflict` for a uniqueness conflict.

## Publish an event

`POST /api/v1/events` requires a signed producer request and an `Idempotency-Key` header. The key belongs to the authenticated producer and is retained for 24 hours by default. Use a different key for each new event; reuse the same key when a response is lost.

```json
{"type":"order.created","target_app_ids":["app_target"],"data":{"order_id":"123"}}
```

`type` must be non-empty after trimming. `target_app_ids` must contain 1–100 unique existing, enabled applications; surrounding whitespace is removed and IDs are sorted. `data` must be a JSON object, including `{}`. Arrays, strings and null are invalid. Unknown top-level fields are rejected: `source_app_id` always comes from authentication. Arbitrary JSON numbers are preserved without floating-point conversion.

A new publication returns `202 Accepted` with this exact shape:

```json
{
  "event": {
    "id": "evt_...",
    "type": "order.created",
    "source_app_id": "app_source",
    "target_app_ids": ["app_target"],
    "data": {"order_id":"123"},
    "created_at": "2026-09-11T10:00:00Z"
  },
  "jobs": [{
    "id": "job_...",
    "event_id": "evt_...",
    "source_app_id": "app_source",
    "target_app_id": "app_target",
    "status": "pending",
    "attempts": 0,
    "created_at": "2026-09-11T10:00:00Z",
    "updated_at": "2026-09-11T10:00:00Z"
  }]
}
```

IDs are opaque. Timestamps are RFC3339 UTC and may include fractional seconds. One job is created per target. Event, jobs, idempotency record and internal stream notifications commit atomically. A replay returns the original publication and initial job snapshots with `202` and `Idempotent-Replayed: true`; use the job GET route to see current status. A reused key never updates the original event even if submitted data changes. Replays continue to work when an original target is subsequently disabled. After key expiry, the same key creates a new event.

## Consume, acknowledge, inspect and control jobs

| Method and path | Authentication | Success |
| --- | --- | --- |
| `GET /api/v1/queue?limit=20&wait=0` | Signed target | `200`, array of `{event,job}` leases. |
| `POST /api/v1/events/{eventID}/ack` | Signed target | `204`, no body; repeated acknowledgements succeed. |
| `GET /api/v1/events/{eventID}` | Signed source or target | `200`, event envelope. |
| `GET /api/v1/jobs/{jobID}` | Signed source or that job's target | `200`, job. |
| `POST /api/v1/jobs/{jobID}/requeue` | Admin bearer | `200`, pending job, if transition is permitted. |
| `POST /api/v1/jobs/{jobID}/dead-letter` | Admin bearer | `200`, dead-letter job, if transition is permitted. |

`limit` defaults to 20 and must be an integer from 1 to 100. `wait` defaults to 0 and must be an integer from 0 to 30 seconds. Waiting requests return when work is available or the timeout elapses; an empty result is `[]`. Each lease lasts 60 seconds. Leased jobs include `lease_until`, increment `attempts`, and set `status` to `leased`. A competing consumer of the same app cannot take an active lease. Process the event, commit your side effects, then acknowledge; see [the queue loop and transition rules](./reliability.md).

Admin routes use the existing `Authorization: Bearer <admin-token>` mechanism. They do not accept an application signature as admin authority. Queue mechanics remain internal; clients use this JSON API.

Event/job errors use the standard JSON envelope. Codes: `400 invalid_request` for malformed JSON, invalid fields, missing idempotency key or invalid queue bounds; `401 unauthorized` for missing/invalid signing credentials; `404 not_found` for missing records or unrelated applications; `409 conflict` for an illegal job transition; `413 request_too_large` over 1 MiB; `500 internal_error` for an unexpected failure. Cross-app reads and acknowledgements use the same 404 response as absent records. Service errors and responses never expose keys, secrets or signatures.

## Copyable signed publish and queue loop

Set `RELAYHUB_URL`, `PRODUCER_API_KEY`, `PRODUCER_HMAC_SECRET`, `TARGET_APP_ID`, `TARGET_API_KEY`, `TARGET_HMAC_SECRET`, and a stable `EVENT_KEY` in your environment. Save and run the following Python 3 code. The exact request target, including its query, and exact body bytes are signed. JSON is serialized once. GET/ack requests sign an empty body.

```python
import hashlib, hmac, json, os, time, urllib.request

base = os.environ["RELAYHUB_URL"].rstrip("/")

def signed(prefix, method, path, value=None, idempotency_key=None):
    body = b"" if value is None else json.dumps(value, separators=(",", ":")).encode()
    timestamp = str(int(time.time()))
    canonical = "\n".join((timestamp, method, path, hashlib.sha256(body).hexdigest()))
    signature = hmac.new(os.environ[prefix + "_HMAC_SECRET"].encode(),
                         canonical.encode(), hashlib.sha256).hexdigest()
    headers = {"X-RelayHub-Api-Key": os.environ[prefix + "_API_KEY"],
               "X-RelayHub-Timestamp": timestamp,
               "X-RelayHub-Signature": signature,
               "Content-Type": "application/json"}
    if idempotency_key is not None:
        headers["Idempotency-Key"] = idempotency_key
    req = urllib.request.Request(base + path, data=body if method != "GET" else None,
                                 headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=40) as response:
        raw = response.read()
        return json.loads(raw) if raw else None

published = signed("PRODUCER", "POST", "/api/v1/events", {
    "type": "order.created", "target_app_ids": [os.environ["TARGET_APP_ID"]],
    "data": {"order_id": "123"}
}, os.environ["EVENT_KEY"])
print("Accepted event:", published["event"]["id"])

for item in signed("TARGET", "GET", "/api/v1/queue?limit=20&wait=30"):
    event = item["event"]
    # Replace this with durable, idempotent processing keyed by event["id"].
    print("Received event:", event["id"])
    signed("TARGET", "POST", "/api/v1/events/" + event["id"] + "/ack")
```

Application signing allows five minutes of clock skew by default. Keep credentials out of browser code and logs. Use TLS when calling a deployed service.

## Remote functions

RelayHub routes application-owned handlers over standard WebSocket; it never runs user code. See [complete function schemas, limits and examples](./functions.md).

| Method and path | Signed actor | Success |
| --- | --- | --- |
| `POST /api/v1/functions` | Owner | `201`, registration (`name`, required `timeout_seconds` 1–30, optional `enabled`). |
| `GET /api/v1/functions` | Owner | `200`, own registrations sorted by name. |
| `DELETE /api/v1/functions/{functionID}` | Owner | `204`, removes registration/name reservation. |
| `POST /api/v1/functions/{functionID}/invoke` | Caller | `200`, `{invocation_id,ok,result}` or `{invocation_id,ok,error}`. |

Invocation requires `Idempotency-Key` and object `input`. Caller/key replay lasts 24 hours and returns the same status/body without redispatch, including `503 function_unavailable` and `504 function_timeout`. Handler `ok:false` is a completed `200` response. Only a subscribed connection belonging to the owner can claim/respond. HTTP bodies remain limited to 1 MiB; complete serialized RPC WebSocket messages must fit 64 KiB, and oversized invocation frames fail `400 invalid_request` before dispatch. Function names are owner-unique; names/timeouts, typed error objects and all endpoint outcomes are documented in the function reference.

## Application validation and partial updates

App names are trimmed using Go `strings.TrimSpace`; the resulting name must be
nonempty and no longer than **128 UTF-8 bytes**. The OpenAPI name pattern checks
trimmed nonempty text and a 128-character upper bound. Runtime byte validation is
stricter for non-ASCII text: 65 `é` characters are 130 bytes and fail. Whitespace
outside the trimmed name does not count toward the byte limit.

Callback URLs must be absolute HTTP(S) URLs with a host, no userinfo (such as
`user@host`) and no fragment. HTTPS is allowed by default, including private hosts;
URL validation alone is not an egress security boundary. HTTP is accepted only
when `RELAYHUB_ALLOW_INSECURE_CALLBACKS=true` and the host is `localhost`, ends in
`.localhost`, `.local` or `.internal`, or is an IP address for which Go's
`IsLoopback` or `IsPrivate` returns true. Link-local IP addresses are excluded. Host comparison is
case-insensitive and removes one trailing dot; this classification does not
resolve DNS. Public HTTP hosts are rejected even with the option enabled.

Application `updated_at` never moves backward: a stale PATCH, disable or rotation
preserves a later timestamp already persisted by another operation. PATCH also
preserves the disabled state when disable wins a concurrent interleaving.

Concurrent partial patches preserve each other's disjoint fields. RelayHub compares
the editable fields atomically and re-reads, merges and revalidates after a
conflict; an older name-only patch cannot restore a removed callback. After 16
conflicting attempts, it returns `409 conflict`; fetch the current app and retry.
Credential rotation and disable remain independent of editable-field updates.

`callback` and `all` require a non-null `callback_url`. `queue` and `websocket`
allow it to be absent or cleared with null. PATCH is merged with the persisted
app **before** validation: `{"delivery_mode":"callback"}` succeeds only if a
valid URL is already stored, and `{"callback_url":null}` fails while the resulting
mode is `callback` or `all`. To clear a callback and change delivery together, send
`{"delivery_mode":"queue","callback_url":null}` in one PATCH. The stateless
request schema permits omitted fields; only runtime can validate their interaction
with previously stored values.
