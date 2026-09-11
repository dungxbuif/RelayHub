# Application API overview

Task 2 exposes the application lifecycle and socket-token endpoints below. All request and response bodies are JSON. Errors use `{"error":{"code":"...","message":"..."}}`. Request bodies are limited to 1 MiB.

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

`delivery_mode` is one of `queue`, `websocket`, `callback`, or `all`. A callback URL must be an absolute HTTP(S) URL. HTTPS is required by default. When `RELAYHUB_ALLOW_INSECURE_CALLBACKS=true`, HTTP is allowed only for loopback, private-address, `.localhost`, `.local`, or `.internal` hosts.

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
