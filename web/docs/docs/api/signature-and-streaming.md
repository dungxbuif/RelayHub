---
title: Authentication
description: Sign application calls, issue scoped tokens and verify callbacks.
---

# Authentication

Applications, connected clients and administrators use different credentials.

## Sign app requests

Send:

```http
X-RelayHub-Api-Key: <app API key>
X-RelayHub-Timestamp: <Unix seconds>
X-RelayHub-Signature: <hex HMAC-SHA256>
```

Hash the exact body bytes with SHA-256 and encode the digest as lowercase hex. Join these values with newline characters:

```text
timestamp
UPPERCASE_HTTP_METHOD
/path?exact=query
hex_sha256_of_body
```

Sign the string with the full app secret using HMAC-SHA256 and encode as hex. Include the query exactly as sent. For an empty body, hash the empty byte sequence.

See the [Node.js example](/user/get-started). Official SDKs sign normal app requests.

## Issue client tokens

Your backend grants only permissions its authenticated user needs. Clients receive short-lived tokens, never app secrets.

Use channel permissions for [realtime](/developer/realtime) and the stream scope for [durable consumption](/developer/streaming). Keep token-bearing URLs out of logs.

## Verify callbacks

Check signatures against raw bytes before parsing. Enforce a timestamp replay window and deduplicate events before business actions. SDK callback-verification helpers implement the signing contract.

## Admin sessions

Administrators log in with email and password. The session uses a secure cookie and requires a CSRF token for mutations. App keys and old admin tokens are not Control Panel login credentials.
