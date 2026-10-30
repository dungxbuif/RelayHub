# Authentication reference

Use the credential type intended for the calling environment. Keep long-lived application credentials on a trusted server and issue short-lived scoped tokens to browsers or agents.

## Signed HTTP requests

The caller sends the application key, a current Unix timestamp and a signature for the exact request. The signed message includes the timestamp, HTTP method, path including the query string and the SHA-256 digest of the exact body bytes, joined with newlines.

Send the signature in the header described by the deployed [OpenAPI contract](https://relayhub.dungxbuif.com/openapi.json). Reject requests outside the allowed clock window, invalid signatures, changed body bytes and replayed idempotency keys.

## Short-lived tokens

Request a token from a trusted service. Bind it to the smallest required scope, channel or stream and expiry. Do not put application keys, signing secrets or agent credentials in browser bundles, URLs or logs.

## Receiver verification

Verify the signature before parsing or acting on a callback. Store the event ID before applying a side effect, then return success only after the receiver has safely accepted the event. Treat delivery as at least once and make side effects idempotent.
