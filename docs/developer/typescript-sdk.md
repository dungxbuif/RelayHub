# TypeScript SDK implementation contract

The `@relayhub/sdk` package supports Node.js 20 or newer and modern browsers.
The Node entry point owns application HMAC credentials and signed HTTP calls.
The browser entry point exports only the durable stream client, public types and
structured errors; it requires a short-lived token provider and cannot import
the signer or application credentials.

`RelayHubClient` exposes app provisioning, routing rule management, event publication, realtime channel publish/subscribe, durable consumption, best-effort observation, function registration/invocation/handling and graceful drain.
Durable consumption obtains a fresh `stream:connect` token for every connection,
negotiates `relayhub.stream.v1`, starts only consumer `default`, bounds handler
concurrency, ACKs after successful completion and NACKs failures. Topic filters
are not part of the v1 SDK because the server reserves that field.

The stream reconnect loop uses bounded exponential backoff with jitter and calls
the token provider again before every attempt. Drain stops reconnect, waits for
active handlers, NACKs unfinished work when its deadline expires and closes the
socket. Durable events use `/api/v1/stream`; best-effort observers and online
function handlers use the existing `/ws` channel and its `rpc.result` frames.
These are separate connections because function claiming is not part of the
durable consumer. HTTP invocation remains idempotent through `Idempotency-Key`. App and routing management use `adminToken` and bearer authentication; they never use application HMAC credentials.

HTTP signing hashes the exact serialized body bytes and signs
`timestamp + "\\n" + METHOD + "\\n" + request-target + "\\n" + sha256(body)`.
Tests use the language-neutral golden vector in the authentication reference,
fake sockets/clocks for delivery and reconnect state, and a real RFC 6455
contract boundary where available. Builds emit ESM, CommonJS and declarations;
the packed artifact has a checked size budget and contains no source maps or
credentials.


SDK intentionally types JSON payload values as `unknown` to keep TypeScript compilation fast and stable. RelayHub validates that event `data`, realtime channel `data`, function input and callback bodies are JSON objects at the HTTP/API boundary.
