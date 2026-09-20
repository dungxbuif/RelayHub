# Go SDK implementation decision

Task 7 implements the approved v1 client under `sdks/go`, imported as
`github.com/dungxbuif/RelayHub/sdks/go` until the planned standalone module split.
Production SDK files use standard Go packages and Gorilla WebSocket, never
private server or broker packages. Public payloads retain `json.RawMessage` so
large JSON integers and UTF-8 text survive transport unchanged.

HTTP publish, registration and invocation use exact HMAC over the transmitted
method, escaped request target and bytes. Requests honor context, cap request and
response bodies, reject redirects and report structured, credential-free errors.
Idempotency keys are explicit; an SDK retry never silently creates a new key.
The shared neutral HMAC fixture is owned by the TypeScript SDK task and consumed
by Go contract tests.

Durable consumers use `/api/v1/stream`, subprotocol `relayhub.stream.v1`, version
1 and consumer `default`, omitting `topics`. Function handlers use the currently
supported `/ws` channel with `rpc.invoke`/`rpc.result`; this is an SDK-managed
separate connection, not a promise of one physical connection for all features.
Multiple function names on one client share their function dispatch connection.

Each socket has one reader, one writer, bounded frame/work queues and a fixed
worker pool. Reconnect uses bounded jittered delay, fresh scoped tokens and
context cancellation. Fatal protocol/authorization errors stop reconnecting.
Handler success ACKs only while its original session and context remain live;
errors NACK with a bounded delay, including typed retry errors. Reconnect never
sends an old-session acknowledgement on a new socket. Business code receives
event/delivery IDs and attempt for idempotent processing.

Drain stops starting new handlers, waits for already accepted work and delivery
acknowledgements, then closes. Context expiration cancels handlers and closes
sockets so work can redeliver. Go cannot forcibly terminate arbitrary user code:
handlers must honor their context; fixed pools prevent unbounded replacement
goroutines when they do not. Function handler contexts use persisted deadlines.

Verification uses shared HMAC/stream fixtures, httptest HTTP and real WebSocket
contracts, the actual durable gateway where practical, bounded concurrency,
redelivery, cancellation/reconnect/drain tests and race/lifecycle checks. The
public Go quickstart/reference and examples ship with the implementation.
