# Durable streaming protocol

Use RelayHub's durable stream when application code must process events without
implementing an HTTP polling loop. The protocol is standard RFC 6455 WebSocket
plus JSON and works through the official TypeScript and Go SDKs. NATS remains a
private RelayHub implementation detail.

## Connect

1. From a trusted backend, sign `POST /api/v1/socket/token` for scope
   `stream:connect`.
2. Connect to
   `wss://relayhub.dungxbuif.com/api/v1/stream?token=<encoded-token>` and request
   subprotocol `relayhub.stream.v1`.
3. Verify the selected subprotocol and the server's `ready.protocol_version` are
   exactly v1.
4. Send `consumer.start` for consumer `default`.

```js
const socket = new WebSocket(
  `wss://relayhub.dungxbuif.com/api/v1/stream?token=${encodeURIComponent(token)}`,
  "relayhub.stream.v1",
);

socket.onmessage = async ({data}) => {
  const frame = JSON.parse(data);
  if (frame.type === "ready") {
    socket.send(JSON.stringify({
      type: "consumer.start", protocol_version: 1,
      consumer: "default", max_in_flight: 16,
    }));
  }
  if (frame.type === "event.delivery") {
    try {
      await processIdempotently(frame.event, frame.delivery_id);
      socket.send(JSON.stringify({type: "delivery.ack", delivery_id: frame.delivery_id}));
    } catch {
      socket.send(JSON.stringify({type: "delivery.nack", delivery_id: frame.delivery_id, delay_ms: 5000}));
    }
  }
};
```

Do not put the application API key or HMAC secret in browser code. Browsers
receive only a short-lived token from your authenticated backend.

During a staged deployment where the durable gateway is not configured, an
otherwise valid handshake receives HTTP `503 stream_unavailable`. Retry with
backoff after the operator enables PostgreSQL and the streaming gateway.

## Wire rules

Messages are JSON text frames of at most 65,536 encoded bytes, valid UTF-8, with
one object and no duplicate or unknown keys. One application owns one durable
consumer named `default`; replicas share its work. The authenticated connection,
not any client-supplied identifier, determines ownership.

Event types keep the HTTP publish rules and gain no streaming-only length limit.
Function input remains an object, while a successful function result may be any
valid JSON value, including `null`, strings, numbers, booleans and arrays.
Function names and handler error codes use
`^[A-Za-z_][A-Za-z0-9_.-]{0,63}$`.

Client frame types are `consumer.start`, `delivery.ack`, `delivery.nack`,
`delivery.progress`, `function.result`, and `ping`. Server frame types are
`ready`, `consumer.started`, `event.delivery`, `delivery.accepted`,
`function.invoke`, `error`, and `pong`.

See the exact [client JSON Schema](../schemas/stream-client-frame.schema.json),
[server JSON Schema](../schemas/stream-server-frame.schema.json), and
[AsyncAPI document](../asyncapi.yaml). Stable error and close-code tables are
included below so clients can make decisions without depending on message text.

Delivery is at least once. ACK only after business side effects commit, and
deduplicate using `event.id` or `delivery_id`. An unacknowledged delivery is
redelivered after disconnect or timeout. A NACK asks for bounded delayed
redelivery; progress extends work only up to a server-owned maximum.
RelayHub records the assignment durably before invoking your handler. Broker
duplicate suppression is time bounded, but only one physical copy of a delivery
can hold an active assignment. A copy arriving after durable ACK does not invoke
your handler. If no ACK was recorded before the assignment expires, RelayHub may
deliver the same delivery ID again; keep handler side effects idempotent.
An invocation result is accepted only from the application and connection to
which RelayHub assigned it. Missing, cross-application and stale-session IDs are
rejected as `function_not_assigned`.

RelayHub records the delivery assignment before sending it. ACK completes that
record before acknowledging the broker; NACK or disconnect releases it for
redelivery; progress renews the bounded processing lease. Duplicate physical
broker messages do not create another user-visible delivery identity.

## Errors and close codes

Stable frame error codes are `invalid_utf8`, `frame_too_large`, `invalid_json`,
`duplicate_key`, `invalid_frame`, `unknown_type`, `unsupported_version`,
`delivery_not_assigned`, `stale_delivery`, `consumer_already_started`,
`consumer_unavailable`, `backpressure`, `function_not_assigned`, and
`internal_error`. Branch on the code and `retryable` boolean; message text can
change.

Application close codes are 4400 protocol violation, 4401 authentication failed,
4403 forbidden, 4406 unsupported version, 4408 timeout, 4429 backpressure and
4503 messaging dependency unavailable. Standard 1000, 1001, 1003 and 1009 retain
their RFC meanings. Mint a fresh token after 4401; retry 1001 and 4503 with
jitter; fix the client before retrying 4400, 4403, 4406 or 1009.
