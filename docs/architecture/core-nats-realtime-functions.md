# Core NATS realtime and remote functions

RelayHub v1 keeps the public `/ws` frames and function HTTP endpoints stable
while replacing Redis Pub/Sub with private Core NATS subjects. PostgreSQL is the
authority for function catalog, caller idempotency, invocation ownership,
deadlines and terminal replies. Core NATS only transports best-effort observation
frames and live function dispatch signals.

## Observation bridge

Event and job notifications are published as an application-scoped envelope on
`rh.v1.realtime.<app_token>`. Every API gateway subscribes to the wildcard and
validates that the envelope application hashes to the received subject before
calling the local WebSocket hub. A lost notification does not affect the durable
event or queue state. Clients recover durable work through the queue APIs.

## Function request and reply

A WebSocket subscribed to `functions` makes its gateway eligible for that
application. Eligible gateways join one queue group on
`rh.v1.rpc.<app_token>.*`; gateways without a local handler do not join. The
invoking gateway publishes the canonical invocation frame to the exact function
subject and requests an acceptance reply on its instance-scoped
`rh.v1.rpc.reply.<instance_token>` subject.

The selected gateway must reserve the PostgreSQL invocation for one authenticated
application and connection, acknowledge that reservation, and enqueue the frame
before replying `accepted`. A disconnect or full local queue releases an
unacknowledged reservation. Once any gateway reports accepted, the invoker never
redispatches it. Ambiguous request replies may be retried only while PostgreSQL
still permits a pending reservation; the database fence prevents a second owner
from receiving an already acknowledged invocation.

The browser result is accepted only when application, connection, invocation,
claimed state and persisted deadline all match. PostgreSQL stores a successful or
handler-error result once. Concurrent and later calls with the same caller and
idempotency key replay the stored outcome for 24 hours without dispatch. Pending
or reserved work becomes `unavailable` at its claim deadline. Claimed work becomes
`timeout` at its function deadline. No responder maps to `503
function_unavailable`; expiry after an accepted handler maps to `504
function_timeout`. Late or mismatched results receive the existing generic
`invalid_rpc_result` response.

## Verification

Focused tests cover application and connection fences, fast replies, stored
terminal replay, no responders, deadline boundaries and the absence of
redispatch after acceptance. Integration tests run two NATS bridge instances and
PostgreSQL to prove one eligible owner receives a request while observation
frames reach each matching local hub with Redis unavailable.
