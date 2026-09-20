---
title: Remote functions
description: Call an action in a connected application and receive its result.
---

# Remote functions

Ask another connected application to perform a short action and return a result. Use functions for a calculation, status check or interaction with an online agent.

Your application runs the handler. RelayHub registers and routes calls; it does not execute uploaded code.

## Register and connect

The owner registers a function with a signed `POST /api/v1/functions` request:

```json
{"name":"calculate","timeout_seconds":5}
```

Save the returned function ID. Names are unique within an application and timeouts range from 1 to 30 seconds.

The owner obtains a socket token with `ws:connect`, connects to `/ws`, waits for ready, then subscribes:

```json
{"type":"subscribe","topics":["functions"]}
```

Keep this handler connection running.

## Answer an invocation

A selected owner connection receives `rpc.invoke` containing the invocation ID, function, input and deadline. Reply on that connection:

```json
{"type":"rpc.result","invocation_id":"received_invocation_id","ok":true,"result":{"value":42}}
```

Return deliberate, caller-safe errors when handling fails. Do not return stack traces or credentials.

## Call the function

A caller sends a signed `POST /api/v1/functions/{functionID}/invoke` with an idempotency key and a body such as:

```json
{"input":{"a":20,"b":22}}
```

Handle unavailable owners and deadline errors. Registration alone does not mean the handler is online.

For long OCR jobs or work that waits for a laptop to reconnect, submit a [queue job](/developer/queue). The [API contract](/openapi.json) and [handler reference](https://relayhub.dungxbuif.com/docs/developer/functions.md) describe exact errors and reply formats.
