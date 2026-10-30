---
title: Go integration
description: Publish events and process durable work from a trusted service.
---

# Go integration

Use the Go SDK in a trusted service or worker. The SDK keeps request signing and delivery settlement consistent with the public contract.

```bash
go get github.com/dungxbuif/RelayHub/sdks/go
```

The normal flow is to create one client, publish with a stable idempotency key, and inspect the delivery timeline when the receiver is slow or offline. Workers should acknowledge only after their application has persisted the effect.

Callback receivers can use the SDK's signature helper with the raw request body. Do not parse and re-encode JSON before verification.
