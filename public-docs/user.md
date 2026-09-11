# User Guide

RelayHub accepts an event from one application and keeps a delivery job for each
target. Your application processes the event; RelayHub handles transport and state.

1. [Start and provision applications](user/getting-started.md).
2. Choose queue processing, WebSocket notifications, callbacks, or `all`.
3. [Deploy and monitor](deploy/README.md) with Redis persistence and a worker.
4. Diagnose failures using [troubleshooting](troubleshooting.md) and [FAQ](user/faq.md).

Every delivery target has durable queue work regardless of delivery mode.
Callbacks require a reachable URL and worker. WebSocket notifications are hints;
queue polling recovers missed events. For request/response between live services,
use [remote functions](developer/functions.md).
