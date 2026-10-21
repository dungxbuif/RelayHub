# User Guide

RelayHub turns an application event into an observable delivery workflow. You
decide which applications should receive it and whether they need durable
processing, a live notification, or both.

## Start here

1. Read [what RelayHub does](product.md) to choose the right capability.
2. Follow the [first event walkthrough](user/getting-started.md).
3. Choose callbacks or the durable stream for work that must survive a
   disconnect; choose WebSocket channels for live updates.
4. Use [remote functions](developer/functions.md) when a live service should
   return a bounded result.

Each target has delivery state, and temporary callback failures are retried. A
consumer should still deduplicate by event ID because reliable delivery does not
mean exactly-once application processing.

For self-hosting, see [deployment](deploy/README.md). For day-to-day diagnosis,
see [troubleshooting](troubleshooting.md) and the [FAQ](user/faq.md).
