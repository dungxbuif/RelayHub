# RelayHub Go SDK

Official context-aware Go client for RelayHub HTTP APIs, Queue v2 workers, administration and Realtime v2.

```go
client, err := relayhub.New(relayhub.Config{
    BaseURL: "https://relayhub.example",
    APIKey: apiKey,
    HMACSecret: hmacSecret,
})
if err != nil { return err }

socket, ready, err := client.DialRealtime(ctx, relayhub.RealtimeTokenRequest{
    ClientID: "worker_42",
    Channels: map[string][]string{"support.room_42": {"subscribe", "publish"}},
})
if err != nil { return err }
defer socket.Close()
_ = ready
```

`RealtimeConn` serializes writes and exposes subscribe, rewind subscribe, bounded history, 50-item batch publish, targeted publish, presence updates and typed reads. Token grants may use one terminal namespace segment such as `project:42:*`; global and multi-segment wildcards are rejected. History is ephemeral reconnect continuity; durable stream, Queue v2 or callback processing remains the recovery path.

`PublishEncrypted` and `DecryptRealtimeFrame` use an explicit `RealtimeEncryptionKeyProvider`. AES-256-GCM keys stay in the integrating application; RelayHub only forwards the opaque envelope. Encryption is restricted to `private:*` channels, and applications own key distribution and rotation.

Message actions are available through `PutMessageAction`, `ListMessageActions`, and `RemoveMessageAction`. Actor identity comes from the Realtime token; puts require stable idempotency keys.

Queue v2 includes typed subscription management, pull/settle/extend, metrics,
DLQ operations and a worker with heartbeat and graceful drain:

```go
worker, err := client.WorkQueue(ctx, "sub_orders", func(ctx context.Context, delivery relayhub.QueueDelivery) relayhub.QueueResult {
    if err := processIdempotently(ctx, delivery.Event.ID, delivery.Event.Data); err != nil {
        return relayhub.QueueRetry(5*time.Second, "dependency_busy")
    }
    return relayhub.QueueACK()
}, relayhub.QueueWorkerOptions{Concurrency: 16, Visibility: time.Minute, Heartbeat: 20*time.Second})
if err != nil { return err }
defer worker.Drain(context.Background())
```

Queue v2 is at-least-once; persist business effects idempotently before ACK.
