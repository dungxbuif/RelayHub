# RelayHub Go SDK

Official context-aware Go client for RelayHub HTTP APIs, administration and Realtime v2.

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

`RealtimeConn` serializes writes and exposes subscribe, unsubscribe, targeted publish, presence updates, and typed reads. Realtime is online-only; durable stream/callback processing remains the recovery path.
