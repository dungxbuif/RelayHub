# MVP verification matrix

All rows are NOT RUN for provider implementation. Bootstrap tests previously passed do not satisfy these rows.

| ID | Scenario | Expected invariant | Owner |
|---|---|---|---|
| A01 | Two projects, same logical IDs/channel names | No cross read/publish/claim | F2/R1 |
| A02 | Spoof X-Forwarded-For on public listener | Admin never served | F3/D1 |
| A03 | Revoke key, logout, token expiry | New API fails; issued JWT expires within declared TTL | F2/F3/R1 |
| Q01 | Same enqueue key 100 times concurrently | One job; same body ID stable; changed body409 | J1 |
| Q02 | DB commit succeeds, HTTP response lost | Retry key resolves same job | J1 |
| Q03 | NATS offline/full | Accepted ledger intact, outbox not marked sent | J2 |
| Q04 | Publish accepted, publisher dies before sent update | Duplicate dispatch creates no parallel attempt | J2/J3 |
| Q05 | Two worker claims and two API instances | One active attempt; no sticky-session requirement | J3 |
| Q06 | Worker effect succeeds then dies before complete | Redelivery allowed; app idempotency prevents duplicate effect | J4/P2 |
| Q07 | Lease expires while old worker still running | Old complete409; new attempt protected | J4 |
| Q08 | Complete committed but ACK/HTTP lost | Repeat complete200; terminal redelivery ACK only | J4 |
| Q09 | Two reconcilers expire same attempt | Counters change once, one retry generation | J4 |
| Q10 | Retry limit/max-age reached | Failed visible; no silently discarded ledger | J4 |
| Q11 | Replay repeated with same key | One new job linked to original failed job | J4 |
| Q12 | Capacity counter hit by concurrent enqueue/claim | Limits hold, no negative usage or oversubscription | J1/J3 |
| R01 | Official JS SDK and raw WebSocket protocol | Both receive authorized events; no Socket.IO claim | R2 |
| R02 | Reconnect after history lost | Client resync snapshot, stale event ignored | R2 |
| R03 | Attempt progress tries unrelated channel | Denied without leaking other job | R2 |
| R04 | Client slow/reconnect storm/token refresh | Bounded buffers, no unauthorized subscription | R1/R2 |
| S01 | Worker SDK complete timeout | Retry complete, not handler | R3 |
| S02 | Worker shutdown/lease loss | Stop new claims, abort signal and clear timers | R3 |
| D01 | Download skill/raw docs via HTTP | Complete files, correct type, no JS/auth needed | P3 |
| D02 | Public export scan | No private inventory or secrets | P3 |
| O01 | House internet outage | VPS accepts bounded backlog and workers resume | D2 |
| O02 | Full restore on isolated host | Jobs/config preserved; terminal jobs not rerun | D2 |
| O03 | Capacity trial 30min | Record p95/error/memory/disk; no unsupported SLA claim | D2 |
| O04 | Same-domain routing regression | Docs public, dashboard private, API errors JSON, WSS works | D1/D3 |

Evidence per row: candidate commit, environment, fixture data, command, timestamp, observed result and artifact link. A skipped dependency test remains NOT RUN, never PASS. Tests must use schema isolation and clean up their own resources only.
