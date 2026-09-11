# PostgreSQL

RelayHub v1 requires a private PostgreSQL instance for application and function
configuration, function invocation fencing, idempotent terminal replay and
delivery state. PostgreSQL does not need a published LAN port when it shares the
RelayHub deployment network.

Set these values on the RelayHub service:

```dotenv
RELAYHUB_POSTGRES_URL=postgres://relayhub:replace-me@relayhub-postgres:5432/relayhub?sslmode=disable
RELAYHUB_SECRET_ENCRYPTION_KEY=replace-with-base64-encoded-32-random-bytes
```

During the staged v1 cutover, configure both variables on the API process to
enable `/api/v1/stream`. Omitting both keeps the Redis prototype API running
and makes an authenticated stream handshake return `503 stream_unavailable`;
supplying only one variable is rejected at startup.
The final v1 Compose cutover makes PostgreSQL mandatory.

Generate the encryption key once and keep it in the same secret manager used for
the PostgreSQL password:

```bash
openssl rand -base64 32
```

RelayHub applies forward-only migrations at startup. Back up PostgreSQL before
upgrading and restore the database together with the encryption key. Losing the
key makes stored HMAC credentials unrecoverable. RelayHub logs a redacted
database address and never prints the password, application API keys or decrypted
HMAC secrets.

Use a migration owner for upgrades and a separate runtime role in production.
Grant the runtime role only the table operations RelayHub documents; the audit
table requires `INSERT` and read access and rejects update, delete and truncate.
RelayHub refuses to start when the database migration ledger is newer than the
running binary.

Function invocation rows retain the caller-key hash, input, selected connection,
deadline and terminal result for 24 hours. PostgreSQL is authoritative; Core NATS
only carries live request/reply traffic. Size retention and backup capacity for
the invocation workload, and keep database clocks synchronized across replicas.

Durable delivery rows retain the winning assignment identity after ACK so a
broker acknowledgement can be retried only by the same connection fence.
Progress never extends an assignment beyond its stored 15-minute maximum.
