# PostgreSQL

RelayHub v1 requires a private PostgreSQL instance for application and function
configuration. PostgreSQL does not need a published LAN port when it shares the
RelayHub deployment network.

Set these values on the RelayHub service:

```dotenv
RELAYHUB_POSTGRES_URL=postgres://relayhub:replace-me@relayhub-postgres:5432/relayhub?sslmode=disable
RELAYHUB_SECRET_ENCRYPTION_KEY=replace-with-base64-encoded-32-random-bytes
```

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

