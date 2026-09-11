# PostgreSQL control store

RelayHub v1 stores control-plane state in PostgreSQL. NATS/JetStream carries
messages, while PostgreSQL remains authoritative for applications, hashed API
key lookup, encrypted HMAC material, callback endpoints, function definitions,
administrator sessions and append-only audit records.

## Intended behavior

- `RELAYHUB_POSTGRES_URL` supplies the private PostgreSQL connection string.
- The pool uses bounded connection and lifetime settings; diagnostics redact
  credentials and never log a full connection string.
- Forward-only embedded migrations run under a PostgreSQL advisory lock and are
  recorded with a checksum. A changed migration is rejected.
- Application creation, compare-and-swap updates, disable and credential
  rotation are transactional. API keys are stored only as hashes. Rotation
  revokes the active credential and inserts a monotonically versioned record so
  callback delivery can revalidate the signing credential before dispatch.
- HMAC material is encrypted with AES-256-GCM using
  `RELAYHUB_SECRET_ENCRYPTION_KEY`, a base64-encoded 32-byte master key. Stored
  ciphertext carries an explicit format version so a later key migration can be
  implemented safely.
- Audit rows contain actor, action, resource identity, outcome and a small
  allowlisted metadata shape. Database triggers reject update, delete and
  truncate operations; runtime credentials should receive only the documented
  table privileges. Audit rows do not contain secrets or request bodies.
- Startup rejects a migration ledger newer than the binary, preventing an older
  RelayHub build from silently using an incompatible schema.

## Schema ownership

The store owns `schema_migrations`, `applications`,
`application_credentials`, `callback_endpoints`, `functions`,
`admin_sessions` and `audit_log`. Event, delivery and outbox tables are added by
the event-acceptance task.

## Verification

Unit tests cover secret encryption, configuration redaction and migration
integrity. Integration tests run against PostgreSQL and cover an empty database,
repeat migration, application CRUD, stale compare-and-swap, concurrent creation,
atomic credential rotation, function ownership and append-only audit writes.
