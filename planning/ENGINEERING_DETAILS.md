# Engineering details — implementation contract v0.2

Ngày 2026-09-11. Thiết kế chưa được triển khai. Bổ sung cho SYSTEM_DESIGN/API_CONTRACT; mọi implementation phải kiểm chứng các invariant dưới đây.

## 1. Layout và runtime

Paths source tương đối repo root, dùng `src/` cho Go/SDK/dashboard; `public-docs/` là site riêng. Giữ `src/internal/config` và `src/internal/httpapi` hiện có, không tạo package platform song song rồi copy config/router. Go module hiện tại tiếp tục dùng trong MVP.

Process modes: `relayhub serve`, `relayhub dispatch`, `relayhub migrate`, `relayhub admin-create`. `serve` giữ public API và admin listener riêng; không mount admin handlers vào public listener. Development admin bind loopback. Edge production chỉ proxy admin từ listener Tailnet, không dựa vào X-Forwarded-For không tin cậy. CLI admin-create đọc password từ terminal hoặc stdin chỉ định, không từ command argument.

Lifecycle: migrations chạy trước serve, version mismatch làm readiness fail; liveness chỉ phản ánh process. API ready nếu DB/schema khả dụng; NATS lỗi thể hiện degraded dispatch nhưng enqueue vẫn được nhận trong quota outbox. Claim thất bại dependency trả 503. Khi Centrifugo lỗi publish trả 503, không giả nhận delivery. Không biến NATS optional cho enqueue thành optional cho claim.

## 2. Schema và migrations

Tất cả IDs là UUID tạo bằng crypto/rand; timestamps timestamptz từ database. Named SQL migrations có checksum, advisory lock migration, forward-only khi production; rollback app chỉ cho schema tương thích. Không tự rollback bằng DROP khi release thất bại.

| Migration | Tables/columns trọng yếu | Constraints và indexes |
|---|---|---|
| 001_identity | projects(id,name,allowed_origins,created_at), api_keys(id,project_id,verifier_hash,scopes,queue_ids,revoked_at), admin_users, admin_sessions | index key ID; FK project; session expiry index |
| 002_resources | queues(id,project_id,name,policy,created_at), channel_policies(id,project_id,pattern), project_usage(project_id,nonterminal_jobs,running_attempts) | unique(project_id,name); usage counts >=0 |
| 003_ledger | jobs(id,project_id,queue_id,handler_version,status,data,policy_snapshot,progress_channel,generation,attempt_count,next_attempt_at,run_deadline,created_at,updated_at,terminal_at,result,result_hash,error,replay_of), idempotency_records(project_id,queue_id,key,request_hash,job_id,expires_at), outbox(id,project_id,job_id,generation,kind,payload,available_at,claim_owner,claim_until,sent_at,delivery_seq,last_dispatch_at) | idempotency unique(project_id,queue_id,key); unique outbox(job_id,generation,kind); jobs(project_id,created_at,id); pending-outbox partial index |
| 004_attempts | attempts(id,project_id,job_id,attempt_no,worker_key_id,worker_id,status,lease_hash,lease_expires_at,run_deadline,result_hash,error,started_at,finished_at) | unique(job_id,attempt_no); unique running attempt per job partial index; lease expiry partial index |
| 005_audit | audit_entries(id,project_id,actor_id,action,resource_id,created_at,metadata) | project/time index; metadata redact secrets |

Composite ownership FKs use unique(project_id,id) on queues/jobs; jobs(project_id,queue_id) references queues(project_id,id), attempts(project_id,job_id) references jobs(project_id,id). Every repository method receives project ID; no global ID-only resource fetch in project handlers.

Body data JSONB plus canonical bytes/hash kept for idempotency. Reject duplicate JSON keys and invalid UTF-8; validate numeric tokens before canonicalization; reject integer values outside ±(2^53−1) with 400 and document using strings for exact larger values. Use a pinned RFC 8785 implementation for supported I-JSON numbers, with tests for key order, numeric equivalence, boundary integers, null and Unicode. Never silently round an unsupported integer into a different accepted payload. Same idempotency key returning an existing job bypasses new-job quota; changed body is 409.

Lock order for job mutations: project_usage → job → attempt → outbox. Keep same order in claim, complete, expiry, replay and enqueue. DB row locking tests must include two concurrent transactions, not only sequential calls.

## 3. MQ topology và outbox

MVP stream `RH_WORK`, file-backed, explicit ACK durable consumers, work-queue retention, reject-new at configured storage capacity. Subject `rh.work.<projectUUID>.<queueUUID>.<versionHash>`: versionHash is lowercase SHA-256 of handlerVersion. Public values are validated; never interpolate arbitrary wildcard/topic characters.

Exactly one durable pull consumer per project/queue/version with exact subject filter; all matching claim requests share it. Creation is idempotent and bounded by configured resource limits. No overlapping filters and no new consumer per worker or browser. Consumer lifecycle stays while pending jobs exist; delete only after drain and deliberate cleanup. Events fan-out use a separate stream in later phase.

Dispatch algorithm:

```text
DB transaction: claim due outbox rows with FOR UPDATE SKIP LOCKED;
  write owner UUID + claim_until, commit.
Publish outside transaction with stable message ID outbox.id + delivery_seq; wait PubAck.
DB transaction: if owner still matches, set sent_at;
  update job accepted→queued only when generation/status still match.
If failure: release/reschedule claim with bounded retry; do not mark sent.
```

Never hold row locks across broker network calls. A stale publisher may publish twice; job ledger is dedup authority. JetStream duplicate window is an optimization, not correctness boundary.

Claim delivery checks project/queue/version/generation in ledger. Terminal or older-generation dispatch is ACKed; future/unknown dispatch is quarantined/logged, not executed. Active live lease is delayed without creating a second attempt. Expired lease is transitioned by locked reconciler, not overwritten by claim. Eligible accepted/queued job plus available concurrency slot creates running attempt and random lease; return to worker only after commit. Crash before HTTP response consumes an attempt and is recovered by expiry.

NATS ACK handles are process-local optimization. Complete arriving on another API instance commits ledger then best-effort ACK if handle exists; otherwise later redelivery sees terminal ledger and ACKs. Correctness cannot depend on sticky sessions.

## 4. Retry, reconciliation và complete

Scheduler ticks each second in MVP, claims due jobs in bounded batches using DB locks. Retry schedule increments generation exactly once and creates unique outbox row. Duplicate scheduler instances cannot duplicate generation transitions.

Lease expiry marks attempt failed, decrements running counter once; either retry_wait or terminal failed. Nonterminal count decremented only once at terminal transition. Complete records result hash and completion outbox transaction before broker ACK. A repeated same-attempt complete with same valid lease hash/result hash returns success even after lease time elapsed if attempt already succeeded; stale nonterminal lease returns LEASE_LOST.

Effective lease expiry is min(now+60s, attempt run_deadline, job max-age deadline). Heartbeat does not extend max run time or max job age. If side effects happened before expiry, handler/downstream idempotency still required. No job cancellation in MVP.

Broker record may age out; reconciler scans eligible ledger jobs without live dispatch/attempt, reissues bounded dispatch using new delivery ID for same generation. Reuse the unique outbox row: under lock increment delivery_seq, clear sent_at and make available; retries within that delivery keep the same sequence. Record last_dispatch_at and compare the delivery sequence as well as owner when marking sent, so a stale publisher cannot mark a newer delivery sent. Same-generation claim is still protected by job lock. Stop retry storms with minimum republish interval 60s and global batch limits. Expired job must be marked failed rather than silently forgotten.

## 5. Admin/query APIs required by dashboard

Add target routes under `/api/v1/admin`, served only by admin listener:

- POST /session: password login, secure cookie + CSRF token, login rate limit; GET /session current identity, DELETE /session logout.
- GET /projects, GET /projects/{id}, GET /projects/{id}/queues.
- GET /projects/{id}/keys: metadata only, never original key/hash.
- GET /projects/{id}/channel-policies.
- GET /projects/{id}/jobs?status=&cursor=&limit= and GET /projects/{id}/jobs/{jobId}.
- GET /projects/{id}/jobs/{jobId}/attempts; POST /projects/{id}/jobs/{jobId}/replay.

Reuse project service methods with explicit project principal after admin authentication; do not teach browser to retain backend keys. Cursor based on created_at + id, authenticated binding to project/filter; max page 100. Enum/status invalid returns 400. Add GET /jobs/{id}/attempts for project jobs:read scope if developer troubleshooting requires it in MVP.

POST login is exempt from session CSRF token but requires exact trusted Origin and JSON content type; mutations after login require CSRF token. Reject unknown origins rather than redirecting API to HTML login. Login/logout are audit events, password never logged.

## 6. Realtime compatibility and quotas

External logical channel maps to a safe wire channel; sessions return wire URL, grants return both logical `channel` and `wireChannel`. Raw Centrifugo SDK clients subscribe to wireChannel; RelayHub helper hides this mapping. Connection principal includes project and user; subscription token binds both principal and wireChannel. Return connection refresh and subscription refresh flows in guide, not only happy-path connect.

NATS is not the Centrifugo history engine. One-node memory engine remains MVP. Native Centrifugo JS SDK and browser raw WebSocket protocol fixture must both pass. Socket.IO remains unsupported. Protocol fixture is built against pinned official protocol spec, no invented JSON auth frames.

Implement project publish/enqueue token buckets in DB initially, locked per project and operation, to work across API instances. Project connection cap requires authoritative admission accounting and disconnect/expiry reconciliation on the chosen Centrifugo version. Compatibility spike must prove OSS hooks can do this; otherwise report the cap as operational trial limit, do not claim enforcement. This is a pre-release acceptance gate, not an assumed built-in feature.

## 7. Dependency strategy

Prefer pgx for PostgreSQL, official nats.go for JetStream, a maintained JWT library compatible with pinned Centrifugo, standard net/http, and one migration implementation. Pin versions and transitive locks in the dependency task; don't fetch latest implicitly. No runtime libraries are installed in this planning change.

Only sanitized capacity summaries go in this public Git repo; exact VPS paths/network inventories remain local excluded artifacts. Public website exports never include internal planning by recursive copy.

## 8. Official references

- [JetStream pull consumers](https://docs.nats.io/learn/jetstream/pull-consumers): bounded fetch and explicit acknowledgement primitives; RelayHub adds its own ledger/lease rules.
- [Centrifugo transports](https://centrifugal.dev/docs/transports/overview): transport and application protocol distinction; pin protocol fixtures with the selected release.
- [RFC 8785 JCS](https://www.rfc-editor.org/rfc/rfc8785): canonical JSON; RelayHub additionally restricts integer range to prevent silent loss.
