# Task 7 report

Status: DONE_WITH_CONCERNS (browser preview limitation; automated gates complete).

## Delivered

- Dependency-free `/docs/` console with User, Developer, API Reference and Skills
  navigation, skip/focus targets, responsive columns, readable code, visible copy
  feedback, clipboard fallback/manual selection, direct Markdown/JSON/ZIP links.
- Canonical public onboarding, application/signing/event/queue/callback/WebSocket/
  function guides, deployment/restore/config reference, security and troubleshooting.
  Removed stale tenant/API-key-only/issue-token/SDK examples. Internal architecture,
  developer and user records reconciled; implementation history stays internal.
- OpenAPI 3.1 for all 24 registered method/path operations (docs catch-all maps to
  `/docs/{resource}`), explicit auth, headers, schemas/statuses/error examples,
  standard WebSocket handshake and operational endpoints.
- Event/client/server JSON Schema 2020-12 with representative valid/invalid fixtures.
- Integration Skill, signing reference, exact copied OpenAPI and reproducible ZIP
  (sorted paths, ZIP_STORED, fixed 1980 timestamp and 100644 permissions).
- Stable absolute llms index and deterministic full Markdown manifest concatenation.
- Canonical checker validates every JSON, OpenAPI/reference/operation structure,
  schema fixtures, Markdown/HTML links/anchors, console copy behavior, real API and
  Redis signed flows, all downloads and MIME/bytes, generation parity and drift.
- Router-derived manifest adds no HTTP endpoint. Runtime API semantics remain
  unchanged; ZIP responses now have explicit MIME and attachment metadata.
- `go generate ./web` builds Skill and llms first. Docker checks contracts/parity
  before compile; Go, Python validators and Node copy tests are build tooling only.

## RED evidence

1. Initial root checker failed listing missing OpenAPI, all three schemas, Skill
   source/reference/ZIP, and CSS/JS assets.
2. Route contract test initially failed because `RouteManifest` did not exist.
3. ZIP behavior test failed with `missing attachment filename`; the production
   handler then added deterministic attachment metadata and the test passed.
4. Copy behavior refinement failed because clicked button lacked `Copied` feedback;
   production script now displays feedback locally and in its live region.
5. Disposable negative controls reject stale ZIP, copied OpenAPI, llms-full,
   web/embed.go, a broken HTML anchor, broken Markdown link, unresolved OpenAPI ref,
   a missing router operation and an event schema accepting invalid fixtures.

## Verification

- `PYTHON=/tmp/relayhub-docs-venv/bin/python sh scripts/check-contracts.sh --self-test`
  passed (validators jsonschema 4.26.0 / openapi-spec-validator 0.9.0).
- Default checker starts fresh Go API and isolated Redis on ephemeral loopback
  ports; signed create/list/read/update/token, event publish/replay/get, queue,
  ack, job controls, function register/list/invoke-unavailable/delete, rotation,
  disable and every unauthenticated route boundary passed. Real RFC 6455 upgrade
  challenge and ready-frame application identity also passed.
- Every public Markdown and required HTML/JSON/CSS/JS/text/ZIP artifact returned
  200 with matching source bytes and expected MIME; `/docs` returned 308; missing
  assets returned JSON 404; ZIP content, normalized metadata and repeat hashes matched.
- `PYTHON=/tmp/relayhub-docs-venv/bin/python npm --prefix public-docs run test:docs`
  passed through the canonical wrapper.
- `go test ./...`, `go test -race ./...`, `go vet ./...`, `go generate ./web`,
  and `git diff --check` passed.
- `docker build -t relayhub:task7-docs .` passed with contract/parity gates.
- Skill ZIP SHA-256:
  `f7444223b49b4f3df9ee6998f78ccf3bfb9190c7096b6baf992cc632f79f4733`.

## Concerns and scope

CUA's in-app browser was unavailable, Chrome could not reach the loopback preview,
and the browser URL policy rejected the local file URL. No policy workaround was
attempted. A visual browser screenshot/manual keyboard session is therefore not
claimed. Semantic navigation/focus targets/mobile structure and actual copy-script
clipboard, denied/insecure fallback, manual selection and fetch failure are tested;
real API delivery is tested separately.

An extra integration-tagged run with a single explicit Redis URL encountered
pre-existing fixture collisions (`owner` records reused across function tests).
The intended `go test -race -tags integration ./... -count=1 -timeout=180s` run passed with isolated Redis testcontainers (redisstore package: 25.305s). No unrelated
runtime or test harness changes were made.

Both human-facing public docs and AI-readable surfaces changed. Internal docs also
changed; no affected documentation class was deferred. No deployment was published
and no real credentials or homelab secrets were used.

## Fix round 1 — all four Important findings

Changes:

1. `/docs/{resource}` now declares JSON objects. The live download smoke parses
   every required JSON artifact and validates it against that operation's actual
   `application/json` response schema in addition to MIME and byte parity.
2. Create/update app schemas enforce trimmed nonempty names with a 128-character
   upper bound, HTTP(S) host/userinfo/fragment rules, and callback/all non-null URL
   constraints expressible without stored state. Public/internal API guides
   describe the authoritative 128 UTF-8-byte limit, Unicode trimming, PATCH merge
   behavior, and exact runtime HTTP host exception (including link-local IPs).
   Live fixtures also exercise invalid URLs, 129 ASCII bytes, 130 UTF-8 bytes,
   missing callbacks and atomic mode/null PATCH interactions.
3. Copy completion now re-enables the button before restoring focus in one final
   block; explicit manual-copy textarea selection keeps focus. The DOM boundary
   test makes disabled `.focus()` a no-op and checks secure/fallback success,
   denied clipboard, load failure and manual/non-manual fallback failures.
4. All 24 manifest routes carry explicit `public|admin|app|ws_token` auth metadata.
   Go tests compare exact OpenAPI security maps; the checker consumes the exported
   manifest, compares fixtures/operations to its category, and exercises correct
   credentials plus wrong-category 401 responses for every protected route.
   A disposable negative control swaps valid admin/app security declarations and
   confirms that parity rejects the swap.

RED observed before fixes:

- App schema accepted `{"name":"orders","delivery_mode":"callback"}`.
- Copy test failed `null !== 'button'` with disabled-element focus semantics.
- Actual downloaded OpenAPI JSON object failed its declared string response schema.
- Route parity test failed to build because the required auth category did not yet
  exist (`route.Auth undefined`).

GREEN verification:

- `PYTHON=/tmp/relayhub-docs-venv/bin/python sh scripts/check-contracts.sh --self-test`
  passed, including the new admin/app swap negative control.
- Full default docs/contracts checker passed with live JSON schema validation,
  app invalid/partial-PATCH fixtures, every wrong-category probe, standard socket
  token upgrade and all existing signed flows.
- npm docs wrapper passed; `go test ./...`, focused HTTP/web tests and race tests,
  `go vet ./...`, generated parity, and `git diff --check` passed.
- Docker build with static contracts/copy/auth/parity gates passed.
- `go generate ./web` rebuilt copied OpenAPI, deterministic Skill ZIP, llms-full
  and embedded assets. New ZIP SHA-256:
  `87bf6f01b1a95d7fab2242fa68b0a7f8ae571c404edd64946a7d3ddafc55f983`.

No runtime application semantics changed. The two ledgered Minor findings remain
outside this fix round. The previously recorded browser preview limitation remains;
all requested behavioral/contract checks for this round pass.

## Fix round 2 — runtime-compatible callback URL pattern

Added positive CreateApp and UpdateApp fixtures for `HTTPS://example.com/hook`
and `https://example.com:/hook` before changing the contract. All four cases ran
RED with the existing pattern's case-sensitive scheme and required port digit.
Changed only the callback URL pattern to portable `[Hh][Tt][Tt][Pp][Ss]?` scheme
matching and zero-or-more optional port digits. No inline regex flags, runtime
changes, or changes to the two Minor findings/browser limitation were introduced.

GREEN: the four new positive cases pass, and all existing negative URL fixtures
remain rejected. `go generate ./web` synchronized canonical/bundled OpenAPI, the
reproducible ZIP, and embed. The llms builder ran and required no content change.
Full `scripts/check-contracts.sh --self-test` passed with JSON/schema validation,
negative drift/auth-swap controls, real API/Redis smoke, and generated parity.
Focused web snapshot/embedded-doc and route-manifest tests and `git diff --check`
also passed. New Skill ZIP SHA-256:
`32cc7b9b3e1d4e660a11f640a413d424f9645d1f12e1b720b3620f3d6155b747`.
