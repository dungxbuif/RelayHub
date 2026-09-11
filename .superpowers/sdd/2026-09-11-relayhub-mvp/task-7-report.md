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
