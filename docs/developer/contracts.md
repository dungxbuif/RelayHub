# Public integration contract

Task 7 publishes the shipped HTTP and WebSocket contract, a dependency-free HTML
console, canonical Markdown, JSON Schema 2020-12 and OpenAPI 3.1, plus an installable
integration Skill. Stable production resources live below
`https://relayhub.dungxbuif.com/docs/`.

Before implementation: the affected internal references are developer API, auth,
registration, reliability, functions, Skills, deployment and architecture docs;
public equivalents, user onboarding, security, troubleshooting and discovery will
be reconciled. No API semantics are intentionally changed. The only runtime
addition is deterministic binary artifact serving and a router-derived manifest
for contract parity; the manifest is not an additional HTTP endpoint.

Verification starts with missing-contract/checker failures. It covers parsed JSON,
OpenAPI references/operations/security, schema positive and negative fixtures,
route parity, real API/Redis requests, asset MIME types, Markdown and HTML links,
copy controls, deterministic Skill/llms generation and embedded snapshot parity.
Final gates include existing Go tests/race/vet, npm wrapper and Docker build.

Markdown stays canonical because the Go binary embeds its assets and the stable
`.md` URLs are an API for agents. A later Docusaurus or other generator can read
the same Markdown and replace only the human console, retaining all stable
resource URLs, downloadable artifacts and parity checks.

## Maintenance commands

Use Python 3.10+ with a virtual environment:

```sh
python3 -m venv .venv
.venv/bin/pip install jsonschema==4.26.0 openapi-spec-validator==0.9.0
export PYTHON="$PWD/.venv/bin/python"
go generate ./web
sh scripts/check-contracts.sh
npm --prefix public-docs run test:docs
```

The default checker starts an isolated real Redis server and freshly compiled API
on ephemeral loopback ports, uses synthetic credentials only in memory, exercises
signed application/event/queue/function flows, fetches every public Markdown and
required artifact, compares exact bytes/MIME and verifies the ZIP. `--static`
retains parsed OpenAPI/schema fixtures, link/anchor checks, generated drift,
reproducibility and router manifest parity for Docker build without running Redis.
Python validator dependencies and Node (for console JavaScript tests) are build/test tooling only. Redis 7+ must be on PATH
for the default runtime check. All subprocesses and temporary data are cleaned up.

`RouteManifest` walks the actual chi registrations, including optional features in
the fully configured router. Tests require a one-to-one OpenAPI operation mapping
and reject accidentally unauthenticated API registrations. No discovery HTTP route
was added. `/docs/*` maps to OpenAPI `/docs/{resource}` with catch-all semantics.
ZIP responses use explicit application/zip and attachment filename metadata.

The Skill builder copies canonical OpenAPI and emits only the three documented
files in sorted order, ZIP_STORED, timestamp 1980-01-01, Unix file mode 100644.
Consecutive builds are byte-identical. The llms builder uses an explicit ordered
Markdown manifest and rejects omissions/duplicates. Both support `--check` without
repairing drift. `go generate ./web` rebuilds both before embedding. Docker checks
all committed output before compile, so accidental stale artifacts cannot ship.

Canonical public human and AI surfaces changed together: onboarding, Skills,
security, troubleshooting, deployment, OpenAPI, schemas and llms indexes. Historical
implementation records remain internal; public prose describes shipped behavior.

`sh scripts/check-contracts.sh --self-test` also runs negative controls in a disposable copy: zip/reference/llms/embed drift, broken Markdown/HTML anchors, unresolved OpenAPI refs, missing router coverage and invalid schema acceptance. The checker executes the actual console JavaScript against controlled clipboard/selection boundaries, covering secure copy, denied/insecure fallback, full Skill copy, manual selection and fetch failure.

## Task 7 review fix round 1 plan

Correct the JSON download response contract; encode application name/URL/mode
constraints and clarify partial PATCH validation against persisted state; restore
copy focus only after re-enabling the button; and bind each manifest route to an
explicit auth category checked against OpenAPI and real requests. Tests first
exercise actual JSON downloads, invalid app fixtures, disabled-element focus
semantics and auth-category swaps. Runtime handlers remain the authority for byte
limits and local HTTP policy; no application API semantics change.

Fix round 1 reconciliation: the manifest now records an explicit auth category
independent of OpenAPI; both exact security parity and live wrong-category probes
are checked. JSON downloads are schema-validated as objects. App request schemas
capture stateless constraints while the API guide explains byte limits, private
HTTP policy and PATCH validation after merging persisted values. Copy focus tests
model disabled elements and preserve intentional manual textarea focus.

Fix round 2 plan: add positive create/update schema fixtures for uppercase HTTPS
and an explicitly empty URL port, both accepted by the existing Go URL parser.
Adjust only the portable scheme/port pattern; retain all negative URL fixtures
and runtime policy. Rebuild the copied contract, Skill ZIP and embedded snapshot,
then run contracts, negative drift controls and docs parity.
