# Documentation strategy — 2026-09-11

## Two maintained audiences

| Surface | Content | Publication |
|---|---|---|
| Internal/local | Architecture, ADRs, schemas, failure modes, runbooks, deployment and planning | Local repository only |
| Public developer docs | Quickstart, auth, API/SDK, queue/socket flows, limits, errors, compatibility, troubleshooting | Public site at /docs/ |

Existing root design documents remain canonical internal sources; public-docs/ holds separately authored public Markdown. Sharing validated API schemas is allowed; recursively exporting the homelab repository is not.

## Site direction

Prefer Docusaurus for the future public site, baseUrl /docs/ on relayhub.dungxbuif.com. Markdown .md is canonical; MDX used sparingly for copy buttons, tabs and download cards. Pin/test the selected Docusaurus release and Markdown parser mode before implementation. Docusaurus does not automatically guarantee raw Markdown or llms exports: add an explicit generator.

Navbar: Get Started · Guides · API Reference · SDKs · Skills · Changelog. Docs static route and assets must precede the protected dashboard catch-all. Public documentation does not require Tailnet or admin login.

Sources: https://docusaurus.io/docs/markdown-features and https://docusaurus.io/docs/static-assets . Site build/deployment is planned, not implemented in this change.

## Agent-readable output contract

Planned routes, not yet deployed:

- /docs/llms.txt: compact index, canonical links, current API version and availability.
- /docs/llms-full.txt: generated public text only; no internal operations documents.
- /docs/raw/<slug>.md: readable Markdown per article, including all tabbed examples as text.
- /docs/openapi/<version>.json: schema for that actual release; distinguish runtime from future design.
- /docs/resources.json: type, slug, URL, version, status, updatedAt and checksum where appropriate.

All outputs fetchable by ordinary HTTP GET without JavaScript. Correct content types; missing assets return 404 instead of SPA HTML. Agent files are discoverability aids, not a claim every agent automatically supports an llms convention. Raw text must preserve headings, code languages, warnings and all steps hidden in interactive UI.

## Skills tab contract

Public /docs/skills/ lists all released integration skills and their resources. Initial intended categories: realtime integration, queue/worker integration, troubleshooting; webhook/functions added when available. Do not ship skills claiming features not implemented.

Each detail page includes purpose, prerequisites, supported API/client versions, required scopes, configuration variables, examples, limitations, changelog and a file manifest.

Actions:

1. Copy complete SKILL.md; indicate that copy alone is sufficient only for self-contained skills.
2. Download raw SKILL.md.
3. Download versioned ZIP containing SKILL.md plus every referenced script/template/reference. Preserve relative paths.
4. Copy/fetch a stable version-specific URL for automated tools.

Canonical future skill packages live under public-docs/skill-packages/<name>/. Generate rendered content, raw files, archive and checksums from those files; do not manually maintain three copies. No credentials, user-private host paths or internal infrastructure inventories in packages. Placeholders must be explicit configuration inputs rather than fabricated working tokens. Installation docs distinguish clients and never promise universal one-click installation.

## Maintenance workflow

Before code: note internal/public/API/skill impact in task. After code: reconcile actual behavior, update guides and schema, run example/contract checks, regenerate exports and skill assets. Update changelog when integration behavior changes. Breaking changes require a migration guide and version compatibility note.

Internal docs may contain future designs. Public default pages cover released behavior; planned content must be visibly marked and excluded from quickstarts that claim to run. Bootstrap remains the only implemented runtime at present.

## Release acceptance

- Docs site builds and internal links/assets resolve under /docs/.
- Plain HTTP fetch obtains raw Markdown/OpenAPI/index/skills without authentication or browser execution.
- Copy output equals canonical SKILL.md; ZIP manifest/checksum matches source and includes every relative reference.
- Integration examples run against the declared release (or are explicitly labeled pseudocode).
- Export allowlist contains only public-docs plus reviewed public schemas; secret/internal-path scan passes.
- Public /docs/ routes work while dashboard/admin remain protected.
- Required docs and relevant skills change in the same implementation task; this is not deferred until the last phase.
