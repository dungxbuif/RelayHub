# Public developer documentation source

Status: documentation workspace is active and runtime baseline is being published for local consumption.

This folder is the publication allowlist. Markdown docs are preferred. Site tooling, agent exports and skill downloads will be generated here as implementation progresses.

## What is currently available

- In-memory provider baseline API running on `src/`:
  - Job enqueue/read: `POST /api/v1/jobs`, `GET /api/v1/jobs/{id}`
  - Worker APIs: `POST /api/v1/workers/claim`, `/api/v1/attempts/{id}/heartbeat|progress|complete|fail`
  - Realtime API stubs: `/api/v1/realtime/sessions`, `/api/v1/realtime/grants`, `/api/v1/realtime/publish`
- Source map and command set in `src/README.md`
- OpenAPI contract should match the implemented in-memory runtime (to be regenerated from source after finalizing endpoints).

## Planned navigation: Get Started, Guides, API Reference, SDKs, Skills, Changelog.

## Scope note

Only internal provider behavior is in progress for this run. External business apps remain out of scope for this provider pass and will integrate later.

The Skills tab, copy/download packages, and versioned resources are not yet released; this section will be added once SDK, sample app, and queue/realtime guide are finalized.
