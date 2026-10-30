# Public product documentation refresh

## Intent

Make RelayHub's public documentation explain the product from a user's and
integrator's perspective before introducing deployment internals. Readers should
be able to discover what RelayHub does, choose a delivery mode, and reach the
right next guide without first learning about PostgreSQL, NATS, workers, or
internal service topology.

## Decision

- Add a product-first overview to `public-docs/product.md`.
- Reframe the public landing page, user guide, Docusaurus introduction, and
  agent index around capabilities and workflows.
- Keep deployment, storage, broker, security, and operational details in their
  existing dedicated references.
- Include the new overview in the canonical `llms-full.txt` manifest so agents
  receive the same product framing as human readers.

## Affected surfaces

- Human public docs: `public-docs/README.md`, `public-docs/user.md`,
  `public-docs/index.html`, `docs-site/docs/intro.md`.
- Agent-readable public docs: `public-docs/llms.txt`, `public-docs/llms-full.txt`.
- Generated runtime snapshot: `web/embed.go`.

## Verification

Run the public-docs static checks, regenerate `llms-full.txt`, run
`go generate ./web`, and run the embed/docs tests. Confirm no product overview
links expose internal architecture details and all links resolve.
