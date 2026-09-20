# Public documentation rewrite

The public site introduces RelayHub to prospective users and integration developers. Replace the existing human-facing copy with a product overview, practical delivery choices, a first integration, focused feature guides, and SDK/agent downloads. Internal deployment topology stays in operator documentation.

Ground examples in the HTTP route manifest and SDK implementations. Keep actual protocol identifiers and endpoint paths intact. Explain realtime history as bounded recovery, not durable work delivery. Keep admin session authentication separate from app HMAC credentials.

Publish actual TypeScript and Go SDK source archives separately from the integration Skill. Generate an agent-readable copy of the new human pages during the docs build so both audiences receive the same introduction and examples. Preserve detailed machine contracts and existing URLs.

Verification: Docusaurus strict link build, reproducible resource checks, embedded docs and public route tests, container build, VM100 deployment health, public pages and archive contents.

Implementation: 14 public pages now follow introduction, first event, delivery selection, integration guides, administration and reference. Removed stale bootstrap-token login instructions from public pages and corrected related agent references. SDK source ZIPs are served through existing `/docs/downloads/` handling; the Skill ZIP remains a separate resource. Existing API paths and websocket protocol identifiers remain literal contracts.

`npm --prefix web/docs run build` generates `public-guide.txt`, SDK/Skill archives and the full agent reference before Docusaurus. Python 3 is required for deterministic archive generation and installed in the docs Docker stage. Source ZIPs omit dependency installs, build output, hidden files and symlinks. Only the English locale is advertised because translated pages are not maintained.

Deployed with the detailed logging changes as `homelab/relayhub:prod-20260920-152029` on VM100. Verified 18 public paths including new feature guides, agent resources, both SDK archives and readiness. Downloaded SDK bytes match the independently checked local archives. TypeScript built and packed in a clean directory; extracted Go SDK tests passed with its bundled signing fixture. Browser inspection confirmed the new homepage and navigation render correctly.
