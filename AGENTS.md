# RelayHub documentation requirements

User requirement recorded 2026-09-11. Maintain docs alongside every implementation change.

- Maintain detailed local/internal docs and separate public developer integration docs.
- Internal sources: root SPEC/SYSTEM_DESIGN/OPERATIONS and planning/; never publish them wholesale.
- Public source: public-docs/; prefer Markdown. Docusaurus is the preferred site candidate; MDX only for presentation needing components.
- Public docs describe verified runtime behavior. Label planned APIs/examples explicitly; do not present pseudocode as an installed SDK.
- Agents must be able to fetch plain Markdown, versioned OpenAPI, llms.txt, llms-full.txt and a machine-readable resource index without browser JavaScript or login.
- Public navigation must include a Skills tab. Each released skill has readable SKILL.md, copy action, raw download and a complete versioned archive containing referenced resources, plus compatibility and setup instructions.
- Generate display/copy/download artifacts from one canonical skill source. Never embed credentials or internal homelab details in exports.
- Feature completion includes internal docs, public integration guide/reference, agent exports, and relevant skills/examples updates or an explicit not-applicable rationale.
- Follow DOCUMENTATION_STRATEGY.md and include docs validation in release gates.
