# Public developer documentation source

Status: documentation workspace only; no public site deployed yet.

This folder is the publication allowlist. Use Markdown for guides and references. Site tooling, agent exports and skill downloads will be generated here as implementation progresses.

Planned navigation: Get Started, Guides, API Reference, SDKs, Skills, Changelog.

Only the Go bootstrap server exists today. Queue, authentication and realtime integration are not available yet. Do not publish the internal target API as a working integration quickstart.

The Skills tab must provide copyable SKILL.md, raw downloads and complete versioned packages. No integration skill packages have been released yet.

Planning update: the proposed grant response includes a wireChannel for direct Centrifugo clients. This is not an available runtime API. No public integration guide or skill is released by this planning-only update.

Planned integration guide: a self-contained queue + realtime sample with two isolated projects. Business applications are external consumers that integrate after RelayHub is complete; no business engine or external app integration is part of provider MVP acceptance. This scope note applies to future human guides, agent exports and Skills resources; none are released yet.
