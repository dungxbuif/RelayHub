---
title: SDKs and integration skill
description: Download the SDKs, examples and instructions for application integration.
---

# SDKs and integration skill

The v1 Developer Kit bundles the integration skill, public contracts, examples and build instructions for the supported client languages. Download the [v1 Developer Kit](/downloads/relayhub-developer-kit-v1.zip), then use the [download manifest](/downloads/manifest.json) to verify its SHA256 before extracting it.

The skill helps an AI agent choose webhook, durable-work or live-update integration, keep credentials in the caller's secret store, use the matching SDK and verify one complete flow. It does not install or operate RelayHub's internal service.

See [Node.js integration](/integrate/node), [Go integration](/integrate/go), [browser integration](/integrate/browser) and [authentication](/security/auth).

The kit is a client-side distribution. It contains no server deployment files, credentials or operator procedures. Build the SDK you need from its included lockfile/module metadata, then run the matching example's verification command.
