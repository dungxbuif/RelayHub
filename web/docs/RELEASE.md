# RelayHub product docs release checklist

This checklist is the release gate for the public product surface. It keeps the public contract and client distribution aligned while allowing unreleased capabilities to remain clearly marked as preview or planned.

## Before review

1. Confirm `content-manifest.json` and `capabilities.json` use the same primary API version. Keep `source_commit` and `evidence_paths` in the private catalog only; never copy them into public text, search output, sitemap, skill or kit metadata.
2. Run the catalog and deterministic reference checks:

   ```bash
   python3 ../../backend/scripts/check-docs.py --product-catalog
   npm run test:references
   npm run check:references
   ```

3. Build the site and confirm generated output is current:

   ```bash
   npm run build
   go -C ../../backend generate ./web
   git diff --exit-code -- ../../backend/web ../../web/docs/static
   ```

4. Run the browser journey. It must cover product positioning, a local webhook use case, quickstart, Node.js/Go integration, authentication, the Developer Kit, the legacy compatibility page and retired-route checks:

   ```bash
   npx playwright install chromium
   npm run test:product-docs
   ```

5. Verify the kit outside the repository and from a path containing spaces:

   ```bash
   python3 ../../backend/scripts/test-developer-kit.py --artifacts static/downloads
   ```

## Release decisions

- A capability is `released` only when its implementation evidence and public contract have been reviewed. Otherwise keep it `preview`, `planned` or `legacy`.
- Do not call a preview client distribution stable. The download manifest, versioned text, skill and examples must agree on the contract version and checksum.
- Do not publish server deployment files, private source paths, component names, topology, credentials, cache files or internal compatibility reports. Client dependency metadata and licenses are allowed when needed to build the SDK.
- Retired operator URLs must return no old content. Remove stale static files, search entries, sitemap entries and generated embeds before deploying the new navigation.

## Final evidence

Attach the catalog result, reference `--check`, strict Docusaurus build, browser smoke result, static contract checker, clean SDK builds and kit checksum manifest to the release review. This repository does not publish packages or deploy the site as part of the docs build.
