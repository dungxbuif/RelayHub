import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp, mkdir, readFile, writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';

const loadGenerator = () => import('./build-reference.mjs');

async function fixtureRoot() {
  const root = await mkdtemp(join(tmpdir(), 'relayhub-reference-'));
  await writeFile(join(root, 'content-manifest.json'), JSON.stringify({
    schema_version: 1,
    primary_version: 'v1',
    versions: [{
      id: 'v1',
      status: 'preview',
      source_commit: 'internal-should-not-export',
      pages: [
        {id: 'intro', source: 'intro.md', public_url: '/', text_path: 'intro.md', capability_ids: ['managed-integration']},
        {id: 'api/v1', source: 'api/v1.md', public_url: '/api/v1', text_path: 'api/v1.md', capability_ids: ['managed-integration']},
      ],
    }],
    internal_route: '/internal/admin/health',
  }, null, 2));
  await writeFile(join(root, 'capabilities.json'), JSON.stringify({
    capabilities: [{
      id: 'managed-integration',
      name: 'Managed integration',
      summary: 'Connect applications without maintaining the service.',
      status: 'preview',
      source_commit: 'internal-should-not-export',
      evidence_paths: ['rust/src/host.rs'],
      limitations: ['Availability follows the published service policy.'],
    }],
  }, null, 2));
  await mkdir(join(root, 'docs'), {recursive: true});
  await mkdir(join(root, 'docs', 'api'), {recursive: true});
  return root;
}

test('text_preserves_examples_and_version', async () => {
  const root = await fixtureRoot();
  await writeFile(join(root, 'docs', 'intro.md'), [
    '---',
    'title: Intro',
    '---',
    '# RelayHub',
    '',
    'Keep this warning: never expose the signing secret to browser code.',
    '',
    '```js',
    'const exact = "keep these bytes";',
    '```',
    '',
    'See the [API](/api/v1).',
  ].join('\n'));
  await writeFile(join(root, 'docs', 'api/v1.md'), [
    '# API v1',
    '',
    'Use `POST /api/v1/events` from a trusted service.',
  ].join('\n'));

  const {buildReference} = await loadGenerator();
  const result = await buildReference({root, check: false});
  assert.equal(result.version, 'v1');
  const full = await readFile(join(root, 'static', 'llms-full.txt'), 'utf8');
  assert.match(full, /const exact = "keep these bytes";/);
  assert.match(full, /never expose the signing secret/);
  assert.equal((full.match(/Source: /g) ?? []).length, 2);
  assert.doesNotMatch(full, /internal-should-not-export|rust\/src\/host\.rs|\/internal\/admin\/health/);
  assert.doesNotMatch(await readFile(join(root, 'static', 'search-index.json'), 'utf8'), /internal-should-not-export|rust\/src\/host\.rs|\/internal\/admin\/health/);
  assert.doesNotMatch(await readFile(join(root, 'static', 'sitemap.xml'), 'utf8'), /internal-should-not-export|rust\/src\/host\.rs|\/internal\/admin\/health/);
  assert.match(await readFile(join(root, 'static', 'versions', 'v1', 'llms-full.txt'), 'utf8'), /API v1/);
  assert.match(await readFile(join(root, 'static', 'versions', 'v1', 'reference.json'), 'utf8'), /"public_url": "\/api\/v1"/);
});

test('public_reference_rejects_unsafe_widgets_and_links', async () => {
  const root = await fixtureRoot();
  await writeFile(join(root, 'docs', 'intro.md'), '# RelayHub\n\n<InteractiveWidget />\n\n[bad](/api/v2/events)\n');
  await writeFile(join(root, 'docs', 'api/v1.md'), '# API v1\n');
  const {buildReference} = await loadGenerator();
  await assert.rejects(() => buildReference({root, check: false}), /text fallback|public link/);
});

test('mdx_widgets_require_text_fallback', async () => {
  const root = await fixtureRoot();
  await writeFile(join(root, 'docs', 'intro.md'), '# RelayHub\n\n<InteractiveWidget textFallback="Read the API guide." />\n');
  await writeFile(join(root, 'docs', 'api/v1.md'), '# API v1\n');
  const {buildReference} = await loadGenerator();
  const result = await buildReference({root, check: false});
  const full = await readFile(join(root, 'static', 'llms-full.txt'), 'utf8');
  assert.match(full, /Read the API guide\./);
  assert.equal(result.outputs.length > 0, true);
});
