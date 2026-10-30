import {mkdir, readFile, writeFile, readdir, unlink, rmdir, lstat} from 'node:fs/promises';
import {resolve, join, relative, sep} from 'node:path';
import {fileURLToPath} from 'node:url';

const DEFAULT_ROOT = resolve(fileURLToPath(new URL('..', import.meta.url)));
const PUBLIC_ORIGIN = 'https://relayhub.dungxbuif.com';
const PUBLIC_FORBIDDEN_TERMS = /\b(?:postgres(?:ql)?|redis|nats|jetstream|centrifugo|partykit|yjs|docker|swarm|traefik|axum|sqlx|rust)\b/i;
const INTERNAL_PATH = /(?:^|[\s`"'(=])\/(?:deploy|operate|internal|private|core-code)(?:\/|[\s`"')]|$)|(?:rust\/src|backend\/|web\/remake-admin)/i;
const MARKDOWN_LINK = /\[[^\]]*\]\(([^\s)]+)(?:\s+"[^"]*")?\)/g;
const FRONTMATTER = /^---\r?\n[\s\S]*?\r?\n---\r?\n?/;

function json(value) {
  return JSON.stringify(value, null, 2) + '\n';
}

function normalizePath(value) {
  const path = value.split(/[?#]/, 1)[0];
  if (path === '/') return '/';
  return path.replace(/\/+$/, '') || '/';
}

function publicUrl(path) {
  const normalized = normalizePath(path);
  return normalized === '/' ? `${PUBLIC_ORIGIN}/` : `${PUBLIC_ORIGIN}${normalized}`;
}

function fallbackFromAttributes(attributes) {
  const match = attributes.match(/(?:textFallback|fallback)\s*=\s*(["'])(.*?)\1/i);
  return match?.[2] ?? null;
}

const GENERATED_ROOT_FILES = new Set([
  'static/README.md',
  'static/llms.txt',
  'static/llms-full.txt',
  'static/public-guide.txt',
  'static/aliases.json',
  'static/search-index.json',
  'static/sitemap.xml',
  'static/versions/index.json',
]);

function transformWidgetBlock(block, source) {
  let result = block;
  result = result.replace(/<([A-Z][\w.]*)\b([^>]*)>([\s\S]*?)<\/\1>/g, (whole, _name, attributes, body) => {
    const fallback = fallbackFromAttributes(attributes);
    if (fallback) return fallback;
    if (body.trim()) return body;
    throw new Error(`${source}: MDX widget needs a text fallback`);
  });
  result = result.replace(/<([A-Z][\w.]*)\b([^>]*)\s*\/\s*>/g, (whole, _name, attributes) => {
    const fallback = fallbackFromAttributes(attributes);
    if (!fallback) throw new Error(`${source}: MDX widget needs a text fallback`);
    return fallback;
  });
  if (/<\/?[A-Z][\w.]*(?:\s[^>]*)?>/.test(result)) {
    throw new Error(`${source}: MDX widget needs a text fallback`);
  }
  return result;
}

function publishedText(source, sourceName) {
  const withoutFrontmatter = source.replace(FRONTMATTER, '');
  const chunks = withoutFrontmatter.split(/(```[\s\S]*?```|~~~[\s\S]*?~~~)/g);
  const text = chunks.map((chunk, index) => index % 2 ? chunk : transformWidgetBlock(chunk, sourceName)).join('').trim() + '\n';
  if (PUBLIC_FORBIDDEN_TERMS.test(text)) throw new Error(`${sourceName}: backend technology leaked into public text`);
  if (INTERNAL_PATH.test(text)) throw new Error(`${sourceName}: internal path leaked into public text`);
  return text;
}

function validateLink(href, sourceName, allowedPages, aliases) {
  if (!href || href.startsWith('#') || href.startsWith('mailto:') || href.startsWith('tel:')) return;
  let parsed;
  try {
    parsed = new URL(href, PUBLIC_ORIGIN);
  } catch {
    throw new Error(`${sourceName}: invalid public link ${href}`);
  }
  if (parsed.origin !== PUBLIC_ORIGIN) return;
  const path = normalizePath(parsed.pathname);
  const allowedAsset = path === '/admin' || path.startsWith('/admin/') ||
    path === '/openapi.json' || path === '/asyncapi.yaml' || path.startsWith('/schemas/') ||
    path === '/llms.txt' || path === '/llms-full.txt' || path.startsWith('/skills/') ||
    path.startsWith('/downloads/');
  if (allowedAsset || allowedPages.has(path) || aliases.has(path)) return;
  throw new Error(`${sourceName}: public link is not in the versioned catalog: ${href}`);
}

function validateManifest(manifest) {
  if (!manifest || manifest.schema_version !== 1) throw new Error('unsupported content manifest schema');
  if (!manifest.primary_version || !Array.isArray(manifest.versions) || !manifest.versions.length) {
    throw new Error('content manifest has no primary version');
  }
  const versions = new Set();
  const pages = new Set();
  const allowedPages = new Set();
  const publicUrls = new Set();
  for (const version of manifest.versions) {
    if (!/^[a-z0-9][a-z0-9.-]*$/.test(version.id ?? '') || versions.has(version.id)) throw new Error(`unsafe or duplicate public version: ${version.id}`);
    versions.add(version.id);
    if (!Array.isArray(version.pages) || !version.pages.length) throw new Error(`version has no pages: ${version.id}`);
    for (const page of version.pages) {
      const key = `${version.id}:${page.id}`;
      if (!page.id || pages.has(key)) throw new Error(`duplicate public page: ${key}`);
      if (!page.source || page.source.startsWith('/') || page.source.split('/').includes('..') || INTERNAL_PATH.test(page.source)) {
        throw new Error(`internal or unsafe public source: ${page.source}`);
      }
      if (!page.public_url?.startsWith('/') || page.public_url.includes('..') || page.public_url.includes('\\')) throw new Error(`unsafe public URL: ${key}`);
      const publicPath = normalizePath(page.public_url);
      if (publicUrls.has(publicPath)) throw new Error(`duplicate public URL: ${publicPath}`);
      publicUrls.add(publicPath);
      pages.add(key);
      allowedPages.add(publicPath);
    }
  }
  if (!versions.has(manifest.primary_version)) throw new Error('primary version is not declared');
  const aliases = new Map((manifest.aliases ?? []).map(alias => [normalizePath(alias.from), normalizePath(alias.to)]));
  for (const [from, to] of aliases) {
    if (!from.startsWith('/') || !to.startsWith('/') || !allowedPages.has(to)) throw new Error(`invalid public alias: ${from} -> ${to}`);
  }
  return {allowedPages, aliases};
}

async function readPage(root, page, allowedPages, aliases) {
  const sourcePath = resolve(root, 'docs', page.source);
  const docsRoot = resolve(root, 'docs');
  if (!sourcePath.startsWith(`${docsRoot}${sep}`) || sourcePath === docsRoot) throw new Error(`public source escapes docs: ${page.source}`);
  const source = await readFile(sourcePath, 'utf8');
  const text = publishedText(source, page.source);
  const prose = text.replace(/```[\s\S]*?```|~~~[\s\S]*?~~~/g, '');
  for (const match of prose.matchAll(MARKDOWN_LINK)) validateLink(match[1], page.source, allowedPages, aliases);
  return text;
}

function publicCapability(capability) {
  return {
    id: capability.id,
    name: capability.name,
    summary: capability.summary,
    status: capability.status,
    limitations: capability.limitations,
  };
}

function versionContract(version) {
  return {
    version: version.id,
    status: version.status,
    contracts: {
      http: version.id === 'v1' ? '/openapi.json' : null,
      messages: version.id === 'v1' ? '/asyncapi.yaml' : null,
    },
    note: 'Only public application contracts are listed here. Service implementation details are intentionally omitted.',
  };
}

async function expectedOutputs(root, manifest, capabilities) {
  const {allowedPages, aliases} = validateManifest(manifest);
  const capabilityById = new Map((capabilities.capabilities ?? []).map(capability => [capability.id, capability]));
  const versions = [];
  const output = new Map();
  for (const version of manifest.versions) {
    const pages = [];
    const seenSources = new Set();
    for (const page of version.pages) {
      if (seenSources.has(page.source)) continue;
      seenSources.add(page.source);
      for (const capabilityId of page.capability_ids ?? []) {
        if (!capabilityById.has(capabilityId)) throw new Error(`unknown capability ${capabilityId} on ${page.id}`);
      }
      if (!page.text_path?.endsWith('.md') || page.text_path.split('/').some(part => !part || part === '.' || part === '..' || !/^[a-zA-Z0-9_.-]+$/.test(part))) {
        throw new Error(`unsafe public output: ${version.id}/${page.text_path}`);
      }
      pages.push({...page, text: await readPage(root, page, allowedPages, aliases)});
    }
    const versionDir = `static/versions/${version.id}`;
    const versionIndex = [
      `# RelayHub ${version.id} reference`,
      '',
      `Status: ${version.status}`,
      '',
      ...pages.map(page => `- [${page.id}](${page.public_url}) — ${publicUrl(page.public_url)}`),
      '',
    ].join('\n');
    const versionFull = [
      `# RelayHub ${version.id} integration reference`,
      '',
      `Status: ${version.status}`,
      '',
      ...pages.map(page => `---\n\nSource: ${publicUrl(page.public_url)}\n\n${page.text.trim()}`),
      '',
    ].join('\n');
    output.set(`${versionDir}/llms.txt`, versionIndex);
    output.set(`${versionDir}/llms-full.txt`, versionFull);
    output.set(`${versionDir}/reference.json`, json({
      version: {id: version.id, status: version.status},
      pages: pages.map(page => ({id: page.id, public_url: page.public_url, text_path: page.text_path, capability_ids: page.capability_ids})),
    }));
    output.set(`${versionDir}/contracts.json`, json(versionContract(version)));
    for (const page of pages) output.set(`${versionDir}/pages/${page.text_path}`, page.text);
    versions.push({id: version.id, status: version.status, pages: pages.map(page => ({id: page.id, public_url: page.public_url}))});
  }
  const primary = versions.find(version => version.id === manifest.primary_version);
  const primaryFull = output.get(`static/versions/${manifest.primary_version}/llms-full.txt`);
  const capabilitiesText = (capabilities.capabilities ?? []).map(publicCapability).map(capability =>
    `- **${capability.name}** (${capability.status}) — ${capability.summary} Limitations: ${capability.limitations.join(' ')}`,
  ).join('\n');
  output.set('static/llms.txt', [
    '# RelayHub public reference',
    '',
    'RelayHub is a managed connection service for application events, delivery workflows and live updates.',
    'Use the product guides for outcomes and the versioned references for exact integration contracts.',
    '',
    '## Versions',
    '',
    ...versions.map(version => `- [${version.id} reference](/versions/${version.id}/llms.txt) — ${version.status}`),
    '',
    '## Capabilities',
    '',
    capabilitiesText,
    '',
  ].join('\n'));
  output.set('static/README.md', [
    '# RelayHub public reference',
    '',
    'RelayHub connects applications to event delivery, durable work and live updates through a managed product surface.',
    '',
    '- [Product guides](/)',
    '- [Versioned text reference](/versions/index.json)',
    '- [Machine-readable HTTP contract](/openapi.json)',
    '- [Machine-readable message contract](/asyncapi.yaml)',
    '- [Agent index](llms.txt)',
    '- [Complete agent reference](llms-full.txt)',
    '',
  ].join('\n'));
  output.set('static/llms-full.txt', primaryFull);
  output.set('static/public-guide.txt', primaryFull);
  output.set('static/aliases.json', json((manifest.aliases ?? []).map(alias => ({from: alias.from, to: alias.to}))));
  output.set('static/versions/index.json', json({primary_version: manifest.primary_version, versions}));
  const searchEntries = versions.flatMap(version => version.pages.map(page => ({
    version: version.id,
    id: page.id,
    url: publicUrl(page.public_url),
  })));
  output.set('static/search-index.json', json(searchEntries));
  output.set('static/sitemap.xml', [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ...searchEntries.map(entry => `  <url><loc>${entry.url}</loc></url>`),
    '</urlset>',
    '',
  ].join('\n'));
  return {output, version: primary.id};
}

async function collectFiles(directory) {
  const files = [];
  let entries;
  try { entries = await readdir(directory, {withFileTypes: true}); } catch { return files; }
  for (const entry of entries) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) files.push(...await collectFiles(path));
    else if (entry.isFile()) files.push(path);
  }
  return files;
}

async function pruneGenerated(root, outputs, check) {
  const candidates = [];
  for (const relativePath of GENERATED_ROOT_FILES) candidates.push(resolve(root, relativePath));
  candidates.push(...await collectFiles(resolve(root, 'static/versions')));
  const expected = new Set([...outputs.keys()].map(relativePath => resolve(root, relativePath)));
  for (const path of candidates) {
    if (expected.has(path)) continue;
    const relativePath = relative(root, path);
    if (check) throw new Error(`${relativePath} is stale; run build-reference.mjs`);
    await unlink(path);
  }
  if (!check) {
    const dirs = [];
    const walk = async directory => {
      let entries;
      try { entries = await readdir(directory, {withFileTypes: true}); } catch { return; }
      for (const entry of entries) if (entry.isDirectory()) { await walk(join(directory, entry.name)); dirs.push(join(directory, entry.name)); }
    };
    const versionsRoot = resolve(root, 'static/versions');
    await walk(versionsRoot);
    for (const directory of dirs.reverse()) {
      try { await rmdir(directory); } catch { /* non-empty */ }
    }
  }
}

async function assertNoSymlinkPath(root, relativePath) {
  let current = root;
  for (const part of relative(root, resolve(root, relativePath)).split(sep)) {
    current = join(current, part);
    try {
      if ((await lstat(current)).isSymbolicLink()) throw new Error(`public output traverses symlink: ${relativePath}`);
    } catch (error) {
      if (error?.code === 'ENOENT') break;
      throw error;
    }
  }
}

async function writeOrCheck(root, outputs, check) {
  for (const relativePath of outputs.keys()) {
    const path = resolve(root, relativePath);
    const staticRoot = resolve(root, 'static');
    if (!path.startsWith(`${staticRoot}${sep}`)) throw new Error(`unsafe public output: ${relativePath}`);
    await assertNoSymlinkPath(root, relativePath);
  }
  await pruneGenerated(root, outputs, check);
  for (const [relativePath, content] of outputs) {
    const path = resolve(root, relativePath);
    if (check) {
      let actual;
      try { actual = await readFile(path, 'utf8'); } catch { throw new Error(`${relativePath} is missing; run build-reference.mjs`); }
      if (actual !== content) throw new Error(`${relativePath} drift; run build-reference.mjs`);
      continue;
    }
    await mkdir(resolve(path, '..'), {recursive: true});
    await writeFile(path, content);
  }
}

export async function buildReference({root = DEFAULT_ROOT, manifest, capabilities, check = false} = {}) {
  root = resolve(root);
  manifest ??= JSON.parse(await readFile(join(root, 'content-manifest.json'), 'utf8'));
  capabilities ??= JSON.parse(await readFile(join(root, 'capabilities.json'), 'utf8'));
  const {output, version} = await expectedOutputs(root, manifest, capabilities);
  await writeOrCheck(root, output, check);
  return {outputs: [...output.keys()], version};
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const check = process.argv.includes('--check');
  const result = await buildReference({check});
  console.log(`${check ? 'PASS' : 'WROTE'}: public references (${result.version}, ${result.outputs.length} files)`);
}
