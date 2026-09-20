import {readFile, writeFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';

const require = createRequire(import.meta.url);
const {relayhubSidebar} = require('../sidebars.js');
const root = new URL('../', import.meta.url);
const ids = relayhubSidebar.flatMap(item => item.items ?? [item.id]);
const sections = [];
for (const id of ids) {
  let source;
  try {
    source = await readFile(new URL(`docs/${id}.md`, root), 'utf8');
  } catch (error) {
    if (error.code !== 'ENOENT') throw error;
    source = await readFile(new URL(`docs/${id}.mdx`, root), 'utf8');
  }
  const page = id === 'intro' ? '' : id;
  sections.push(`Source: https://relayhub.dungxbuif.com/${page}\n\n` +
    source.replace(/^---\n[\s\S]*?\n---\n/, '').trim());
}
await writeFile(new URL('static/public-guide.txt', root),
  '# RelayHub public guide\n\nGenerated from the published tutorials. Root-relative links use https://relayhub.dungxbuif.com.\n\n' +
  sections.join('\n\n---\n\n') + '\n');
for (const script of ['build-skill.sh', 'build-llms.sh']) {
  execFileSync('sh', [fileURLToPath(new URL(`../../backend/scripts/${script}`, root))], {stdio: 'inherit'});
}
