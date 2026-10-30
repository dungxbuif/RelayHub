import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {buildReference} from './build-reference.mjs';

const root = new URL('../', import.meta.url);
await buildReference();
execFileSync('sh', [fileURLToPath(new URL('../../backend/scripts/build-skill.sh', root))], {stdio: 'inherit'});
