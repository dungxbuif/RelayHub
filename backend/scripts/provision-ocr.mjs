import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { createHash, createHmac } from 'node:crypto';

// Read the admin password from stdin, never from argv or a checked-in file.
const password = fs.readFileSync(0, 'utf8').trim();
const origin = process.env.RELAYHUB_ORIGIN || 'https://relayhub.dungxbuif.com';
const email = process.env.RELAYHUB_ADMIN_EMAIL;
const destination = process.env.RELAYHUB_CREDENTIAL_FILE || path.join(os.homedir(), '.config/relayhub/ocr.json');
if (!password || !email || new URL(origin).protocol !== 'https:') throw new Error('Admin email, stdin password and HTTPS origin required');
fs.mkdirSync(path.dirname(destination), { recursive: true, mode: 0o700 });
const state = fs.existsSync(destination) ? JSON.parse(fs.readFileSync(destination, 'utf8')) : { origin };
if (state.origin !== origin) throw new Error('Credential file belongs to another origin');
function save() { fs.writeFileSync(destination, JSON.stringify(state, null, 2), { mode: 0o600 }); fs.chmodSync(destination, 0o600); }
async function call(route, method, body, headers = {}) {
  const response = await fetch(origin + route, { method, headers: { 'Content-Type': 'application/json', ...headers }, body: body === undefined ? undefined : JSON.stringify(body), redirect: 'error', signal: AbortSignal.timeout(15000) });
  if (!response.ok) throw new Error(`${method} ${route}: HTTP ${response.status}`);
  return response;
}
const login = await call('/api/v1/admin/session', 'POST', { email, password });
const session = await login.json();
const cookie = login.headers.getSetCookie().find(value => value.startsWith('__Host-relayhub_admin='))?.split(';')[0];
if (!cookie || !session.csrf_token) throw new Error('Invalid admin session response');
const headers = { Cookie: cookie, 'X-RelayHub-CSRF': session.csrf_token };
try {
  const existing = await (await call('/api/v1/apps', 'GET', undefined, headers)).json();
  for (const name of ['ocr-proxy', 'ocr-worker-mac']) {
    if (state[name]) {
      if (!existing.some(app => app.id === state[name].app_id && app.name === name && app.enabled)) throw new Error(`Saved app ${name} does not match server`);
      continue;
    }
    if (existing.some(app => app.name === name)) throw new Error(`App ${name} already exists without saved credentials; refusing duplicate creation`);
    state[name] = await (await call('/api/v1/apps', 'POST', { name, delivery_mode: 'queue' }, headers)).json();
    save();
  }
  const worker = state['ocr-worker-mac'];
  async function signed(route, method, body) {
    const bytes = body === undefined ? '' : JSON.stringify(body);
    const timestamp = String(Math.floor(Date.now() / 1000));
    const signature = createHmac('sha256', worker.hmac_secret).update([timestamp, method, route, createHash('sha256').update(bytes).digest('hex')].join('\n')).digest('hex');
    return (await call(route, method, body, { 'X-RelayHub-Api-Key': worker.api_key, 'X-RelayHub-Timestamp': timestamp, 'X-RelayHub-Signature': signature })).json();
  }
  const subscriptions = await signed('/api/v2/subscriptions', 'GET');
  const found = subscriptions.items.find(item => item.name === 'ocr-jobs');
  state.subscription = found || await signed('/api/v2/subscriptions', 'POST', {
    name: 'ocr-jobs', event_types: ['ocr.document.requested', 'ocr.scan.requested'],
    default_visibility_seconds: 120, max_visibility_seconds: 300,
    max_total_lease_seconds: 3600, max_attempts: 20, max_batch_size: 1,
    retention_seconds: 604800, max_in_flight: 4, retry_delay_seconds: 30,
  });
  save();
  console.log(JSON.stringify({ apps: Object.fromEntries(['ocr-proxy', 'ocr-worker-mac'].map(name => [name, state[name].app_id])), subscription: state.subscription.id, credentialFile: destination }));
} finally {
  await call('/api/v1/admin/session', 'DELETE', undefined, headers);
}
