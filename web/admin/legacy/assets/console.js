(() => {
  const storeKey = 'relayhub-console-settings';
  const $ = (id) => document.getElementById(id);
  const enc = new TextEncoder();
  let socket;

  function loadSettings() {
    try { return JSON.parse(sessionStorage.getItem(storeKey) || localStorage.getItem(storeKey) || '{}'); }
    catch { return {}; }
  }
  function saveSettings(settings) { sessionStorage.setItem(storeKey, JSON.stringify(settings)); }
  function baseUrl() {
    const value = loadSettings().baseUrl || location.origin;
    return value.replace(/\/$/, '');
  }
  function output(id, value) { $(id).textContent = typeof value === 'string' ? value : JSON.stringify(value, null, 2); }
  function parseJSON(text) { try { return JSON.parse(text || '{}'); } catch { throw new Error('Data JSON is invalid.'); } }
  function requireAdmin() {
    const token = loadSettings().adminToken;
    if (!token) throw new Error('Admin token is required.');
    return token;
  }
  function requireApp() {
    const settings = loadSettings();
    if (!settings.apiKey || !settings.hmacSecret) throw new Error('App API key and HMAC secret are required.');
    return settings;
  }
  async function hmac(secret, text) {
    const key = await crypto.subtle.importKey('raw', enc.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
    const bytes = new Uint8Array(await crypto.subtle.sign('HMAC', key, enc.encode(text)));
    return [...bytes].map((byte) => byte.toString(16).padStart(2, '0')).join('');
  }
  async function sha256Hex(bytes) {
    const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', bytes));
    return [...digest].map((byte) => byte.toString(16).padStart(2, '0')).join('');
  }
  async function signedFetch(method, path, body, extraHeaders = {}) {
    const app = requireApp();
    const text = body === undefined ? '' : JSON.stringify(body);
    const bytes = enc.encode(text);
    const url = new URL(path, baseUrl());
    const timestamp = Math.floor(Date.now() / 1000).toString();
    const canonical = `${timestamp}\n${method}\n${url.pathname + url.search}\n${await sha256Hex(bytes)}`;
    const headers = {
      'X-RelayHub-Api-Key': app.apiKey,
      'X-RelayHub-Timestamp': timestamp,
      'X-RelayHub-Signature': await hmac(app.hmacSecret, canonical),
      ...extraHeaders,
    };
    if (text) headers['Content-Type'] = 'application/json';
    return apiFetch(url, { method, headers, body: text || undefined });
  }
  async function adminFetch(method, path, body) {
    const headers = { Authorization: `Bearer ${requireAdmin()}` };
    if (body !== undefined) headers['Content-Type'] = 'application/json';
    return apiFetch(new URL(path, baseUrl()), { method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
  }
  async function apiFetch(url, init) {
    const response = await fetch(url, init);
    const text = await response.text();
    const payload = text ? JSON.parse(text) : null;
    if (!response.ok) throw new Error(payload?.error?.message || `HTTP ${response.status}`);
    return payload;
  }
  function formBody(form) { return Object.fromEntries(new FormData(form).entries()); }
  function bind(formId, outputId, handler) {
    $(formId).addEventListener('submit', async (event) => {
      event.preventDefault();
      try { output(outputId, await handler(event.currentTarget)); }
      catch (error) { output(outputId, error instanceof Error ? error.message : String(error)); }
    });
  }

  const settings = loadSettings();
  const settingsForm = $('settings-form');
  settingsForm.baseUrl.value = settings.baseUrl || location.origin;
  settingsForm.adminToken.value = settings.adminToken || '';
  settingsForm.apiKey.value = settings.apiKey || '';
  settingsForm.hmacSecret.value = settings.hmacSecret || '';
  settingsForm.addEventListener('submit', (event) => {
    event.preventDefault();
    const values = formBody(settingsForm);
    saveSettings(values);
    $('copy-status').textContent = 'Settings saved for this browser session.';
  });
  $('clear-settings').addEventListener('click', () => { sessionStorage.removeItem(storeKey); localStorage.removeItem(storeKey); settingsForm.reset(); settingsForm.baseUrl.value = location.origin; });

  bind('create-app-form', 'app-output', async (form) => {
    const values = formBody(form);
    const body = { name: values.name, delivery_mode: values.delivery_mode };
    if (values.callback_url) body.callback_url = values.callback_url;
    const credentials = await adminFetch('POST', '/api/v1/apps', body);
    settingsForm.apiKey.value = credentials.api_key || settingsForm.apiKey.value;
    settingsForm.hmacSecret.value = credentials.hmac_secret || settingsForm.hmacSecret.value;
    saveSettings(formBody(settingsForm));
    return { credentials, note: 'api_key and hmac_secret were copied into the app credential fields for local testing.' };
  });
  $('refresh-apps').addEventListener('click', async () => { try { output('app-output', await adminFetch('GET', '/api/v1/apps')); } catch (error) { output('app-output', error.message); } });

  bind('create-rule-form', 'rule-output', async (form) => {
    const values = formBody(form);
    const body = { event_type: values.event_type, target_app_id: values.target_app_id, enabled: form.enabled.checked };
    if (values.source_app_id) body.source_app_id = values.source_app_id;
    if (values.realtime_channel) body.realtime_channel = values.realtime_channel;
    return adminFetch('POST', '/api/v1/routing/rules', body);
  });
  $('refresh-rules').addEventListener('click', async () => { try { output('rule-output', await adminFetch('GET', '/api/v1/routing/rules')); } catch (error) { output('rule-output', error.message); } });

  bind('publish-event-form', 'event-output', async (form) => {
    const values = formBody(form);
    const body = { type: values.type, data: parseJSON(values.data) };
    const targets = values.target_app_ids.split(',').map((v) => v.trim()).filter(Boolean);
    if (targets.length) body.target_app_ids = targets;
    return signedFetch('POST', '/api/v1/events', body, { 'Idempotency-Key': values.idempotencyKey });
  });

  bind('publish-channel-form', 'realtime-output', async (form) => {
    const values = formBody(form);
    await signedFetch('POST', `/api/v1/realtime/channels/${encodeURIComponent(values.channel)}/publish`, { data: parseJSON(values.data) });
    return { accepted: true };
  });
  bind('subscribe-form', 'realtime-output', async (form) => {
    const channel = formBody(form).channel;
    const tokenResponse = await signedFetch('POST', '/api/v1/socket/token', { scopes: ['ws:connect', 'ws:subscribe', 'ws:read'], ttl_seconds: 600 });
    const url = new URL('/ws', baseUrl());
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
    url.searchParams.set('token', tokenResponse.token);
    if (socket) socket.close(1000, 'replace');
    socket = new WebSocket(url);
    socket.addEventListener('open', () => output('realtime-output', `Connected. Waiting for ready before subscribing to channel:${channel}.`));
    socket.addEventListener('message', (event) => {
      const frame = JSON.parse(event.data);
      if (frame.type === 'ready') socket.send(JSON.stringify({ type: 'subscribe', topics: [`channel:${channel}`] }));
      output('realtime-output', frame);
    });
    socket.addEventListener('close', () => output('realtime-output', 'Socket closed.'));
    return { connecting: true, channel };
  });
  $('close-socket').addEventListener('click', () => socket?.close(1000, 'operator close'));
})();
