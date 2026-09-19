import assert from 'node:assert/strict';
import test from 'node:test';
import { RelayHubClient } from '../dist/esm/node.js';

function jsonResponse(body, init = {}) {
  return new Response(JSON.stringify(body), { status: 200, headers: { 'Content-Type': 'application/json', ...(init.headers ?? {}) }, ...init });
}

function makeClient(handler) {
  const requests = [];
  const fetch = async (url, init = {}) => {
    const body = init.body ? JSON.parse(init.body) : undefined;
    requests.push({ url: new URL(url), method: init.method, headers: init.headers, body });
    return handler(requests.at(-1));
  };
  const client = new RelayHubClient({
    baseUrl: 'https://relayhub.example',
    apiKey: 'rhak_test',
    hmacSecret: 'secret',
    adminToken: 'admin-token',
    fetch,
    socketFactory: () => { throw new Error('socket not expected'); },
    now: () => 1_770_000_000_000,
  });
  return { client, requests };
}

test('publishes routed events and realtime channel messages through current v1 routes', async () => {
  const { client, requests } = makeClient((request) => {
    if (request.url.pathname === '/api/v1/events') return jsonResponse({ event: { id: 'evt_1' }, jobs: [] });
    if (request.url.pathname === '/api/v1/realtime/channels/orders/publish') return jsonResponse({ accepted: true });
    throw new Error(`unexpected ${request.method} ${request.url.pathname}`);
  });

  await client.events.publish({ type: 'order.created', data: { id: 'o1' } }, { idempotencyKey: 'idem-1' });
  await client.realtime.publish('orders', { id: 'o1' });

  assert.equal(requests[0].method, 'POST');
  assert.equal(requests[0].url.pathname, '/api/v1/events');
  assert.deepEqual(requests[0].body, { type: 'order.created', data: { id: 'o1' } });
  assert.equal(requests[0].headers['Idempotency-Key'], 'idem-1');
  assert.equal(requests[1].url.pathname, '/api/v1/realtime/channels/orders/publish');
  assert.deepEqual(requests[1].body, { data: { id: 'o1' } });
});

test('admin helpers use bearer auth for app and routing management', async () => {
  const { client, requests } = makeClient((request) => {
    if (request.url.pathname === '/api/v1/apps') return jsonResponse(request.method === 'GET' ? [] : { app_id: 'app_1', api_key: 'rhak', hmac_secret: 'secret' });
    if (request.url.pathname === '/api/v1/routing/rules') return jsonResponse(request.method === 'GET' ? [] : { id: 'rr_1', event_type: 'order.created', target_app_id: 'app_2' });
    throw new Error(`unexpected ${request.method} ${request.url.pathname}`);
  });

  await client.apps.create({ name: 'orders', delivery_mode: 'websocket' });
  await client.apps.list();
  await client.routing.createRule({ event_type: 'order.created', target_app_id: 'app_2', realtime_channel: 'orders' });
  await client.routing.listRules();

  assert.deepEqual(requests.map((request) => `${request.method} ${request.url.pathname}`), [
    'POST /api/v1/apps',
    'GET /api/v1/apps',
    'POST /api/v1/routing/rules',
    'GET /api/v1/routing/rules',
  ]);
  for (const request of requests) assert.equal(request.headers.Authorization, 'Bearer admin-token');
});
