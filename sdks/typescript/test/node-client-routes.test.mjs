import assert from 'node:assert/strict';
import test from 'node:test';
import { RelayHubClient, verifyCallbackSignature } from '../dist/esm/node.js';

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

test('queue v2 helpers use signed app-scoped routes and preserve receipts', async () => {
  const { client, requests } = makeClient((request) => {
    if (request.url.pathname === '/api/v2/subscriptions' && request.method === 'POST') return jsonResponse({ id: 'sub_1', name: 'orders' }, { status: 201 });
    if (request.url.pathname.endsWith('/pull')) return jsonResponse({ items: [{ id: 'qdl_1', receipt: 'opaque-receipt' }] });
    if (request.url.pathname.endsWith('/settle')) return jsonResponse({ items: [{ receipt: request.body.items[0].receipt, status: 'acked' }] });
    if (request.url.pathname.endsWith('/metrics')) return jsonResponse({ available: 0, in_flight: 0, acknowledged: 1, dead_letter: 0 });
    throw new Error(`unexpected ${request.method} ${request.url.pathname}`);
  });
  const subscription = await client.queue.create({ name: 'orders' });
  const batch = await client.queue.pull(subscription.id, { max_messages: 10, visibility_seconds: 30 });
  const settled = await client.queue.settle(subscription.id, [{ receipt: batch.items[0].receipt, disposition: 'ack' }]);
  const depth = await client.queue.metrics(subscription.id);
  assert.equal(settled.items[0].status, 'acked');
  assert.equal(depth.acknowledged, 1);
  assert.deepEqual(requests.map((request) => `${request.method} ${request.url.pathname}`), [
    'POST /api/v2/subscriptions',
    'POST /api/v2/subscriptions/sub_1/pull',
    'POST /api/v2/subscriptions/sub_1/settle',
    'GET /api/v2/subscriptions/sub_1/metrics',
  ]);
  for (const request of requests) assert.ok(request.headers['X-RelayHub-Signature']);
});

test('queue advanced helpers expose schedules, drain and bounded DLQ export', async () => {
  const {client, requests} = makeClient(request => {
    if (request.url.pathname.endsWith('/schedules') && request.method === 'POST') return jsonResponse({id: 'qsch_1', policy_version: 1});
    if (request.url.pathname.endsWith('/schedules')) return jsonResponse({items: [{id: 'qsch_1'}]});
    if (request.url.pathname.endsWith('/drain') && request.method === 'POST') return jsonResponse({subscription_id: 'sub_1', status: 'draining', in_flight: 1}, {status: 202});
    if (request.url.pathname.endsWith('/drain')) return jsonResponse({subscription_id: 'sub_1', status: 'drained', in_flight: 0});
    if (request.url.pathname.endsWith('/dead-letters/export')) return jsonResponse({items: [{delivery_id: 'qdl_1'}]});
    throw new Error(`unexpected ${request.method} ${request.url.pathname}`);
  });
  const input = {name: 'daily', cron_expression: '0 9 * * *', timezone: 'Asia/Ho_Chi_Minh', event_type: 'report.daily', data: {kind: 'daily'}};
  await client.queue.createSchedule('sub_1', input);
  await client.queue.listSchedules('sub_1');
  await client.queue.drain('sub_1', 45);
  await client.queue.drainStatus('sub_1');
  await client.queue.exportDeadLetters('sub_1', {limit: 25, cursor: 'qdl_0'});
  assert.deepEqual(requests.map(request => `${request.method} ${request.url.pathname}${request.url.search}`), [
    'POST /api/v2/subscriptions/sub_1/schedules',
    'GET /api/v2/subscriptions/sub_1/schedules',
    'POST /api/v2/subscriptions/sub_1/drain',
    'GET /api/v2/subscriptions/sub_1/drain',
    'GET /api/v2/subscriptions/sub_1/dead-letters/export?format=json&limit=25&cursor=qdl_0',
  ]);
});

test('realtime file helpers keep bytes out of RelayHub and use signed metadata routes', async () => {
  const {client, requests} = makeClient(request => {
    if (request.url.pathname === '/api/v2/realtime/files') return jsonResponse({file: {id: 'file_1', status: 'pending'}, upload_url: 'https://objects.example/upload', required_headers: {}}, {status: 201});
    if (request.url.pathname.endsWith('/complete')) return jsonResponse({id: 'file_1', status: 'ready'});
    if (request.url.pathname.endsWith('/download')) return jsonResponse({file: {id: 'file_1', status: 'ready'}, download_url: 'https://objects.example/download'});
    throw new Error(`unexpected ${request.method} ${request.url.pathname}`);
  });
  const sha256 = 'a'.repeat(64);
  const upload = await client.realtime.createFile({channel: 'room', name: 'photo.png', mime_type: 'image/png', size_bytes: 12, sha256});
  await client.realtime.completeFile(upload.file.id);
  await client.realtime.fileDownload(upload.file.id);
  assert.deepEqual(requests.map(request => `${request.method} ${request.url.pathname}`), ['POST /api/v2/realtime/files', 'POST /api/v2/realtime/files/file_1/complete', 'GET /api/v2/realtime/files/file_1/download']);
  assert.equal(requests[0].body.bytes, undefined);
  for (const request of requests) assert.ok(request.headers['X-RelayHub-Signature']);
});

test('push helpers are app-signed and never return or echo device tokens', async () => {
  const {client, requests} = makeClient(request => {
    if (request.url.pathname === '/api/v2/realtime/push/devices' && request.method === 'POST') return jsonResponse({id: 'device_1', app_id: 'app_1', provider: 'fcm'}, {status: 201});
    if (request.url.pathname.endsWith('/devices/device_1')) return jsonResponse({});
    if (request.url.pathname.endsWith('/notifications')) return jsonResponse({outcomes: [{id: 'push_1', device_id: 'device_1', status: 'delivered', provider: 'fcm'}]}, {status: 202});
    throw new Error(`unexpected ${request.method} ${request.url.pathname}`);
  });
  const device = await client.realtime.registerPushDevice('fcm', 'provider-device-token');
  await client.realtime.bindPushDevice('private:room', device.id);
  const result = await client.realtime.publishPush('private:room', {title: 'Order ready', data: {order_id: 'o1'}});
  await client.realtime.deletePushDevice(device.id);
  assert.equal(device.token, undefined);
  assert.equal(result.outcomes[0].status, 'delivered');
  assert.deepEqual(requests.map(request => `${request.method} ${request.url.pathname}`), [
    'POST /api/v2/realtime/push/devices',
    'PUT /api/v2/realtime/push/channels/private%3Aroom/devices/device_1',
    'POST /api/v2/realtime/push/channels/private%3Aroom/notifications',
    'DELETE /api/v2/realtime/push/devices/device_1',
  ]);
  for (const request of requests) assert.ok(request.headers['X-RelayHub-Signature']);
});

test('callback verifier checks exact bytes, target and timestamp window', () => {
  const body = new TextEncoder().encode('{"event":{"id":"evt_1"}}');
  const signature = 'c6f79668e3c43ff6102c81660e9964a5389574c3225385ffc078f889840f2b27';
  const input = {secret: 'secret', timestamp: '1770000000', requestTarget: '/callbacks/realtime', body, signature, now: 1770000000000};
  assert.equal(verifyCallbackSignature(input), true);
  assert.equal(verifyCallbackSignature({...input, body: new TextEncoder().encode('{}')}), false);
  assert.equal(verifyCallbackSignature({...input, now: 1770001000000}), false);
});
