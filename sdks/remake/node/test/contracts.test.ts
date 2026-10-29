import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { once } from 'node:events';

const fixtures = JSON.parse(await readFile(new URL('../../../../contracts/v1/fixtures.json', import.meta.url), 'utf8'));
async function sdk() {
  return import('../src/client.js').catch(() => assert.fail('Node SDK is not implemented'));
}

test('golden_frames_match', async () => {
  const { decodeFrame, encodeFrame } = await sdk();
  for (const wire of fixtures.frames) {
    const frame = decodeFrame(JSON.stringify(wire));
    assert.deepEqual(JSON.parse(encodeFrame(frame)), wire);
    if (frame.type === 'delivery') {
      assert.deepEqual([...frame.input.body], [0, 255, 127, 128]);
      assert.deepEqual(frame.input.headers, [['X-Tag', 'one'], ['X-Tag', 'two']]);
      assert.deepEqual(frame.lease, fixtures.delivery_lease);
    }
    if (frame.type === 'response') assert.deepEqual(frame.response.headers, fixtures.http_response.headers);
  }
});

test('strict_frozen_boundaries', async () => {
  const { decodeFrame, encodeFrame } = await sdk();
  const wire = fixtures.frames[1];
  for (const bad of [
    { ...wire, version: 2 }, { ...wire, workspace_id: wire.request_id },
    { ...wire, type: 'nack' }, { ...wire, request_id: 'bad' },
    { ...wire, lease: { ...wire.lease, generation: 0 } },
    { ...wire, lease: { ...wire.lease, attempt: 6 } },
    { ...wire, input: { ...wire.input, body: 'AP9/gA' } },
    { ...wire, input: { ...wire.input, body: 'AP9/gB==' } },
    { ...wire, input: { ...wire.input, target_url: 'http://example.test' } },
    { ...wire, input: { ...wire.input, path: '/bad%ZZ' } },
    { ...wire, input: { ...wire.input, headers: [['Bad\r\nName', 'x']] } },
    { ...fixtures.frames[0], max_in_flight: 17 },
    { ...fixtures.frames[3], response: { ...fixtures.http_response, status: 101 } },
    { version: 1, request_id: wire.request_id, type: 'heartbeat' },
  ]) assert.throws(() => decodeFrame(JSON.stringify(bad)));
  const frame = decodeFrame(JSON.stringify(wire));
  assert.equal(frame.type, 'delivery');
  if (frame.type !== 'delivery') return;
  frame.input.body = Buffer.alloc(1_048_576);
  assert.equal(decodeFrame(encodeFrame(frame)).type, 'delivery');
  frame.input.body = Buffer.alloc(1_048_577);
  assert.throws(() => encodeFrame(frame));
  assert.throws(() => decodeFrame(' '.repeat(2_097_153)));
});

test('unicode_header_values_match_frozen_rust_validation', async () => {
  const { decodeFrame, encodeFrame } = await sdk();
  const wire = JSON.parse(JSON.stringify(fixtures.frames[1]));
  wire.input.headers = [['X-Label', 'こんにちは']];
  const frame = decodeFrame(JSON.stringify(wire));
  assert.equal(frame.type, 'delivery');
  if (frame.type !== 'delivery') return;
  assert.deepEqual(frame.input.headers, [['X-Label', 'こんにちは']]);
  const roundTrip = decodeFrame(encodeFrame(frame));
  assert.equal(roundTrip.type, 'delivery');
  if (roundTrip.type !== 'delivery') return;
  assert.deepEqual(roundTrip.input.headers, [['X-Label', 'こんにちは']]);
  for (const value of ['\u0000', '\u0008', '\u000b', '\u000c', '\u000e', '\u001f', '\u007f']) {
    wire.input.headers = [['X-Label', value]];
    assert.throws(() => decodeFrame(JSON.stringify(wire)), `control byte ${value.charCodeAt(0)}`);
  }
  wire.input.headers = [['X-Label', '\t']];
  assert.equal(decodeFrame(JSON.stringify(wire)).type, 'delivery');
});

test('idempotency_whitespace_is_rejected_before_transport', async () => {
  const { RelayHub } = await sdk();
  let requests = 0;
  const server = createServer(() => { requests++; });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  const client = new RelayHub({ url: `http://127.0.0.1:${address.port}`, token: 'synthetic-test-token' });
  const input = { ...fixtures.event_input, body: Buffer.from([0, 255, 127, 128]) };
  await assert.rejects(client.publish(input, ' order-1'), (error: unknown) => (error as { status?: number }).status === 422);
  await assert.rejects(client.publish(input, 'order-1 '), (error: unknown) => (error as { status?: number }).status === 422);
  assert.equal(requests, 0);
  server.close();
});

test('shutdown_budget_includes_socket_close', async t => {
  const { WebSocketServer } = await import('ws');
  const { setImmediate: turn } = await import('node:timers/promises');
  const { RelayHub } = await sdk();
  const server = new WebSocketServer({ host: '127.0.0.1', port: 0, handleProtocols: () => 'relayhub.v1' });
  await once(server, 'listening');
  const address = server.address(); assert.ok(address && typeof address !== 'string');
  let started!: () => void, release!: () => void;
  const began = new Promise<void>(r => { started = r; }), work = new Promise<void>(r => { release = r; });
  server.on('connection', ws => { ws.once('message', () => ws.send(JSON.stringify(fixtures.frames[1]))); });
  const c = new RelayHub({ url: `http://127.0.0.1:${address.port}`, token: 'synthetic-test-token', agentId: fixtures.frames[0].agent_id });
  const agent = await c.connectAgent(async () => { started(); await work; });
  await began;
  for (const ws of server.clients) ws.pause(); // A peer that never answers the close handshake.
  t.mock.timers.enable({ apis: ['setTimeout'] });
  let closed = false; const closing = agent.close().then(() => { closed = true; });
  try {
    t.mock.timers.tick(29_500); release(); await turn(); await turn();
    assert.equal(closed, false);
    t.mock.timers.tick(500); await turn(); await turn();
    assert.equal(closed, true, 'the 30-second budget includes the close handshake');
  } finally {
    t.mock.timers.tick(1000);
    for (const ws of server.clients) { ws.resume(); ws.terminate(); }
    await closing; t.mock.timers.reset();
    await new Promise<void>(resolve => server.close(() => resolve()));
  }
});
