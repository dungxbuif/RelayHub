import { randomUUID } from 'node:crypto';
import WebSocket from 'ws';
import { decodeFrame, encodeFrame, RelayHubError } from './client.js';
import type { Delivery, DeliveryLease, Frame, ProcessingResult, RealtimeFrame } from './client.js';

export interface AgentHandle {
  /** Stop admitting handlers/reconnecting; drain up to 30 seconds. Unfinished work stays unacked.
   * A timed-out JavaScript handler cannot be cancelled and may still complete its side effects.
   */
  close(): Promise<void>;
}
function socket(url: URL, token: string): WebSocket {
  const address = new URL(url); address.protocol = address.protocol === 'https:' ? 'wss:' : 'ws:';
  return new WebSocket(address, 'relayhub.v1', {
    headers: { authorization: `Bearer ${token}` }, followRedirects: false,
    handshakeTimeout: 10_000, maxPayload: 2_097_152, perMessageDeflate: false,
  });
}
function send(ws: WebSocket, frame: Frame): Promise<void> {
  return new Promise((resolve, reject) => {
    if (ws.readyState !== WebSocket.OPEN) { reject(new RelayHubError(503, 'transport_unavailable')); return; }
    ws.send(encodeFrame(frame), error => error ? reject(new RelayHubError(503, 'transport_unavailable')) : resolve());
  });
}
function opened(ws: WebSocket): Promise<void> {
  return new Promise((resolve, reject) => {
    const fail = () => { reject(new RelayHubError(503, 'transport_unavailable')); };
    ws.once('error', fail);
    ws.once('close', fail);
    ws.once('unexpected-response', (_request, response) => { response.destroy(); ws.terminate(); reject(new RelayHubError(response.statusCode ?? 503, 'connection_rejected')); });
    ws.once('open', () => {
      if (ws.protocol !== 'relayhub.v1') { ws.terminate(); fail(); return; }
      resolve();
    });
  });
}
async function closeSocket(ws?: WebSocket): Promise<void> {
  if (!ws || ws.readyState === WebSocket.CLOSED) return;
  await new Promise<void>(resolve => {
    const timer = setTimeout(() => { ws.terminate(); }, 1000);
    ws.once('close', () => { clearTimeout(timer); resolve(); });
    if (ws.readyState === WebSocket.OPEN) ws.close(); else ws.terminate();
  });
}

export async function connectAgent(url: URL, token: string, agentId: string, max: number, handler: (d: Delivery) => Promise<ProcessingResult>): Promise<AgentHandle> {
  let stopping = false, current: WebSocket | undefined, reconnectTimer: NodeJS.Timeout | undefined;
  let closing: Promise<void> | undefined, backoff = 1000;
  const active = new Map<string, { lease: DeliveryLease; ws: WebSocket; task: Promise<void> }>();
  async function connect(): Promise<void> {
    const ws = socket(url, token); current = ws;
    const heartbeat = setInterval(() => {
      if (ws.readyState !== WebSocket.OPEN) return;
      const frames: Frame[] = [{ version: 1, request_id: randomUUID(), type: 'heartbeat', lease: null }];
      for (const job of active.values()) if (job.ws === ws) frames.push({ version: 1, request_id: randomUUID(), type: 'heartbeat', lease: job.lease });
      for (const frame of frames) void send(ws, frame).catch(() => ws.terminate());
    }, 20_000);
    ws.on('error', () => {}); // Transport errors are redacted; close drives reconnect.
    ws.once('close', () => {
      clearInterval(heartbeat);
      if (!stopping) { reconnectTimer = setTimeout(() => { if (!stopping) void connect().catch(() => {}); }, backoff); backoff = Math.min(backoff * 2, 30_000); }
    });
    ws.on('message', bytes => {
      if (stopping) return;
      let frame: Frame;
      try { frame = decodeFrame(bytes.toString()); } catch { ws.terminate(); return; }
      if (frame.type !== 'delivery' || active.size >= max || active.has(frame.lease.delivery_id)) { ws.terminate(); return; }
      const delivery = frame;
      // Copy correlation before invoking untrusted business code; user mutation must not change ACK fencing.
      const lease = { ...delivery.lease }, request_id = delivery.request_id;
      if ([...active.values()].some(job => job.ws === ws && job.lease.attempt_token === lease.attempt_token)) { ws.terminate(); return; }
      backoff = 1000;
      const task = Promise.resolve().then(async () => {
        const result = await handler(delivery);
        if (ws.readyState !== WebSocket.OPEN) return;
        const correlation = { version: 1 as const, request_id, delivery_id: lease.delivery_id, event_id: lease.event_id, attempt_token: lease.attempt_token };
        if (result === undefined) await send(ws, { ...correlation, type: 'ack' });
        else {
          // Task 5 only settles final HTTP responses, though the frozen frame parser accepts 1xx.
          if (result.status < 200) throw new RelayHubError(422, 'invalid_response');
          await send(ws, { ...correlation, type: 'response', response: result });
        }
      }).catch(() => {
        // Frozen v1 has no NACK. Close without settlement; the server lease/retry policy owns recovery.
        ws.terminate();
      }).finally(() => { active.delete(lease.delivery_id); });
      active.set(lease.delivery_id, { lease, ws, task });
    });
    await opened(ws);
    if (stopping) { ws.terminate(); return; }
    await send(ws, { version: 1, request_id: randomUUID(), type: 'hello', agent_id: agentId, max_in_flight: max });
  }
  try { await connect(); } catch (error) { stopping = true; clearTimeout(reconnectTimer); current?.terminate(); throw error; }
  return {
    close() {
      if (closing) return closing;
      stopping = true; clearTimeout(reconnectTimer);
      closing = (async () => {
        let deadline!: NodeJS.Timeout;
        await Promise.race([
          Promise.all([...active.values()].map(job => job.task)).then(() => closeSocket(current)),
          new Promise<void>(resolve => { deadline = setTimeout(() => { current?.terminate(); resolve(); }, 30_000); }),
        ]);
        clearTimeout(deadline);
      })();
      return closing;
    },
  };
}

export async function subscribe(url: URL, token: string, channel: string, handler: (f: RealtimeFrame) => void | Promise<void>): Promise<AgentHandle> {
  const ws = socket(url, token); let stopped = false, pending = 0, closing: Promise<void> | undefined;
  let tail = Promise.resolve();
  ws.on('error', () => {});
  ws.on('message', bytes => {
    if (stopped) return;
    try {
      const frame = decodeFrame(bytes.toString());
      if (frame.type !== 'realtime' || frame.channel !== channel || pending >= 64) throw new RelayHubError(422, 'invalid_realtime');
      pending++;
      tail = tail.then(async () => { if (!stopped) await handler(frame); }).catch(() => { stopped = true; ws.terminate(); }).finally(() => { pending--; });
    } catch { stopped = true; ws.terminate(); }
  });
  await opened(ws);
  return { close() { stopped = true; return closing ??= closeSocket(ws); } };
}
