/** Experimental Rust v1 SDK, backend Node.js only. Never ship app/agent credentials to browsers.
 * Publish uses POST /api/v1/events with Idempotency-Key; the library host must bind Events there.
 * Response frames require the host's Responses::complete_response adapter (not yet in Gateway).
 */
import { randomUUID } from 'node:crypto';
import { validateHeaderName, validateHeaderValue } from 'node:http';
import { connectAgent, subscribe } from './agent.js';
export type { AgentHandle } from './agent.js';
import type { AgentHandle } from './agent.js';

export type Headers = [string, string][];
export interface EventInput { endpoint_id: string; event_type: string; method: string; path: string; headers: Headers; body: Buffer }
export interface Accepted { event_id: string; delivery_id: string; status: 202 }
export interface DeliveryLease { delivery_id: string; event_id: string; attempt_token: string; generation: number; attempt: number }
export interface HttpResponse { status: number; headers: Headers; body: Buffer }
interface Envelope { version: 1; request_id: string }
export interface Delivery extends Envelope { type: 'delivery'; lease: DeliveryLease; input: EventInput }
export interface RealtimeFrame extends Envelope { type: 'realtime'; channel: string; body: Buffer }
type Correlation = Pick<DeliveryLease, 'delivery_id' | 'event_id' | 'attempt_token'>;
export type Frame = Delivery | RealtimeFrame
  | (Envelope & { type: 'hello'; agent_id: string; max_in_flight: number })
  | (Envelope & Correlation & { type: 'ack' })
  | (Envelope & Correlation & { type: 'response'; response: HttpResponse })
  | (Envelope & { type: 'heartbeat'; lease: DeliveryLease | null });
/** Resolve with void to ACK, or a final HTTP response to settle via Response.
 * Throw to leave work unacked and retryable. There is no NACK frame in v1.
 */
export type ProcessingResult = void | HttpResponse;
export interface PublishReceipt { status: 202; observed_recipients: number }
export interface RelayHubOptions { url: string; token: string; agentId?: string; maxInFlight?: number }

/** Static messages only: no credentials, URLs, payloads or remote error text. */
export class RelayHubError extends Error {
  readonly status: number;
  constructor(status: number, code: string) { super(code); this.name = 'RelayHubError'; this.status = status; }
}
function invalid(): never { throw new RelayHubError(422, 'invalid_frame'); }
function object(value: unknown, keys: string[]): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return invalid();
  const result = value as Record<string, unknown>;
  if (Object.keys(result).length !== keys.length || keys.some(k => !Object.hasOwn(result, k))) return invalid();
  return result;
}
function str(v: unknown): string { if (typeof v !== 'string') return invalid(); return v; }
function uuid(v: unknown): string {
  const s = str(v); if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(s)) return invalid(); return s;
}
function integer(v: unknown, min: number, max: number): number {
  if (typeof v !== 'number' || !Number.isInteger(v) || v < min || v > max) return invalid(); return v;
}
function body(v: unknown): Buffer {
  const s = str(v);
  if (s.length > 1_398_104) throw new RelayHubError(413, 'body_too_large');
  const b = Buffer.from(s, 'base64');
  if (b.toString('base64') !== s) return invalid();
  if (b.length > 1_048_576) throw new RelayHubError(413, 'body_too_large'); return b;
}
function wireBody(b: Buffer): string {
  if (!Buffer.isBuffer(b)) return invalid();
  if (b.length > 1_048_576) throw new RelayHubError(413, 'body_too_large'); return b.toString('base64');
}
function headers(v: unknown): Headers {
  if (!Array.isArray(v)) return invalid();
  return v.map(pair => {
    if (!Array.isArray(pair) || pair.length !== 2) return invalid();
    const [name, value] = pair.map(str);
    try { validateHeaderName(name); validateHeaderValue(name, value); } catch { return invalid(); }
    return [name, value];
  });
}
function event(v: unknown): EventInput {
  const o = object(v, ['endpoint_id', 'event_type', 'method', 'path', 'headers', 'body']);
  const method = str(o.method), path = str(o.path);
  if (!/^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/.test(method) || !path.startsWith('/') || /[^\x21-\x7e]|#|%(?![0-9a-fA-F]{2})/.test(path)) return invalid();
  return { endpoint_id: uuid(o.endpoint_id), event_type: str(o.event_type), method, path, headers: headers(o.headers), body: body(o.body) };
}
function lease(v: unknown): DeliveryLease {
  const o = object(v, ['delivery_id', 'event_id', 'attempt_token', 'generation', 'attempt']);
  return { delivery_id: uuid(o.delivery_id), event_id: uuid(o.event_id), attempt_token: uuid(o.attempt_token), generation: integer(o.generation, 1, 4_294_967_295), attempt: integer(o.attempt, 1, 5) };
}
function response(v: unknown): HttpResponse {
  const o = object(v, ['status', 'headers', 'body']); const status = integer(o.status, 100, 599);
  if (status === 101) return invalid(); return { status, headers: headers(o.headers), body: body(o.body) };
}
function parse(json: string): unknown { try { return JSON.parse(json); } catch { return invalid(); } }
/** Decode exactly the frozen Task 1 JSON/base64 frame contract. */
export function decodeFrame(json: string): Frame {
  if (Buffer.byteLength(json) > 2_097_152) throw new RelayHubError(413, 'frame_too_large');
  const raw = parse(json);
  if (!raw || typeof raw !== 'object') return invalid();
  const v = raw as Record<string, unknown>;
  if (v.version !== 1) throw new RelayHubError(422, 'unsupported_version');
  const base = { version: 1 as const, request_id: uuid(v.request_id) };
  const check = (...keys: string[]) => object(v, ['version', 'request_id', 'type', ...keys]);
  switch (v.type) {
    case 'hello': check('agent_id', 'max_in_flight'); return { ...base, type: 'hello', agent_id: uuid(v.agent_id), max_in_flight: integer(v.max_in_flight, 1, 16) };
    case 'delivery': check('lease', 'input'); return { ...base, type: 'delivery', lease: lease(v.lease), input: event(v.input) };
    case 'ack': case 'response': {
      check('delivery_id', 'event_id', 'attempt_token', ...(v.type === 'response' ? ['response'] : []));
      const correlation = { ...base, delivery_id: uuid(v.delivery_id), event_id: uuid(v.event_id), attempt_token: uuid(v.attempt_token) };
      return v.type === 'ack' ? { ...correlation, type: 'ack' } : { ...correlation, type: 'response', response: response(v.response) };
    }
    case 'realtime': check('channel', 'body'); return { ...base, type: 'realtime', channel: str(v.channel), body: body(v.body) };
    case 'heartbeat': check('lease'); return { ...base, type: 'heartbeat', lease: v.lease === null ? null : lease(v.lease) };
    default: return invalid();
  }
}
/** Encode binary bodies as canonical standard base64, preserving ordered duplicate headers. */
export function encodeFrame(frame: Frame): string {
  let wire: unknown = frame;
  if (frame.type === 'delivery') wire = { ...frame, input: { ...frame.input, body: wireBody(frame.input.body) } };
  if (frame.type === 'response') wire = { ...frame, response: { ...frame.response, body: wireBody(frame.response.body) } };
  if (frame.type === 'realtime') wire = { ...frame, body: wireBody(frame.body) };
  const json = JSON.stringify(wire); decodeFrame(json); return json;
}
function channel(name: string): void {
  if (Buffer.byteLength(name) > 128 || !/^[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*$/.test(name)) throw new RelayHubError(422, 'invalid_channel');
}

export class RelayHub {
  #url: URL;
  #token: string;
  #agentId?: string;
  #maxInFlight: number;
  constructor(options: RelayHubOptions) {
    if (typeof process === 'undefined' || !process.versions?.node || typeof window !== 'undefined') throw new RelayHubError(422, 'backend_only');
    let url: URL; try { url = new URL(options.url); } catch { throw new RelayHubError(422, 'invalid_url'); }
    if (url.username || url.password || url.search || url.hash || !(url.protocol === 'https:' || (url.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)))) throw new RelayHubError(422, 'invalid_url');
    if (typeof options.token !== 'string' || !/^[\x21-\x7e]+$/.test(options.token)) throw new RelayHubError(422, 'invalid_credential');
    this.#url = url; this.#token = options.token; this.#agentId = options.agentId === undefined ? undefined : uuid(options.agentId);
    this.#maxInFlight = integer(options.maxInFlight ?? 16, 1, 16);
  }
  #route(path: string): URL { return new URL(this.#url.toString().replace(/\/$/, '') + path); }
  async #post(path: string, json: string, extra: Record<string, string> = {}): Promise<unknown> {
    if (Buffer.byteLength(json) > 2_097_152) throw new RelayHubError(413, 'frame_too_large');
    try {
      const result = await fetch(this.#route(path), { method: 'POST', headers: { authorization: `Bearer ${this.#token}`, 'content-type': 'application/json', ...extra }, body: json, redirect: 'manual', signal: AbortSignal.timeout(30_000) });
      if (result.status !== 202) { await result.body?.cancel(); throw new RelayHubError(result.status, 'request_rejected'); }
      // Receipts are small. Bound the body even if a broken peer omits Content-Length.
      const reader = result.body?.getReader(); if (!reader) return invalid();
      const parts: Uint8Array[] = []; let length = 0;
      try { for (;;) { const { done, value } = await reader.read(); if (done) break; length += value.length; if (length > 4096) return invalid(); parts.push(value); } }
      finally { await reader.cancel(); }
      return parse(Buffer.concat(parts).toString('utf8'));
    } catch (error) { if (error instanceof RelayHubError) throw error; throw new RelayHubError(503, 'transport_unavailable'); }
  }
  /** At least once: reuse the same key after an ambiguous HTTP failure; acceptance is not processing. */
  async publish(input: EventInput, idempotencyKey: string): Promise<Accepted> {
    if (typeof idempotencyKey !== 'string' || !idempotencyKey || Buffer.byteLength(idempotencyKey) > 256 || /[\x00-\x1f\x7f-\x9f]/.test(idempotencyKey)) throw new RelayHubError(422, 'invalid_idempotency_key');
    const wire = { ...input, body: wireBody(input.body) }; event(wire);
    const o = object(await this.#post('/api/v1/events', JSON.stringify(wire), { 'idempotency-key': idempotencyKey }), ['event_id', 'delivery_id', 'status']);
    if (o.status !== 202) return invalid(); return { event_id: uuid(o.event_id), delivery_id: uuid(o.delivery_id), status: 202 };
  }
  /** Handlers MUST be idempotent by delivery.lease.event_id: disconnects can repeat side effects.
   * Lease/attempt metadata is preserved. No local queue, retry counter or persistence is invented.
   */
  async connectAgent(handler: (delivery: Delivery) => Promise<ProcessingResult>): Promise<AgentHandle> {
    if (!this.#agentId) throw new RelayHubError(422, 'agent_id_required');
    return connectAgent(this.#route('/api/v1/socket'), this.#token, this.#agentId, this.#maxInFlight, handler);
  }
  /** Ephemeral routing receipt only; observed_recipients is not a processing ACK. */
  async publishRealtime(name: string, payload: Buffer): Promise<PublishReceipt> {
    channel(name);
    const o = object(await this.#post('/api/v1/realtime', encodeFrame({ version: 1, request_id: randomUUID(), type: 'realtime', channel: name, body: payload })), ['status', 'observed_recipients']);
    if (o.status !== 202) return invalid(); return { status: 202, observed_recipients: integer(o.observed_recipients, 0, Number.MAX_SAFE_INTEGER) };
  }
  /** Live-only subscription; closes on transport loss. Resubscribe explicitly; missed messages are lost. */
  async subscribe(name: string, handler: (frame: RealtimeFrame) => void | Promise<void>): Promise<AgentHandle> {
    channel(name); return subscribe(this.#route(`/api/v1/realtime/${encodeURIComponent(name)}`), this.#token, name, handler);
  }
}
