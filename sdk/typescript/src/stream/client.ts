import { RelayHubError, RetryDelivery } from "../errors.js";
import type { ConsumerHandle, EventHandler, RelayEvent, SocketFactory, SocketLike, TokenProvider } from "../types.js";

export interface StreamClientOptions {
  baseUrl: string;
  tokenProvider: TokenProvider;
  socketFactory?: SocketFactory;
  random?: () => number;
  sleep?: (milliseconds: number) => Promise<void>;
  onError?: (error: RelayHubError) => void;
}

interface ConsumerState { handler: EventHandler; concurrency: number }

export class RelayHubStreamClient {
  private readonly options: StreamClientOptions;
  private consumer: ConsumerState | undefined;
  private socket: SocketLike | undefined;
  private loop: Promise<void> | undefined;
  private stopping = false;
  private readonly inflight = new Map<string, Promise<void>>();

  constructor(options: StreamClientOptions) {
    if (!options.baseUrl || !options.tokenProvider) throw new TypeError("baseUrl and tokenProvider are required");
    this.options = options;
  }

  consume(handler: EventHandler, options: { concurrency?: number } = {}): ConsumerHandle {
    if (this.consumer) throw new RelayHubError("A durable consumer is already active.", { code: "consumer_already_started" });
    const concurrency = options.concurrency ?? 16;
    if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 256) throw new TypeError("concurrency must be an integer from 1 through 256");
    this.consumer = { handler, concurrency };
    this.stopping = false;
    this.loop ??= this.run();
    return { drain: (drainOptions) => this.drain(drainOptions) };
  }

  async drain(options: { timeoutMs?: number } = {}): Promise<void> {
    this.stopping = true;
    const timeoutMs = options.timeoutMs ?? 10_000;
    const all = Promise.allSettled([...this.inflight.values()]);
    await Promise.race([all, new Promise<void>((resolve) => setTimeout(resolve, timeoutMs))]);
    for (const deliveryId of this.inflight.keys()) this.send({ type: "delivery.nack", delivery_id: deliveryId, delay_ms: 0 });
    this.socket?.close(1000, "drain");
    await this.loop;
    this.loop = undefined;
    this.consumer = undefined;
  }

  async close(options: { drain?: boolean; timeoutMs?: number } = {}): Promise<void> {
    if (options.drain !== false) {
      const drainOptions: { timeoutMs?: number } = {};
      if (options.timeoutMs !== undefined) drainOptions.timeoutMs = options.timeoutMs;
      await this.drain(drainOptions);
    }
    else {
      this.stopping = true;
      this.socket?.close(1000, "close");
      await this.loop;
      this.loop = undefined;
      this.consumer = undefined;
    }
  }

  private async run(): Promise<void> {
    let attempt = 0;
    while (!this.stopping && this.consumer) {
      try {
        const token = await this.options.tokenProvider(["stream:connect"]);
        await this.runConnection(token);
        attempt = 0;
      } catch (error) {
        this.report(error);
      }
      if (!this.stopping && this.consumer) {
        const base = Math.min(30_000, 250 * 2 ** Math.min(attempt++, 7));
        const jitter = 0.5 + (this.options.random?.() ?? Math.random());
        await (this.options.sleep ?? ((ms) => new Promise<void>((resolve) => setTimeout(resolve, ms))))(Math.floor(base * jitter));
      }
    }
  }

  private runConnection(token: string): Promise<void> {
    const socket = (this.options.socketFactory ?? browserSocketFactory)(streamURL(this.options.baseUrl, token), "relayhub.stream.v1");
    this.socket = socket;
    return new Promise((resolve, reject) => {
      socket.addEventListener("open", () => {
        if (socket.protocol !== "relayhub.stream.v1") {
          socket.close(4406, "unsupported subprotocol");
          reject(new RelayHubError("RelayHub did not select relayhub.stream.v1.", { code: "unsupported_version" }));
        }
      }, { once: true });
      socket.addEventListener("message", (event) => this.onMessage(socket, event.data), undefined);
      socket.addEventListener("error", () => this.report(new RelayHubError("Streaming transport failed.", { code: "transport_error", retryable: true })));
      socket.addEventListener("close", () => { if (this.socket === socket) this.socket = undefined; resolve(); }, { once: true });
    });
  }

  private onMessage(socket: SocketLike, data: unknown): void {
    if (typeof data !== "string") {
      socket.close(4400, "JSON text required");
      return;
    }
    let frame: any;
    try { frame = JSON.parse(data); } catch { socket.close(4400, "invalid JSON"); return; }
    if (frame.type === "ready" && this.consumer) {
      this.send({ type: "consumer.start", protocol_version: 1, consumer: "default", max_in_flight: this.consumer.concurrency });
    } else if (frame.type === "event.delivery" && this.consumer) {
      if (this.inflight.size >= this.consumer.concurrency) {
        this.send({ type: "delivery.nack", delivery_id: frame.delivery_id, delay_ms: 1_000 });
        return;
      }
      const task = this.handleDelivery(socket, frame);
      this.inflight.set(frame.delivery_id, task);
      void task.finally(() => this.inflight.delete(frame.delivery_id));
    } else if (frame.type === "error") {
      this.report(new RelayHubError(frame.message ?? "RelayHub stream error", { code: frame.code ?? "stream_error", retryable: frame.retryable === true }));
    }
  }

  private async handleDelivery(socket: SocketLike, frame: { delivery_id: string; attempt: number; event: RelayEvent }): Promise<void> {
    try {
      await this.consumer!.handler(frame.event, { deliveryId: frame.delivery_id, eventId: frame.event.id, attempt: frame.attempt });
      if (this.socket === socket && socket.readyState === 1) this.send({ type: "delivery.ack", delivery_id: frame.delivery_id });
    } catch (error) {
      const delay = error instanceof RetryDelivery ? error.delayMs : 1_000;
      if (this.socket === socket && socket.readyState === 1) this.send({ type: "delivery.nack", delivery_id: frame.delivery_id, delay_ms: delay });
    }
  }

  private send(frame: unknown): void {
    if (this.socket?.readyState === 1) this.socket.send(JSON.stringify(frame));
  }

  private report(error: unknown): void {
    const structured = error instanceof RelayHubError ? error : new RelayHubError(error instanceof Error ? error.message : "RelayHub stream failed.", { code: "transport_error", retryable: true });
    this.options.onError?.(structured);
  }
}

function streamURL(baseUrl: string, token: string): string {
  const url = new URL("/api/v1/stream", baseUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.searchParams.set("token", token);
  return url.toString();
}

function browserSocketFactory(url: string, protocols?: string | string[]): SocketLike {
  if (typeof globalThis.WebSocket !== "function") throw new RelayHubError("No WebSocket implementation is available.", { code: "transport_unavailable" });
  return new globalThis.WebSocket(url, protocols) as unknown as SocketLike;
}
