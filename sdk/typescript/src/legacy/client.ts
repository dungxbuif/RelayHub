import { RelayHubError } from "../errors.js";
import type { ChannelHandler, EventObserver, FunctionHandler, RelayEvent, SocketFactory, SocketLike, Subscription, TokenProvider } from "../types.js";

export class LegacyClient {
  private readonly observers = new Set<EventObserver>();
  private readonly functions = new Map<string, FunctionHandler>();
  private readonly channels = new Map<string, Set<ChannelHandler>>();
  private socket: SocketLike | undefined;
  private loop: Promise<void> | undefined;
  private stopping = false;
  private activeFunctions = 0;

  constructor(private readonly options: { baseUrl: string; tokenProvider: TokenProvider; socketFactory: SocketFactory; onError?: (error: RelayHubError) => void; sleep?: (ms: number) => Promise<void>; random?: () => number; functionConcurrency?: number }) {}

  observe(handler: EventObserver): Subscription {
    this.observers.add(handler);
    this.start();
    return { close: async () => { this.observers.delete(handler); await this.stopIfIdle(); } };
  }

  subscribeChannel(channel: string, handler: ChannelHandler): Subscription {
    validateChannel(channel);
    let handlers = this.channels.get(channel);
    if (!handlers) {
      handlers = new Set();
      this.channels.set(channel, handlers);
    }
    handlers.add(handler);
    this.start();
    if (this.socket?.readyState === 1) this.subscribe();
    return { close: async () => {
      const current = this.channels.get(channel);
      current?.delete(handler);
      if (current && current.size === 0) this.channels.delete(channel);
      await this.stopIfIdle();
    } };
  }

  handle(name: string, handler: FunctionHandler): Subscription {
    if (!/^[A-Za-z_][A-Za-z0-9_.-]{0,63}$/.test(name)) throw new TypeError("invalid function name");
    if (this.functions.has(name)) throw new RelayHubError("Function handler already registered.", { code: "handler_exists" });
    this.functions.set(name, handler);
    this.start();
    return { close: async () => { this.functions.delete(name); await this.stopIfIdle(); } };
  }

  async close(): Promise<void> {
    this.stopping = true;
    this.observers.clear();
    this.functions.clear();
    this.channels.clear();
    this.socket?.close(1000, "close");
    await this.loop;
    this.loop = undefined;
  }

  private start(): void {
    this.stopping = false;
    this.loop ??= this.run();
    if (this.socket?.readyState === 1) this.subscribe();
  }

  private async stopIfIdle(): Promise<void> {
    if (this.observers.size || this.functions.size || this.channels.size) return;
    this.stopping = true;
    this.socket?.close(1000, "idle");
    await this.loop;
    this.loop = undefined;
  }

  private async run(): Promise<void> {
    let attempt = 0;
    while (!this.stopping && (this.observers.size > 0 || this.functions.size > 0 || this.channels.size > 0)) {
      try {
        const token = await this.options.tokenProvider(["ws:connect", "ws:subscribe", "ws:read"]);
        await this.connect(token);
        attempt = 0;
      } catch (error) { this.report(error); }
      if (!this.stopping && (this.observers.size || this.functions.size || this.channels.size)) {
        const base = Math.min(30_000, 250 * 2 ** Math.min(attempt++, 7));
        const jitter = 0.5 + (this.options.random?.() ?? Math.random());
        await (this.options.sleep ?? ((ms) => new Promise<void>((resolve) => setTimeout(resolve, ms))))(Math.floor(base * jitter));
      }
    }
  }

  private connect(token: string): Promise<void> {
    const url = new URL("/ws", this.options.baseUrl);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    url.searchParams.set("token", token);
    const socket = this.options.socketFactory(url.toString());
    this.socket = socket;
    return new Promise((resolve) => {
      socket.addEventListener("message", (event) => this.message(socket, event.data));
      socket.addEventListener("error", () => this.report(new RelayHubError("Observer transport failed.", { code: "transport_error", retryable: true })));
      socket.addEventListener("close", () => { if (this.socket === socket) this.socket = undefined; resolve(); }, { once: true });
    });
  }

  private message(socket: SocketLike, data: unknown): void {
    if (typeof data !== "string") return;
    let frame: any;
    try { frame = JSON.parse(data); } catch { return; }
    if (frame.type === "ready") this.subscribe();
    else if (frame.type === "event") {
      for (const observer of this.observers) void Promise.resolve(observer(frame.event as RelayEvent)).catch((error) => this.report(error));
    } else if (frame.type === "channel.message") this.channelMessage(frame);
    else if (frame.type === "rpc.invoke") void this.invoke(socket, frame);
    else if (frame.type === "error") this.report(new RelayHubError(frame.message ?? "RelayHub socket error", { code: frame.code ?? "socket_error" }));
  }

  private subscribe(): void {
    const topics: string[] = [];
    if (this.observers.size) topics.push("events");
    if (this.functions.size) topics.push("functions");
    for (const channel of this.channels.keys()) topics.push(`channel:${channel}`);
    if (topics.length && this.socket?.readyState === 1) this.socket.send(JSON.stringify({ type: "subscribe", topics }));
  }


  private channelMessage(frame: { channel?: string; publisher_app_id?: string; data?: Record<string, any> }): void {
    if (!frame.channel) return;
    const handlers = this.channels.get(frame.channel);
    if (!handlers) return;
    for (const handler of handlers) {
      void Promise.resolve(handler({ channel: frame.channel, publisherAppId: frame.publisher_app_id ?? "", data: frame.data ?? {} })).catch((error) => this.report(error));
    }
  }

  private async invoke(socket: SocketLike, frame: { invocation_id: string; function: string; input: Record<string, any>; deadline: string }): Promise<void> {
    const handler = this.functions.get(frame.function);
    if (!handler || this.activeFunctions >= (this.options.functionConcurrency ?? 16)) {
      if (socket.readyState === 1) socket.send(JSON.stringify({ type: "rpc.result", invocation_id: frame.invocation_id, ok: false, error: { code: "handler_busy", message: "No handler capacity is available." } }));
      return;
    }
    this.activeFunctions++;
    try {
      const result = await handler(frame.input, { invocationId: frame.invocation_id, deadline: frame.deadline });
      if (this.socket === socket && socket.readyState === 1) socket.send(JSON.stringify({ type: "rpc.result", invocation_id: frame.invocation_id, ok: true, result }));
    } catch (error) {
      if (this.socket === socket && socket.readyState === 1) socket.send(JSON.stringify({ type: "rpc.result", invocation_id: frame.invocation_id, ok: false, error: { code: "handler_failed", message: error instanceof Error ? error.message.slice(0, 1024) || "Handler failed." : "Handler failed." } }));
    } finally { this.activeFunctions--; }
  }

  private report(error: unknown): void {
    this.options.onError?.(error instanceof RelayHubError ? error : new RelayHubError(error instanceof Error ? error.message : "RelayHub observer failed.", { code: "handler_error" }));
  }
}

function validateChannel(channel: string): void {
  if (!/^[A-Za-z0-9_.:-]{1,128}$/.test(channel)) throw new TypeError("invalid channel name");
}
