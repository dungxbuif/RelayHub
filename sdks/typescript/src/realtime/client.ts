import { RelayHubError } from "../errors.js";
import type { JSONValue, PresenceMessage, RealtimeAudience, RealtimeMessage, RealtimeTokenProvider, SocketFactory, SocketLike } from "../types.js";

export interface RealtimeClientOptions {
  baseUrl: string;
  clientId: string;
  channels: Record<string, Array<"subscribe" | "publish" | "presence">>;
  tokenProvider: RealtimeTokenProvider;
  socketFactory: SocketFactory;
  onMessage?: (message: RealtimeMessage) => void | Promise<void>;
  onPresence?: (presence: PresenceMessage) => void | Promise<void>;
  onError?: (error: RelayHubError) => void;
}

export class RelayHubRealtimeClient {
  private socket: SocketLike | undefined;
  constructor(private readonly options: RealtimeClientOptions) {
    validateClientId(options.clientId);
    for (const [channel, actions] of Object.entries(options.channels)) {
      validateChannel(channel);
      if (!actions.length || new Set(actions).size !== actions.length) throw new TypeError("invalid realtime actions");
    }
  }

  async connect(): Promise<void> {
    if (this.socket) return;
    const token = await this.options.tokenProvider({ clientId: this.options.clientId, channels: this.options.channels });
    if (!token) throw new TypeError("token provider returned an empty token");
    const url = new URL("/ws", this.options.baseUrl);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    url.searchParams.set("token", token);
    const socket = this.options.socketFactory(url.toString(), "relayhub.realtime.v2");
    this.socket = socket;
    await new Promise<void>((resolve, reject) => {
      let settled = false;
      socket.addEventListener("message", (event) => {
        const frame = this.decode(event.data);
        if (!frame) return;
        if (frame.type === "ready" && frame.protocol === "relayhub.realtime.v2") { settled = true; resolve(); }
        else this.dispatch(frame);
      });
      socket.addEventListener("error", () => { if (!settled) reject(new RelayHubError("Realtime connection failed.", { code: "transport_error", retryable: true })); });
      socket.addEventListener("close", () => { if (this.socket === socket) this.socket = undefined; if (!settled) reject(new RelayHubError("Realtime connection closed before ready.", { code: "transport_error", retryable: true })); }, { once: true });
    });
  }

  subscribe(channels: string[]): void { this.channelsFrame("subscribe", channels); }
  unsubscribe(channels: string[]): void { this.channelsFrame("unsubscribe", channels); }
  publish(channel: string, data: Record<string, JSONValue>, audience: RealtimeAudience = { type: "all" }): void {
    validateChannel(channel); this.send({ type: "channel.publish", channel, audience, data });
  }
  updatePresence(channel: string, data: Record<string, JSONValue>): void {
    validateChannel(channel); this.send({ type: "presence.update", channel, data });
  }
  close(): void { this.socket?.close(1000, "client close"); this.socket = undefined; }

  private channelsFrame(type: "subscribe" | "unsubscribe", channels: string[]): void {
    if (!channels.length || channels.length > 100 || new Set(channels).size !== channels.length) throw new TypeError("invalid realtime channels");
    channels.forEach(validateChannel); this.send({ type, channels });
  }
  private send(frame: object): void {
    if (this.socket?.readyState !== 1) throw new RelayHubError("Realtime client is not connected.", { code: "not_connected" });
    this.socket.send(JSON.stringify(frame));
  }
  private decode(data: unknown): any | undefined {
    if (typeof data !== "string" || data.length > 65_536) return undefined;
    try { const frame = JSON.parse(data); return frame && typeof frame === "object" ? frame : undefined; } catch { return undefined; }
  }
  private dispatch(frame: any): void {
    if (frame.type === "channel.message" && typeof frame.channel === "string") void Promise.resolve(this.options.onMessage?.({ channel: frame.channel, data: frame.data ?? {}, messageId: frame.message_id ?? "", publishedAt: frame.published_at ?? "", publisherClientId: frame.publisher_client_id ?? "", publisherConnectionId: frame.publisher_connection_id ?? "", audience: frame.audience ?? { type: "all" } })).catch((error) => this.report(error));
    else if ((frame.type === "presence.join" || frame.type === "presence.update" || frame.type === "presence.leave" || frame.type === "presence.timeout") && typeof frame.channel === "string") void Promise.resolve(this.options.onPresence?.({ type: frame.type, channel: frame.channel, data: frame.data, clientId: frame.publisher_client_id ?? "", connectionId: frame.publisher_connection_id ?? "", occupancy: frame.occupancy ?? 0 })).catch((error) => this.report(error));
    else if (frame.type === "error") this.report(new RelayHubError(frame.message ?? "RelayHub realtime error.", { code: frame.code ?? "socket_error" }));
  }
  private report(error: unknown): void { this.options.onError?.(error instanceof RelayHubError ? error : new RelayHubError(error instanceof Error ? error.message : "Realtime handler failed.", { code: "handler_error" })); }
}

function validateChannel(channel: string): void { if (!/^[a-z0-9][a-z0-9_.:-]{0,95}$/.test(channel)) throw new TypeError("invalid realtime channel"); }
function validateClientId(clientId: string): void { if (!/^[A-Za-z0-9_.:-]{1,128}$/.test(clientId)) throw new TypeError("invalid realtime client ID"); }
