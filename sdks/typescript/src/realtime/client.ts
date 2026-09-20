import { RelayHubError } from "../errors.js";
import { decryptRealtimeEnvelope, encryptRealtimePayload, validateRealtimeEncryptionEnvelope } from "./crypto.js";
import type { JSONValue, PresenceMessage, RealtimeAction, RealtimeAudience, RealtimeBatchResult, RealtimeEncryptionEnvelope, RealtimeEncryptionKeyProvider, RealtimeHistoryOptions, RealtimeHistoryResult, RealtimeMessage, RealtimeMessageAction, RealtimePublishItem, RealtimeTokenProvider, SocketFactory, SocketLike } from "../types.js";

export interface RealtimeClientOptions {
  baseUrl: string;
  clientId: string;
  channels: Record<string, RealtimeAction[]>;
  tokenProvider: RealtimeTokenProvider;
  socketFactory: SocketFactory;
  encryptionKeyProvider?: RealtimeEncryptionKeyProvider;
  onMessage?: (message: RealtimeMessage) => void | Promise<void>;
  onPresence?: (presence: PresenceMessage) => void | Promise<void>;
  onHistory?: (history: RealtimeHistoryResult) => void | Promise<void>;
  onBatchResult?: (result: RealtimeBatchResult) => void | Promise<void>;
  onAction?: (action: RealtimeMessageAction) => void | Promise<void>;
  onActions?: (actions: RealtimeMessageAction[]) => void | Promise<void>;
  onError?: (error: RelayHubError) => void;
}

export class RelayHubRealtimeClient {
  private socket: SocketLike | undefined;
  constructor(private readonly options: RealtimeClientOptions) {
    validateClientId(options.clientId);
    for (const [channel, actions] of Object.entries(options.channels)) {
      validateChannelGrant(channel);
      if (!actions.length || new Set(actions).size !== actions.length || actions.some(action => !realtimeActions.has(action))) throw new TypeError("invalid realtime actions");
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

  subscribe(channels: string[], rewind?: RealtimeHistoryOptions): void {
    this.validateChannels(channels);
    if (rewind) {
      if (channels.length > 10) throw new TypeError("invalid realtime rewind channels");
      validateHistoryOptions(rewind);
    }
    this.send({ type: "subscribe", channels, ...(rewind ? {rewind} : {}) });
  }
  unsubscribe(channels: string[]): void { this.channelsFrame("unsubscribe", channels); }
  publish(channel: string, data: Record<string, JSONValue>, audience: RealtimeAudience = { type: "all" }): void {
    validateChannel(channel); this.send({ type: "channel.publish", channel, audience, data });
  }
  async publishEncrypted(channel: string, data: Record<string, JSONValue>, audience: RealtimeAudience = {type: "all"}): Promise<void> {
    if (!channel.startsWith("private:") || channel.length <= 8) throw new TypeError("encryption requires a private realtime channel");
    const provider = this.options.encryptionKeyProvider;
    if (!provider) throw new RelayHubError("Realtime encryption key provider is not configured.", {code: "encryption_key_unavailable"});
    const encryption = await encryptRealtimePayload(provider, channel, data);
    this.send({type: "channel.publish", channel, audience, encryption});
  }
  updatePresence(channel: string, data: Record<string, JSONValue>): void {
    validateChannel(channel); this.send({ type: "presence.update", channel, data });
  }
  publishFile(channel: string, fileId: string, audience: RealtimeAudience = {type:"all"}): void {
    validateChannel(channel); if (!/^file_[A-Za-z0-9_.:-]{1,123}$/.test(fileId)) throw new TypeError("invalid realtime file ID");
    this.send({type:"file.publish", channel, file_id:fileId, audience});
  }
  putMessageAction(channel: string, messageId: string, actionType: "reaction" | "annotation", idempotencyKey: string, data: Record<string, JSONValue>): void {
    if (!validMessageReference(channel, messageId) || !/^[A-Za-z0-9_.:-]{1,128}$/.test(idempotencyKey) || (actionType !== "reaction" && actionType !== "annotation") || !data || typeof data !== "object" || Array.isArray(data)) throw new TypeError("invalid realtime message action");
    this.send({type: "message.action.put", channel, message_id: messageId, action_type: actionType, idempotency_key: idempotencyKey, data});
  }
  listMessageActions(channel: string, messageId: string): void {
    if (!validMessageReference(channel, messageId)) throw new TypeError("invalid realtime message action");
    this.send({type: "message.actions.get", channel, message_id: messageId});
  }
  removeMessageAction(channel: string, messageId: string, actionId: string): void {
    if (!validMessageReference(channel, messageId) || !/^[A-Za-z0-9_.:-]{1,128}$/.test(actionId)) throw new TypeError("invalid realtime message action");
    this.send({type: "message.action.remove", channel, message_id: messageId, action_id: actionId});
  }
  history(channel: string, options: RealtimeHistoryOptions): void {
    validateChannel(channel); validateHistoryOptions(options); this.send({type: "history.get", channel, ...options});
  }
  publishBatch(items: RealtimePublishItem[]): void {
    if (!items.length || items.length > 50) throw new TypeError("invalid realtime publish batch");
    const ids = new Set<string>();
    let encrypted: boolean | undefined;
    for (const item of items) {
      if (!/^[A-Za-z0-9_.:-]{1,128}$/.test(item.id) || ids.has(item.id)) throw new TypeError("invalid realtime publish batch");
      validateChannel(item.channel);
      const itemEncrypted = item.encryption !== undefined;
      if (encrypted !== undefined && encrypted !== itemEncrypted) throw new TypeError("invalid realtime publish batch");
      encrypted = itemEncrypted;
      if (itemEncrypted) validateRealtimeEncryptionEnvelope(item.channel, item.encryption);
      else if (!item.data || typeof item.data !== "object" || Array.isArray(item.data)) throw new TypeError("invalid realtime publish batch");
      ids.add(item.id);
    }
    this.send({type: "channel.publish.batch", items});
  }
  close(): void { this.socket?.close(1000, "client close"); this.socket = undefined; }

  private channelsFrame(type: "subscribe" | "unsubscribe", channels: string[]): void {
	this.validateChannels(channels); this.send({ type, channels });
  }
  private validateChannels(channels: string[]): void {
    if (!channels.length || channels.length > 100 || new Set(channels).size !== channels.length) throw new TypeError("invalid realtime channels");
    channels.forEach(validateChannel);
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
    if (frame.type === "channel.message" && typeof frame.channel === "string") void this.message(frame).then(message => this.options.onMessage?.(message)).catch((error) => this.report(error));
    else if ((frame.type === "presence.join" || frame.type === "presence.update" || frame.type === "presence.leave" || frame.type === "presence.timeout") && typeof frame.channel === "string") void Promise.resolve(this.options.onPresence?.({ type: frame.type, channel: frame.channel, data: frame.data, clientId: frame.publisher_client_id ?? "", connectionId: frame.publisher_connection_id ?? "", occupancy: frame.occupancy ?? 0 })).catch((error) => this.report(error));
    else if (frame.type === "history.result" && typeof frame.channel === "string" && Array.isArray(frame.items)) void Promise.resolve(this.options.onHistory?.({channel: frame.channel, items: frame.items.map((item: any) => ({...toMessage(item), cursor: item.cursor ?? ""})), nextCursor: frame.next_cursor, continuityCursor: frame.continuity_cursor})).catch((error) => this.report(error));
    else if (frame.type === "channel.publish.batch.result" && Array.isArray(frame.outcomes)) void Promise.resolve(this.options.onBatchResult?.({outcomes: frame.outcomes.map((item: any) => ({id: item.id ?? "", accepted: item.accepted === true, messageId: item.message_id, code: item.code}))})).catch((error) => this.report(error));
    else if ((frame.type === "message.action.updated" || frame.type === "message.action.removed") && validMessageAction(frame.action)) void Promise.resolve(this.options.onAction?.(frame.action)).catch((error) => this.report(error));
    else if (frame.type === "message.actions.result" && Array.isArray(frame.actions) && frame.actions.every(validMessageAction)) void Promise.resolve(this.options.onActions?.(frame.actions)).catch((error) => this.report(error));
    else if (frame.type === "error") this.report(new RelayHubError(frame.message ?? "RelayHub realtime error.", { code: frame.code ?? "socket_error" }));
  }
  private async message(frame: any): Promise<RealtimeMessage> {
    let data = frame.data ?? {};
    let encryption: RealtimeEncryptionEnvelope | undefined;
    if (frame.encryption !== undefined) {
      if (!this.options.encryptionKeyProvider) throw new RelayHubError("Encrypted realtime message requires a key provider.", {code: "encryption_key_unavailable"});
      encryption = frame.encryption as RealtimeEncryptionEnvelope;
      data = await decryptRealtimeEnvelope(this.options.encryptionKeyProvider, frame.channel, encryption);
    }
    return {channel: frame.channel, data, ...(encryption ? {encryption} : {}), ...(frame.file ? {file: frame.file} : {}), messageId: frame.message_id ?? "", publishedAt: frame.published_at ?? "", publisherClientId: frame.publisher_client_id ?? "", publisherConnectionId: frame.publisher_connection_id ?? "", audience: frame.audience ?? {type: "all"}};
  }
  private report(error: unknown): void { this.options.onError?.(error instanceof RelayHubError ? error : new RelayHubError(error instanceof Error ? error.message : "Realtime handler failed.", { code: "handler_error" })); }
}

function validateChannel(channel: string): void { if (!/^[a-z0-9][a-z0-9_.:-]{0,95}$/.test(channel)) throw new TypeError("invalid realtime channel"); }
function validateChannelGrant(channel: string): void {
  if (/^[a-z0-9][a-z0-9_.:-]{0,95}$/.test(channel) && channel.split(":").every(Boolean)) return;
  const prefix = channel.endsWith(":*") ? channel.slice(0, -2) : "";
  if (!prefix || channel.split("*").length !== 2 || !/^[a-z0-9][a-z0-9_.:-]{0,95}$/.test(prefix) || !prefix.split(":").every(Boolean)) throw new TypeError("invalid realtime channel grant");
}
function validateHistoryOptions(options: RealtimeHistoryOptions): void {
  if (!Number.isInteger(options.limit) || options.limit < 1 || options.limit > 100 || (options.cursor !== undefined && !/^[A-Za-z0-9_-]{1,128}$/.test(options.cursor))) throw new TypeError("invalid realtime history options");
}
function toMessage(frame: any): RealtimeMessage {
  return {channel: frame.channel ?? "", data: frame.data ?? {}, messageId: frame.message_id ?? "", publishedAt: frame.published_at ?? "", publisherClientId: frame.publisher_client_id ?? "", publisherConnectionId: frame.publisher_connection_id ?? "", audience: frame.audience ?? {type: "all"}};
}
function validateClientId(clientId: string): void { if (!/^[A-Za-z0-9_.:-]{1,128}$/.test(clientId)) throw new TypeError("invalid realtime client ID"); }
function validMessageReference(channel: string, messageId: string): boolean { return /^[a-z0-9][a-z0-9_.:-]{0,95}$/.test(channel) && /^msg_[A-Za-z0-9_.:-]{1,124}$/.test(messageId); }
function validMessageAction(value: any): value is RealtimeMessageAction { return value && typeof value === "object" && /^[A-Za-z0-9_.:-]{1,128}$/.test(value.id ?? "") && validMessageReference(value.channel ?? "", value.message_id ?? "") && (value.type === "reaction" || value.type === "annotation"); }
const realtimeActions = new Set(["subscribe", "publish", "presence", "history", "annotate", "file.publish", "push.manage"]);
