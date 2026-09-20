import WebSocket from "ws";
import { timingSafeEqual } from "node:crypto";
import { DeadLetterDelivery, RelayHubError, RetryDelivery } from "./errors.js";
import { AdminSessionHTTPClient, SignedHTTPClient } from "./http/client.js";
import { canonicalRequest, signRequest } from "./http/signing.js";
import { LegacyClient } from "./legacy/client.js";
import { RelayHubStreamClient } from "./stream/client.js";
import { RelayHubRealtimeClient } from "./realtime/client.js";
import { decryptRealtimeEnvelope, encryptRealtimePayload } from "./realtime/crypto.js";
import { RelayHubQueueWorker } from "./queue/worker.js";
import type { App, AppCredentials, ChannelHandler, CreateAppInput, EventHandler, EventInput, EventObserver, FunctionHandler, FunctionRegistration, JSONValue, Publication, PushDevice, PushNotification, PushOutcome, QueueDeadLetter, QueueDelivery, QueueDepth, QueueDrain, QueueExtendItem, QueueHandler, QueueSchedule, QueueScheduleInput, QueueSettlement, QueueSettlementResult, QueueSubscription, QueueSubscriptionInput, RealtimeFile, RealtimeFileDownload, RealtimeFileInput, RealtimeFileUpload, RPCResult, RoutingRule, RoutingRuleInput, SocketFactory, Subscription, TokenProvider } from "./types.js";

export interface RelayHubClientOptions {
  baseUrl: string;
  apiKey: string;
  hmacSecret: string;
  adminSession?: { cookie: string; csrfToken: string };
  fetch?: typeof fetch;
  socketFactory?: SocketFactory;
  tokenProvider?: TokenProvider;
  now?: () => number;
  random?: () => number;
  sleep?: (milliseconds: number) => Promise<void>;
  onError?: (error: RelayHubError) => void;
}

export function verifyCallbackSignature(input: { secret: string; timestamp: string; requestTarget: string; body: Uint8Array; signature: string; now?: number; maxSkewSeconds?: number }): boolean {
  if (!/^\d+$/.test(input.timestamp) || !/^[a-f0-9]{64}$/.test(input.signature)) return false;
  const at = Number(input.timestamp);
  const now = Math.floor((input.now ?? Date.now()) / 1000);
  const maxSkew = input.maxSkewSeconds ?? 300;
  if (!Number.isSafeInteger(at) || maxSkew <= 0 || Math.abs(now - at) > maxSkew) return false;
  const expected = signRequest({ secret: input.secret, timestamp: input.timestamp, method: "POST", requestTarget: input.requestTarget, body: input.body });
  return timingSafeEqual(Buffer.from(expected, "hex"), Buffer.from(input.signature, "hex"));
}

export class RelayHubClient {
  readonly events: {
    publish: (input: EventInput, options: { idempotencyKey: string }) => Promise<Publication>;
    consume: (handler: EventHandler, options?: { concurrency?: number }) => ReturnType<RelayHubStreamClient["consume"]>;
    observe: (handler: EventObserver) => Subscription;
  };
  readonly realtime: {
    publish: (channel: string, data: Record<string, JSONValue>) => Promise<void>;
    subscribe: (channel: string, handler: ChannelHandler) => Subscription;
    createFile: (input: RealtimeFileInput) => Promise<RealtimeFileUpload>;
    completeFile: (id: string) => Promise<RealtimeFile>;
    fileDownload: (id: string) => Promise<RealtimeFileDownload>;
    registerPushDevice: (provider: "apns" | "fcm", token: string) => Promise<PushDevice>;
    deletePushDevice: (id: string) => Promise<void>;
    bindPushDevice: (channel: string, id: string, bind?: boolean) => Promise<void>;
    publishPush: (channel: string, notification: PushNotification) => Promise<{outcomes: PushOutcome[]}>;
  };
  readonly apps: {
    create: (input: CreateAppInput) => Promise<AppCredentials>;
    list: () => Promise<App[]>;
  };
  readonly routing: {
    createRule: (input: RoutingRuleInput) => Promise<RoutingRule>;
    listRules: () => Promise<RoutingRule[]>;
    updateRule: (id: string, input: Partial<RoutingRuleInput>) => Promise<RoutingRule>;
    deleteRule: (id: string) => Promise<void>;
  };
  readonly functions: {
    register: (name: string, options: { timeoutSeconds: number; enabled?: boolean }) => Promise<FunctionRegistration>;
    list: () => Promise<FunctionRegistration[]>;
    delete: (id: string) => Promise<void>;
    invoke: (id: string, input: Record<string, JSONValue>, options: { idempotencyKey: string }) => Promise<RPCResult>;
    handle: (name: string, handler: FunctionHandler) => Subscription;
  };
  readonly queue: {
    create: (input: QueueSubscriptionInput) => Promise<QueueSubscription>;
    list: () => Promise<QueueSubscription[]>;
    get: (id: string) => Promise<QueueSubscription>;
    update: (id: string, policyVersion: number, input: QueueSubscriptionInput) => Promise<QueueSubscription>;
    delete: (id: string) => Promise<void>;
    pause: (id: string) => Promise<QueueSubscription>;
    resume: (id: string) => Promise<QueueSubscription>;
    pull: (id: string, input: { max_messages: number; wait_seconds?: number; visibility_seconds?: number }) => Promise<{ items: QueueDelivery[] }>;
    settle: (id: string, items: QueueSettlement[]) => Promise<{ items: QueueSettlementResult[] }>;
    extend: (id: string, items: QueueExtendItem[]) => Promise<{ items: QueueSettlementResult[] }>;
    metrics: (id: string) => Promise<QueueDepth>;
    drain: (id: string, timeoutSeconds?: number) => Promise<QueueDrain>;
    drainStatus: (id: string) => Promise<QueueDrain>;
    createSchedule: (id: string, input: QueueScheduleInput) => Promise<QueueSchedule>;
    listSchedules: (id: string) => Promise<QueueSchedule[]>;
    updateSchedule: (id: string, scheduleId: string, policyVersion: number, input: QueueScheduleInput) => Promise<QueueSchedule>;
    deleteSchedule: (id: string, scheduleId: string) => Promise<void>;
    deadLetters: (id: string, options?: { limit?: number; cursor?: string }) => Promise<{ items: QueueDeadLetter[]; next_cursor: string }>;
    replayDeadLetters: (id: string, deliveryIds: string[]) => Promise<{ replayed: number }>;
    deleteDeadLetters: (id: string, deliveryIds: string[]) => Promise<{ deleted: number }>;
    exportDeadLetters: (id: string, options?: {limit?: number; cursor?: string}) => Promise<{items: QueueDeadLetter[]}>;
    work: (id: string, handler: QueueHandler, options?: ConstructorParameters<typeof RelayHubQueueWorker>[3]) => RelayHubQueueWorker;
  };
  private readonly stream: RelayHubStreamClient;
  private readonly legacy: LegacyClient;

  constructor(options: RelayHubClientOptions) {
    if (!options.apiKey || !options.hmacSecret) throw new TypeError("apiKey and hmacSecret are required by the Node entry point");
    const http = new SignedHTTPClient({ baseUrl: options.baseUrl, apiKey: options.apiKey, hmacSecret: options.hmacSecret, fetch: options.fetch ?? globalThis.fetch, now: options.now ?? Date.now });
    const admin = options.adminSession ? new AdminSessionHTTPClient({ baseUrl: options.baseUrl, ...options.adminSession, fetch: options.fetch ?? globalThis.fetch }) : undefined;
    const requireAdmin = () => { if (!admin) throw new TypeError("adminSession is required for app and routing management"); return admin; };
    const tokenProvider = options.tokenProvider ?? (async (scopes) => {
      const response = await http.request<{ token: string }>("POST", "/api/v1/socket/token", { body: { scopes, ttl_seconds: 600 } });
      return response.token;
    });
    const socketFactory = options.socketFactory ?? ((url, protocols) => new WebSocket(url, protocols) as unknown as ReturnType<SocketFactory>);
    const streamOptions: ConstructorParameters<typeof RelayHubStreamClient>[0] = { baseUrl: options.baseUrl, tokenProvider, socketFactory };
    if (options.random) streamOptions.random = options.random;
    if (options.sleep) streamOptions.sleep = options.sleep;
    if (options.onError) streamOptions.onError = options.onError;
    this.stream = new RelayHubStreamClient(streamOptions);
    const legacyOptions: ConstructorParameters<typeof LegacyClient>[0] = { baseUrl: options.baseUrl, tokenProvider, socketFactory };
    if (options.random) legacyOptions.random = options.random;
    if (options.sleep) legacyOptions.sleep = options.sleep;
    if (options.onError) legacyOptions.onError = options.onError;
    this.legacy = new LegacyClient(legacyOptions);
    this.events = {
      publish: (input, publishOptions) => http.request("POST", "/api/v1/events", { body: input, idempotencyKey: publishOptions.idempotencyKey }),
      consume: (handler, consumeOptions) => this.stream.consume(handler, consumeOptions),
      observe: (handler) => this.legacy.observe(handler),
    };
    this.realtime = {
      publish: (channel, data) => http.request("POST", `/api/v1/realtime/channels/${encodeURIComponent(channel)}/publish`, { body: { data } }),
      subscribe: (channel, handler) => this.legacy.subscribeChannel(channel, handler),
      createFile: (input) => http.request("POST", "/api/v2/realtime/files", {body: input}),
      completeFile: (id) => http.request("POST", `/api/v2/realtime/files/${encodeURIComponent(id)}/complete`),
      fileDownload: (id) => http.request("GET", `/api/v2/realtime/files/${encodeURIComponent(id)}/download`),
      registerPushDevice: (provider, token) => http.request("POST", "/api/v2/realtime/push/devices", {body: {provider, token}}),
      deletePushDevice: (id) => http.request("DELETE", `/api/v2/realtime/push/devices/${encodeURIComponent(id)}`),
      bindPushDevice: (channel, id, bind = true) => http.request(bind ? "PUT" : "DELETE", `/api/v2/realtime/push/channels/${encodeURIComponent(channel)}/devices/${encodeURIComponent(id)}`),
      publishPush: (channel, notification) => http.request("POST", `/api/v2/realtime/push/channels/${encodeURIComponent(channel)}/notifications`, {body: notification}),
    };
    this.apps = {
      create: (input) => requireAdmin().request("POST", "/api/v1/apps", { body: input }),
      list: () => requireAdmin().request("GET", "/api/v1/apps"),
    };
    this.routing = {
      createRule: (input) => requireAdmin().request("POST", "/api/v1/routing/rules", { body: input }),
      listRules: () => requireAdmin().request("GET", "/api/v1/routing/rules"),
      updateRule: (id, input) => requireAdmin().request("PATCH", `/api/v1/routing/rules/${encodeURIComponent(id)}`, { body: input }),
      deleteRule: (id) => requireAdmin().request("DELETE", `/api/v1/routing/rules/${encodeURIComponent(id)}`),
    };
    this.functions = {
      register: (name, registerOptions) => http.request("POST", "/api/v1/functions", { body: { name, timeout_seconds: registerOptions.timeoutSeconds, ...(registerOptions.enabled === undefined ? {} : { enabled: registerOptions.enabled }) } }),
      list: () => http.request("GET", "/api/v1/functions"),
      delete: (id) => http.request("DELETE", `/api/v1/functions/${encodeURIComponent(id)}`),
      invoke: (id, input, invokeOptions) => http.request("POST", `/api/v1/functions/${encodeURIComponent(id)}/invoke`, { body: { input }, idempotencyKey: invokeOptions.idempotencyKey }),
      handle: (name, handler) => this.legacy.handle(name, handler),
    };
    const subscriptionPath = (id: string) => `/api/v2/subscriptions/${encodeURIComponent(id)}`;
    const queueTransport = {
      pull: (id: string, input: { max_messages: number; wait_seconds: number; visibility_seconds: number }) => http.request<{ items: QueueDelivery[] }>("POST", `${subscriptionPath(id)}/pull`, { body: input }),
      settle: (id: string, items: QueueSettlement[]) => http.request<{ items: QueueSettlementResult[] }>("POST", `${subscriptionPath(id)}/settle`, { body: { items } }),
      extend: (id: string, items: QueueExtendItem[]) => http.request<{ items: QueueSettlementResult[] }>("POST", `${subscriptionPath(id)}/leases/extend`, { body: { items } }),
    };
    this.queue = {
      create: (input) => http.request("POST", "/api/v2/subscriptions", { body: input }),
      list: async () => (await http.request<{ items: QueueSubscription[] }>("GET", "/api/v2/subscriptions")).items,
      get: (id) => http.request("GET", subscriptionPath(id)),
      update: (id, policyVersion, input) => http.request("PUT", subscriptionPath(id), { body: { ...input, policy_version: policyVersion } }),
      delete: (id) => http.request("DELETE", subscriptionPath(id)),
      pause: (id) => http.request("POST", `${subscriptionPath(id)}/pause`),
      resume: (id) => http.request("POST", `${subscriptionPath(id)}/resume`),
      pull: (id, input) => http.request("POST", `${subscriptionPath(id)}/pull`, { body: input }),
      settle: queueTransport.settle,
      extend: queueTransport.extend,
      metrics: (id) => http.request("GET", `${subscriptionPath(id)}/metrics`),
      drain: (id, timeoutSeconds = 30) => http.request("POST", `${subscriptionPath(id)}/drain`, {body: {timeout_seconds: timeoutSeconds}}),
      drainStatus: (id) => http.request("GET", `${subscriptionPath(id)}/drain`),
      createSchedule: (id, input) => http.request("POST", `${subscriptionPath(id)}/schedules`, {body: input}),
      listSchedules: async (id) => (await http.request<{items: QueueSchedule[]}>("GET", `${subscriptionPath(id)}/schedules`)).items,
      updateSchedule: (id, scheduleId, policyVersion, input) => http.request("PUT", `${subscriptionPath(id)}/schedules/${encodeURIComponent(scheduleId)}`, {body: {...input, policy_version: policyVersion}}),
      deleteSchedule: (id, scheduleId) => http.request("DELETE", `${subscriptionPath(id)}/schedules/${encodeURIComponent(scheduleId)}`),
      deadLetters: (id, deadLetterOptions = {}) => {
        const query = new URLSearchParams();
        if (deadLetterOptions.limit !== undefined) query.set("limit", String(deadLetterOptions.limit));
        if (deadLetterOptions.cursor) query.set("cursor", deadLetterOptions.cursor);
        const suffix = query.size ? `?${query}` : "";
        return http.request("GET", `${subscriptionPath(id)}/dead-letters${suffix}`);
      },
      replayDeadLetters: (id, deliveryIds) => http.request("POST", `${subscriptionPath(id)}/dead-letters/replay`, { body: { delivery_ids: deliveryIds } }),
      deleteDeadLetters: (id, deliveryIds) => http.request("POST", `${subscriptionPath(id)}/dead-letters/delete`, { body: { delivery_ids: deliveryIds } }),
      exportDeadLetters: (id, options = {}) => { const query = new URLSearchParams({format: "json"}); if (options.limit !== undefined) query.set("limit", String(options.limit)); if (options.cursor) query.set("cursor", options.cursor); return http.request("GET", `${subscriptionPath(id)}/dead-letters/export?${query}`); },
      work: (id, handler, workerOptions) => new RelayHubQueueWorker(queueTransport, id, handler, workerOptions),
    };
  }

  async close(options: { drain?: boolean; timeoutMs?: number } = {}): Promise<void> {
    await Promise.all([this.stream.close(options), this.legacy.close()]);
  }
}

export { DeadLetterDelivery, RelayHubError, RelayHubQueueWorker, RetryDelivery, RelayHubRealtimeClient, RelayHubStreamClient, canonicalRequest, decryptRealtimeEnvelope, encryptRealtimePayload, signRequest };
export type { QueueWorkerOptions } from "./queue/worker.js";
export type { RealtimeClientOptions } from "./realtime/client.js";
export type * from "./types.js";
