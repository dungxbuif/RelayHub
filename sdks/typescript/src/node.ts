import WebSocket from "ws";
import { RelayHubError, RetryDelivery } from "./errors.js";
import { BearerHTTPClient, SignedHTTPClient } from "./http/client.js";
import { canonicalRequest, signRequest } from "./http/signing.js";
import { LegacyClient } from "./legacy/client.js";
import { RelayHubStreamClient } from "./stream/client.js";
import type { App, AppCredentials, ChannelHandler, CreateAppInput, EventHandler, EventInput, EventObserver, FunctionHandler, FunctionRegistration, JSONValue, Publication, RPCResult, RoutingRule, RoutingRuleInput, SocketFactory, Subscription, TokenProvider } from "./types.js";

export interface RelayHubClientOptions {
  baseUrl: string;
  apiKey: string;
  hmacSecret: string;
  adminToken?: string;
  fetch?: typeof fetch;
  socketFactory?: SocketFactory;
  tokenProvider?: TokenProvider;
  now?: () => number;
  random?: () => number;
  sleep?: (milliseconds: number) => Promise<void>;
  onError?: (error: RelayHubError) => void;
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
  private readonly stream: RelayHubStreamClient;
  private readonly legacy: LegacyClient;

  constructor(options: RelayHubClientOptions) {
    if (!options.apiKey || !options.hmacSecret) throw new TypeError("apiKey and hmacSecret are required by the Node entry point");
    const http = new SignedHTTPClient({ baseUrl: options.baseUrl, apiKey: options.apiKey, hmacSecret: options.hmacSecret, fetch: options.fetch ?? globalThis.fetch, now: options.now ?? Date.now });
    const admin = options.adminToken ? new BearerHTTPClient({ baseUrl: options.baseUrl, token: options.adminToken, fetch: options.fetch ?? globalThis.fetch }) : undefined;
    const requireAdmin = () => { if (!admin) throw new TypeError("adminToken is required for app and routing management"); return admin; };
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
  }

  async close(options: { drain?: boolean; timeoutMs?: number } = {}): Promise<void> {
    await Promise.all([this.stream.close(options), this.legacy.close()]);
  }
}

export { RelayHubError, RetryDelivery, RelayHubStreamClient, canonicalRequest, signRequest };
export type * from "./types.js";
