export type JSONValue = unknown;

export interface EventInput { type: string; target_app_ids?: string[]; data: Record<string, JSONValue> }
export interface RelayEvent extends EventInput { id: string; source_app_id: string; created_at: string }
export interface Publication { event: RelayEvent; jobs: Job[] }
export interface Job { id: string; event_id: string; source_app_id: string; target_app_id: string; status: string; attempts: number; created_at: string; updated_at: string }
export interface AppCredentials { app_id: string; api_key: string; hmac_secret: string }
export interface App { id: string; name: string; callback_url: string | null; delivery_mode: "websocket" | "callback" | "all" | "queue"; enabled: boolean; created_at: string; updated_at: string }
export interface CreateAppInput { name: string; callback_url?: string | null; delivery_mode: "websocket" | "callback" | "all" | "queue" }
export interface RoutingRuleInput { source_app_id?: string | null; event_type: string; target_app_id: string; realtime_channel?: string | null; enabled?: boolean }
export interface RoutingRule { id: string; source_app_id: string | null; event_type: string; target_app_id: string; realtime_channel: string | null; enabled: boolean; created_at: string; updated_at: string }
export type ChannelHandler = (message: { channel: string; publisherAppId: string; data: Record<string, JSONValue> }) => void | Promise<void>;
export interface DeliveryContext { deliveryId: string; eventId: string; attempt: number }
export type EventHandler = (event: RelayEvent, context: DeliveryContext) => void | Promise<void>;
export type EventObserver = (event: RelayEvent) => void | Promise<void>;
export type FunctionHandler = (input: Record<string, JSONValue>, context: { invocationId: string; deadline: string }) => JSONValue | Promise<JSONValue>;
export interface FunctionRegistration { id: string; app_id: string; name: string; timeout_seconds: number; enabled: boolean; created_at: string; updated_at: string }
export interface RPCResult { invocation_id: string; ok: boolean; result?: JSONValue; error?: { code: string; message: string } }
export interface ConsumerHandle { drain(options?: { timeoutMs?: number }): Promise<void> }
export interface Subscription { close(): Promise<void> }

export interface SocketLike {
  readonly protocol: string;
  readonly readyState: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: "open" | "message" | "close" | "error", listener: (event: any) => void, options?: { once?: boolean }): void;
}
export type SocketFactory = (url: string, protocols?: string | string[]) => SocketLike;
export type TokenProvider = (scopes: string[]) => Promise<string>;
