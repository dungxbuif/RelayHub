export type JSONValue = unknown;

export interface QueuePublishOptions { available_at?: string; delay_seconds?: number; ordering_key?: string; priority?: number; deduplication_key?: string; metadata?: Record<string, JSONValue> }
export interface EventInput { type: string; target_app_ids?: string[]; data: Record<string, JSONValue>; queue?: QueuePublishOptions }
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
export interface QueueSubscriptionInput { name: string; enabled?: boolean; event_types?: string[]; max_attempts?: number; default_visibility_seconds?: number; max_visibility_seconds?: number; max_total_lease_seconds?: number; retention_seconds?: number; max_in_flight?: number; max_batch_size?: number; retry_delay_seconds?: number; ordering_mode?: "none" | "key"; deduplication_seconds?: number; max_dispatch_rate?: number | null }
export interface QueueSubscription extends Required<Omit<QueueSubscriptionInput, "max_dispatch_rate">> { id: string; app_id: string; paused_at?: string; max_dispatch_rate?: number; policy_version: number; created_at: string; updated_at: string }
export interface QueueDelivery { id: string; subscription_id: string; event: RelayEvent; receipt: string; attempt: number; generation: number; lease_expires_at: string; ordering_key?: string; priority: number; metadata?: Record<string, JSONValue> }
export type QueueDisposition = "ack" | "retry" | "dead_letter";
export interface QueueSettlement { receipt: string; disposition: QueueDisposition; delay_seconds?: number; reason?: string }
export interface QueueExtendItem { receipt: string; extension_seconds: number }
export interface QueueSettlementResult { receipt: string; status: "acked" | "available" | "dead_letter" | "invalid_receipt" | "extended" }
export interface QueueDepth { available: number; in_flight: number; acknowledged: number; dead_letter: number; oldest_available_at?: string }
export interface QueueDeadLetter { delivery_id: string; subscription_id: string; event_id: string; attempts: number; generation: number; reason?: string; updated_at: string }
export type QueueHandler = (delivery: QueueDelivery) => void | Promise<void>;
export type RealtimeAction = "subscribe" | "publish" | "presence" | "history" | "annotate" | "file.publish" | "push.manage";
export type RealtimeAudience = { type: "all" | "others" } | { type: "connection"; connection_id: string } | { type: "client"; client_id: string };
export interface RealtimeTokenRequest { clientId: string; channels: Record<string, RealtimeAction[]>; ttlSeconds?: number }
export type RealtimeTokenProvider = (request: RealtimeTokenRequest) => Promise<string>;
export interface RealtimeEncryptionEnvelope { algorithm: "aes-256-gcm"; key_id: string; nonce: string; ciphertext: string }
export interface RealtimeEncryptionKeyProvider {
  encryptionKey(channel: string): Promise<{ keyId: string; key: Uint8Array }>;
  decryptionKey(channel: string, keyId: string): Promise<Uint8Array>;
}
export interface RealtimeMessage { channel: string; data: Record<string, JSONValue>; encryption?: RealtimeEncryptionEnvelope; file?: RealtimeFile; messageId: string; publishedAt: string; publisherClientId: string; publisherConnectionId: string; audience: RealtimeAudience }
export interface RealtimeHistoryOptions { limit: number; cursor?: string }
export interface RealtimeHistoryMessage extends RealtimeMessage { cursor: string }
export interface RealtimeHistoryResult { channel: string; items: RealtimeHistoryMessage[]; nextCursor?: string; continuityCursor?: string }
export type RealtimePublishItem = { id: string; channel: string; data: Record<string, JSONValue>; encryption?: never; audience?: RealtimeAudience } | { id: string; channel: string; data?: never; encryption: RealtimeEncryptionEnvelope; audience?: RealtimeAudience };
export interface RealtimePublishOutcome { id: string; accepted: boolean; messageId?: string; code?: string }
export interface RealtimeBatchResult { outcomes: RealtimePublishOutcome[] }
export interface RealtimeMessageAction { id: string; channel: string; message_id: string; client_id: string; type: "reaction" | "annotation"; idempotency_key: string; data: Record<string, JSONValue>; created_at: string; removed_at?: string }
export interface RealtimeFileInput { channel: string; name: string; mime_type: string; size_bytes: number; sha256: string }
export interface RealtimeFile extends RealtimeFileInput { id: string; app_id: string; status: "pending" | "ready" | "quarantined"; created_at: string; expires_at: string; completed_at?: string }
export interface RealtimeFileUpload { file: RealtimeFile; upload_url: string; required_headers: Record<string, string> }
export interface RealtimeFileDownload { file: RealtimeFile; download_url: string }
export interface PushDevice { id: string; app_id: string; provider: "apns" | "fcm" }
export interface PushNotification { title?: string; body?: string; data?: Record<string, JSONValue> }
export interface PushOutcome { id: string; device_id: string; channel: string; provider: "apns" | "fcm"; status: "delivered" | "failed"; provider_message_id?: string; reason?: string }
export interface PresenceMessage { type: "presence.join" | "presence.update" | "presence.leave" | "presence.timeout"; channel: string; data?: Record<string, JSONValue>; clientId: string; connectionId: string; occupancy: number }

export interface SocketLike {
  readonly protocol: string;
  readonly readyState: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: "open" | "message" | "close" | "error", listener: (event: any) => void, options?: { once?: boolean }): void;
}
export type SocketFactory = (url: string, protocols?: string | string[]) => SocketLike;
export type TokenProvider = (scopes: string[]) => Promise<string>;
