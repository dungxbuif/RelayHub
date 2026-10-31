import type { AdminApiClient } from "./client";

export type MetricPoint = { at: string; values: Record<string, number> };
export type InstanceSummary = { instance_id: string; connections: number; nats_connected: boolean; nats_changed_at: string; heartbeat_at: string };
export type DurableCounts = {
  pending: number;
  retrying: number;
  dead_letter: number;
  oldest_pending_at?: string;
  delivery_latency_p50_ms?: number;
  delivery_latency_p95_ms?: number;
  delivery_latency_p99_ms?: number;
};
export type MetricsSnapshot = { generated_at: string; window_seconds: number; step_seconds: number; series: MetricPoint[]; instances: InstanceSummary[]; active_connections: number; degraded_components: string[] };
export type DashboardSnapshot = MetricsSnapshot & { durable: DurableCounts };
export type EventSummary = { id: string; type: string; source_app_id: string; target_count: number; delivery_count: number; created_at: string };
export type DeadLetterSummary = { delivery_id: string; job_id: string; event_id: string; source_app_id: string; target_app_id: string; sink: string; reason: string; attempts: number; created_at: string; updated_at: string };
export type EventDetail = EventSummary & { target_app_ids: string[]; data: Record<string, unknown> };
export type DeadLetterDetail = DeadLetterSummary & { event_type: string };
export type DeliveryLifecycle = { delivery_id: string; job_id: string; event_id: string; target_app_id: string; sink: string; status: string; reason?: string; generation: number; attempts: number; created_at: string; updated_at: string };
export type DeliveryAttempt = { delivery_id: string; generation: number; attempt: number; outcome: string; reason?: string; started_at: string; updated_at: string };
export type TimelineItem = { id: string; type: string; occurred_at: string; event_id: string; delivery_id?: string; job_id?: string; generation?: number; attempt?: number; outcome?: string; reason?: string; actor_type?: string; actor_id?: string };
export type EventTimeline = { event: EventDetail; deliveries: DeliveryLifecycle[]; attempts: DeliveryAttempt[]; items: TimelineItem[] };
export type ReplayItem = { delivery_id: string; event_id: string; job_id: string; from_generation: number; generation: number; status: string };
export type ReplayResult = { items: ReplayItem[] };
export type AuditSummary = { id: number; occurred_at: string; actor_type: string; actor_id?: string; action: string; resource_type: string; resource_id?: string; outcome: string; metadata?: Record<string, unknown> };
export type CursorPage<T> = { items: T[]; next_cursor?: string };

export function getDashboard(client: AdminApiClient, signal?: AbortSignal): Promise<DashboardSnapshot> {
  return client.request("/api/v1/admin/dashboard?window=15m&step=1m", { signal });
}
export function listEvents(client: AdminApiClient, query: URLSearchParams, signal?: AbortSignal): Promise<CursorPage<EventSummary>> {
  return client.request(`/api/v1/admin/events?${boundedQuery(query, ["limit", "cursor", "type", "source_app_id", "from", "to"])}`, { signal });
}
export function listDeadLetters(client: AdminApiClient, query: URLSearchParams, signal?: AbortSignal): Promise<CursorPage<DeadLetterSummary>> {
  return client.request(`/api/v1/admin/dlq?${boundedQuery(query, ["limit", "cursor", "source_app_id", "target_app_id", "sink", "reason", "from", "to"])}`, { signal });
}
export function listAudit(client: AdminApiClient, query: URLSearchParams, signal?: AbortSignal): Promise<CursorPage<AuditSummary>> {
  return client.request(`/api/v1/admin/audit?${boundedQuery(query, ["limit", "cursor", "actor_type", "action", "resource_type", "resource_id", "outcome", "from", "to"])}`, { signal });
}
export function getEventTimeline(client: AdminApiClient, eventID: string, signal?: AbortSignal): Promise<EventTimeline> {
  return client.request(`/api/v1/admin/events/${encodeURIComponent(eventID)}/timeline`, { signal });
}
export function getDeadLetter(client: AdminApiClient, deliveryID: string, signal?: AbortSignal): Promise<DeadLetterDetail> {
  return client.request(`/api/v1/admin/dlq/${encodeURIComponent(deliveryID)}`, { signal });
}
export function replayDeadLetters(client: AdminApiClient, deliveryIDs: string[], idempotencyKey: string): Promise<ReplayResult> {
  return client.request("/api/v1/admin/dlq/replay", { method: "POST", headers: { "Idempotency-Key": idempotencyKey }, body: { delivery_ids: deliveryIDs } });
}
export function replayDeadLetter(client: AdminApiClient, deliveryID: string, idempotencyKey: string): Promise<ReplayResult> {
  return client.request(`/api/v1/admin/dlq/${encodeURIComponent(deliveryID)}/replay`, { method: "POST", headers: { "Idempotency-Key": idempotencyKey } });
}
export function boundedQuery(source: URLSearchParams, allowed: string[]): string {
  const result = new URLSearchParams();
  for (const key of allowed) { const value = source.get(key)?.trim(); if (value) result.set(key, value.slice(0, 512)); }
  const limit = Number.parseInt(result.get("limit") ?? "25", 10);
  result.set("limit", Number.isFinite(limit) && limit > 0 && limit <= 100 ? String(limit) : "25");
  return result.toString();
}

const sensitivePayloadKey = /(?:token|secret|password|authorization|cookie|signature|api[_-]?key|credential|presigned|upload[_-]?url)/i;

export function safeEventPayload(value: unknown, depth = 0): unknown {
  if (depth > 5) return "[TRUNCATED]";
  if (typeof value === "string") return value.length > 2_048 ? `${value.slice(0, 2_048)}…[TRUNCATED]` : value;
  if (Array.isArray(value)) return value.slice(0, 100).map((item) => safeEventPayload(item, depth + 1));
  if (!value || typeof value !== "object") return value;
  const output: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(value).slice(0, 100)) output[key] = sensitivePayloadKey.test(key) ? "[REDACTED]" : safeEventPayload(item, depth + 1);
  return output;
}
