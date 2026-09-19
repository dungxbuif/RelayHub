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
function boundedQuery(source: URLSearchParams, allowed: string[]): string {
  const result = new URLSearchParams();
  for (const key of allowed) { const value = source.get(key)?.trim(); if (value) result.set(key, value.slice(0, 512)); }
  if (!result.has("limit")) result.set("limit", "25");
  return result.toString();
}
