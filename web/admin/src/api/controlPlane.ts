import type { AdminApiClient } from "./client";

export type App = { id: string; name: string; callback_url?: string; delivery_mode: "queue" | "websocket" | "callback" | "all"; enabled: boolean; created_at: string; updated_at: string };
export type Credentials = { app_id: string; api_key: string; hmac_secret: string };
export type RoutingRule = { id: string; source_app_id?: string; event_type: string; target_app_id: string; realtime_channel?: string; enabled: boolean; created_at: string; updated_at: string };
export type AppInput = { name: string; delivery_mode: App["delivery_mode"]; callback_url?: string | null };
export type RuleInput = { source_app_id?: string | null; event_type: string; target_app_id: string; realtime_channel?: string | null; enabled?: boolean };
export type RealtimeConnection = { app_id: string; client_id: string; connection_id: string; instance_id: string; protocol: string; channels: string[]; connected_at: string; last_seen_at: string };

export const listApps = (client: AdminApiClient, signal?: AbortSignal) => client.request<App[]>("/api/v1/apps", { signal });
export const createApp = (client: AdminApiClient, input: AppInput) => client.request<Credentials>("/api/v1/apps", { method: "POST", body: input });
export const updateApp = (client: AdminApiClient, id: string, input: Partial<AppInput>) => client.request<App>(`/api/v1/admin/apps/${encodeURIComponent(id)}`, { method: "PATCH", body: input });
export const disableApp = (client: AdminApiClient, id: string) => client.request<App>(`/api/v1/apps/${encodeURIComponent(id)}`, { method: "DELETE" });
export const rotateApp = (client: AdminApiClient, id: string) => client.request<Credentials>(`/api/v1/apps/${encodeURIComponent(id)}/rotate-secret`, { method: "POST" });
export const listRules = (client: AdminApiClient, signal?: AbortSignal) => client.request<RoutingRule[]>("/api/v1/routing/rules", { signal });
export const createRule = (client: AdminApiClient, input: RuleInput) => client.request<RoutingRule>("/api/v1/routing/rules", { method: "POST", body: input });
export const updateRule = (client: AdminApiClient, id: string, input: Partial<RuleInput>) => client.request<RoutingRule>(`/api/v1/routing/rules/${encodeURIComponent(id)}`, { method: "PATCH", body: input });
export const deleteRule = (client: AdminApiClient, id: string) => client.request(`/api/v1/routing/rules/${encodeURIComponent(id)}`, { method: "DELETE" });
export const studioToken = (client: AdminApiClient, appID: string, protocol: "realtime_v2" | "realtime" | "stream", clientID = "studio", channels: string[] = []) => client.request<{ token: string; expires_at: string }>("/api/v1/admin/studio/token", { method: "POST", body: { app_id: appID, protocol, ...(protocol === "realtime_v2" ? { client_id: clientID, channels } : {}) } });
export const studioPublish = (client: AdminApiClient, appID: string, channel: string, data: Record<string, unknown>) => client.request("/api/v1/admin/studio/publish", { method: "POST", body: { app_id: appID, channel, data } });
export const listConnections = (client: AdminApiClient, appID: string, signal?: AbortSignal) => client.request<{ connections: RealtimeConnection[] }>(`/api/v1/admin/connections?app_id=${encodeURIComponent(appID)}&limit=100`, { signal });
export const disconnectConnection = (client: AdminApiClient, appID: string, connectionID: string) => client.request(`/api/v1/admin/connections/${encodeURIComponent(connectionID)}?app_id=${encodeURIComponent(appID)}`, { method: "DELETE" });
