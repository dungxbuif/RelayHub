// Identity interfaces mirror rust/src/identity.rs OpenAPI; protocol interfaces
// mirror contracts/v1/schema.json. No secret is persisted by this client.
export type Principal =
  | { kind: "system_admin"; user_id: string }
  | { kind: "workspace_owner"; user_id: string; workspace_id: string }
  | { kind: "application"; workspace_id: string; app_id: string; key_id: string }
  | { kind: "agent"; workspace_id: string; app_id: string; agent_id: string; key_id: string };
export interface Session { token: string; csrf_token: string; expires_in_seconds: number }
export interface ProvisionedUser { user_id: string; workspace_id: string; setup_token: string; expires_in_seconds: number }
export interface Application { id: string; workspace_id: string; name: string }
export interface Credential { id: string; workspace_id: string; app_id: string; kind: "application" | "agent"; agent_id: string | null; token: string }
export type EndpointPolicy = { mode: "async" } | { mode: "sync"; timeout_seconds: number; offline: "queue" | "reject"; timeout: "continue" | "expire_pending" };
export interface EventInput { endpoint_id: string; event_type: string; method: string; path: string; headers: [string, string][]; body: string }
export interface Accepted { event_id: string; delivery_id: string; status: number }
export interface HttpResponse { status: number; headers: [string, string][]; body: string }
export interface DeliveryView { delivery_id: string; event_id: string; generation: number; state: string; attempts: number }
export interface EventStatus extends DeliveryView { response_state: string; response: HttpResponse | null }
export interface PublishReceipt { status: number; observed_recipients: number }

// shortcut: dashboard HTTP adapters are provisional, freeze their OpenAPI before release.
// INTEGRATION HANDOFF: Rust currently exposes none of the
// following dashboard adapters. Freeze/mount these contracts before release:
// GET/POST /workspaces/{id}/endpoints; PUT .../endpoints/{id}/policy;
// GET .../agents; GET .../deliveries?state=&cursor=&limit=25;
// POST .../deliveries/{id}/replay -> Accepted; GET .../deliveries/{id}/audit;
// POST .../events -> Accepted (normal durable acceptance, Idempotency-Key);
// POST .../realtime-tokens -> RealtimeToken, then teach /realtime to verify it.
// All adapters MUST authorize cookie principal/workspace server-side. Tokens
// MUST be short-lived and bound to workspace/channel/publish-only permission.
export interface Endpoint { id: string; app_id: string; policy: EndpointPolicy; auth: "bearer" | "public" }
export interface CreatedEndpoint extends Endpoint { ingress_secret?: string }
export interface AgentPresence { agent_id: string; app_id: string; online: boolean }
export interface Delivery extends DeliveryView { input?: EventInput }
export interface DeliveryPage { items: Delivery[]; next_cursor: string | null }
export interface ReplayAudit { delivery_id: string; source_delivery_id: string; actor_user_id: string; created_at: string }
export interface RealtimeToken { token: string; workspace_id: string; channel: string; expires_in_seconds: number }

export class ApiError extends Error {
  constructor(public readonly status: number) {
    super(status === 403 ? "403 — Access denied." : status === 401 ? "401 — Sign in again." : status === 409 ? "409 — State changed; refresh before trying again." : `Request failed (HTTP ${status}).`);
  }
}
class InputError extends Error {}
export function errorMessage(error: unknown): string {
  return error instanceof ApiError || error instanceof InputError ? error.message : "Unable to complete the request. Check the connection and try again.";
}
export const workspacePath = (id: string) => `/workspaces/${encodeURIComponent(id)}`;
export const deliveryState = (state: string) => state === "succeeded" ? "ACK (succeeded)" : state;
export function encodeBody(text: string): string {
  const bytes = new TextEncoder().encode(text);
  if (bytes.length > 1_048_576) throw new InputError("Payload exceeds the 1 MiB limit.");
  return btoa(Array.from(bytes, byte => String.fromCharCode(byte)).join(""));
}
export function payloadText(body: string): string {
  try { return new TextDecoder().decode(Uint8Array.from(atob(body), c => c.charCodeAt(0))); }
  catch { return "Invalid base64 payload"; }
}

let csrf = "";
export const api = {
  canWrite: () => Boolean(csrf),
  async request<T>(path: string, method = "GET", body?: unknown, extraHeaders: Record<string, string> = {}): Promise<T> {
    // Never allow callers to send cookies/tokens to another origin or API.
    if (!path.startsWith("/") || path.startsWith("//") || path.includes("\\")) throw new Error("Invalid API path");
    const headers = new Headers(extraHeaders);
    headers.set("Accept", "application/json");
    if (method !== "GET" && !(path === "/session" && method === "POST")) {
      if (!csrf) throw new ApiError(401);
      headers.set("X-CSRF-Token", csrf);
    }
    if (body !== undefined) headers.set("Content-Type", "application/json");
    const response = await fetch(`/api/v1${path}`, {
      method, headers, credentials: "include", cache: "no-store", redirect: "error",
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!response.ok) {
      if (response.status === 401) csrf = "";
      // Error bodies can contain reflected credentials; never render or log them.
      throw new ApiError(response.status);
    }
    if (response.status === 204) return undefined as T;
    return response.json() as Promise<T>;
  },
  async login(email: string, password: string): Promise<Principal> {
    csrf = "";
    const session = await api.request<Session>("/session", "POST", { email, password });
    csrf = session.csrf_token;
    return api.request<Principal>("/session");
  },
  async logout() {
    await api.request<void>("/session", "DELETE");
    csrf = "";
  },
  async realtime(workspace: string, channel: string, payload: string): Promise<PublishReceipt> {
    const body = encodeBody(payload);
    const scoped = await api.request<RealtimeToken>(`${workspacePath(workspace)}/realtime-tokens`, "POST", { channel, expires_in_seconds: 60 });
    if (!scoped.token || scoped.workspace_id !== workspace || scoped.channel !== channel || !Number.isFinite(scoped.expires_in_seconds) || scoped.expires_in_seconds <= 0 || scoped.expires_in_seconds > 60) {
      throw new InputError("Invalid realtime token scope or expiry");
    }
    return api.request<PublishReceipt>("/realtime", "POST", {
      type: "realtime", version: 1, request_id: crypto.randomUUID(), channel, body,
    }, { Authorization: `Bearer ${scoped.token}` });
  },
};
