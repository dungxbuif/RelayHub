import { RelayHubError } from "../errors.js";
import { signRequest } from "./signing.js";

export class SignedHTTPClient {
  constructor(private readonly options: { baseUrl: string; apiKey: string; hmacSecret: string; fetch: typeof fetch; now: () => number }) {}

  async request<T>(method: string, path: string, options: { body?: unknown; idempotencyKey?: string } = {}): Promise<T> {
    const url = new URL(path, this.options.baseUrl);
    const bodyText = options.body === undefined ? "" : JSON.stringify(options.body);
    const body = new TextEncoder().encode(bodyText);
    const timestamp = Math.floor(this.options.now() / 1000).toString();
    const headers: Record<string, string> = {
      "X-RelayHub-Api-Key": this.options.apiKey,
      "X-RelayHub-Timestamp": timestamp,
      "X-RelayHub-Signature": signRequest({ secret: this.options.hmacSecret, timestamp, method, requestTarget: url.pathname + url.search, body }),
    };
    if (bodyText) headers["Content-Type"] = "application/json";
    if (options.idempotencyKey) headers["Idempotency-Key"] = options.idempotencyKey;
    const init: RequestInit = { method, headers };
    if (bodyText) init.body = bodyText;
    const response = await this.options.fetch(url, init);
    const requestId = response.headers.get("X-Request-Id") ?? undefined;
    if (!response.ok) {
      let payload: any = {};
      try { payload = await response.json(); } catch {}
      const errorOptions: { code: string; status: number; requestId?: string } = { code: payload?.error?.code ?? "http_error", status: response.status };
      if (requestId) errorOptions.requestId = requestId;
      throw new RelayHubError(payload?.error?.message ?? `RelayHub returned HTTP ${response.status}.`, errorOptions);
    }
    if (response.status === 204) return undefined as T;
    return await response.json() as T;
  }
}

export class BearerHTTPClient {
  constructor(private readonly options: { baseUrl: string; token: string; fetch: typeof fetch }) {
    if (!options.token) throw new TypeError("adminToken is required for this operation");
  }

  async request<T>(method: string, path: string, options: { body?: unknown } = {}): Promise<T> {
    const url = new URL(path, this.options.baseUrl);
    const bodyText = options.body === undefined ? "" : JSON.stringify(options.body);
    const headers: Record<string, string> = { Authorization: `Bearer ${this.options.token}` };
    if (bodyText) headers["Content-Type"] = "application/json";
    const init: RequestInit = { method, headers };
    if (bodyText) init.body = bodyText;
    const response = await this.options.fetch(url, init);
    const requestId = response.headers.get("X-Request-Id") ?? undefined;
    if (!response.ok) {
      let payload: any = {};
      try { payload = await response.json(); } catch {}
      const errorOptions: { code: string; status: number; requestId?: string } = { code: payload?.error?.code ?? "http_error", status: response.status };
      if (requestId) errorOptions.requestId = requestId;
      throw new RelayHubError(payload?.error?.message ?? `RelayHub returned HTTP ${response.status}.`, errorOptions);
    }
    if (response.status === 204) return undefined as T;
    return await response.json() as T;
  }
}
