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

export class AdminSessionHTTPClient {
  constructor(private readonly options: { baseUrl: string; cookie: string; csrfToken: string; fetch: typeof fetch }) {
    const base = new URL(options.baseUrl);
    if (!/^__Host-relayhub_admin=[A-Za-z0-9_-]+$/.test(options.cookie) || !/^[A-Za-z0-9_-]+$/.test(options.csrfToken) ||
        base.username || base.password || base.search || base.hash || (base.pathname !== '/' && base.pathname !== '') ||
        (base.protocol !== 'https:' && !(base.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(base.hostname)))) {
      throw new TypeError("A valid admin session and HTTPS origin are required");
    }
  }

  async request<T>(method: string, path: string, options: { body?: unknown } = {}): Promise<T> {
    const url = new URL(path, this.options.baseUrl);
    const bodyText = options.body === undefined ? "" : JSON.stringify(options.body);
    if (url.origin !== new URL(this.options.baseUrl).origin) throw new TypeError("Cross-origin admin request refused");
    const headers: Record<string, string> = { Cookie: this.options.cookie, 'X-RelayHub-CSRF': this.options.csrfToken };
    if (bodyText) headers["Content-Type"] = "application/json";
    const init: RequestInit = { method, headers, redirect: 'error' };
    if (bodyText) init.body = bodyText;
    let response: Response;
    try { response = await this.options.fetch(url, init); }
    catch { throw new RelayHubError("Admin request transport failed.", {code: "transport_error", status: 0}); }
    const requestId = response.headers.get("X-Request-Id") ?? undefined;
    if (!response.ok) {
      let payload: any = {};
      try { payload = await response.json(); } catch {}
      const code = /^[a-z][a-z0-9_]{0,63}$/.test(payload?.error?.code ?? '') ? payload.error.code : 'http_error';
      const errorOptions: { code: string; status: number; requestId?: string } = { code, status: response.status };
      if (requestId) errorOptions.requestId = requestId;
      throw new RelayHubError(`RelayHub returned HTTP ${response.status}.`, errorOptions);
    }
    if (response.status === 204) return undefined as T;
    return await response.json() as T;
  }
}
