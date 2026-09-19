import type { ErrorEnvelope } from "./types";

const unsafeMethods = new Set(["POST", "PUT", "PATCH", "DELETE"]);

export class ApiError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(redactText(message));
    this.name = "ApiError";
    this.status = status;
    this.code = code;
  }
}

type RequestOptions = Omit<RequestInit, "body"> & { body?: unknown };

export class AdminApiClient {
  constructor(private readonly csrfToken: () => string, private readonly onUnauthorized: () => void) {}

  async request<T = undefined>(path: string, options: RequestOptions = {}): Promise<T> {
    const method = (options.method ?? "GET").toUpperCase();
    const headers = new Headers(options.headers);
    headers.set("Accept", "application/json");
    if (unsafeMethods.has(method)) {
      const csrf = this.csrfToken();
      if (csrf) headers.set("X-RelayHub-CSRF", csrf);
    }
    let body: BodyInit | undefined;
    if (options.body !== undefined) {
      headers.set("Content-Type", "application/json");
      body = JSON.stringify(options.body);
    }
    const response = await fetch(path, { ...options, method, headers, body, credentials: "same-origin" });
    const text = await response.text();
    let payload: unknown;
    if (text && (response.headers.get("Content-Type") ?? "").includes("application/json")) {
      try { payload = JSON.parse(text); }
      catch { throw new ApiError(response.status, "invalid_response", "RelayHub returned malformed JSON."); }
    }
    if (!response.ok) {
      if (response.status === 401) this.onUnauthorized();
      const envelope = payload as ErrorEnvelope | undefined;
      throw new ApiError(response.status, envelope?.error?.code ?? "request_failed", envelope?.error?.message ?? `Request failed with HTTP ${response.status}.`);
    }
    if (response.status === 204 || payload === undefined) return undefined as T;
    return payload as T;
  }
}

export function redactText(value: string): string {
  return value
    .replace(/(Bearer\s+)[^\s,;]+/gi, "$1[REDACTED]")
    .replace(/((?:X-RelayHub-(?:Api-Key|Signature)|Authorization|Cookie)\s*[:=]\s*)[^\s,;]+/gi, "$1[REDACTED]")
    .replace(/([?&](?:token|access_token)=)[^&\s]+/gi, "$1[REDACTED]")
    .slice(0, 1_024);
}
