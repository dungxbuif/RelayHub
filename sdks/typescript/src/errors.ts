export class RelayHubError extends Error {
  readonly code: string;
  readonly status: number | undefined;
  readonly retryable: boolean;
  readonly requestId: string | undefined;
  constructor(message: string, options: { code: string; status?: number; retryable?: boolean; requestId?: string }) {
    super(message);
    this.name = "RelayHubError";
    this.code = options.code;
    this.status = options.status;
    this.retryable = options.retryable ?? (options.status !== undefined && (options.status === 429 || options.status >= 500));
    this.requestId = options.requestId;
  }
}

export class RetryDelivery extends Error {
  readonly delayMs: number;
  constructor(message = "Retry delivery", options: { delayMs?: number } = {}) {
    super(message);
    this.name = "RetryDelivery";
    this.delayMs = Math.max(0, Math.min(300_000, options.delayMs ?? 1_000));
  }
}
