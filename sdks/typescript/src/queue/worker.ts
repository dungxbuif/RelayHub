import { DeadLetterDelivery, RelayHubError, RetryDelivery } from "../errors.js";
import type { ConsumerHandle, QueueDelivery, QueueHandler, QueueSettlement, QueueSettlementResult } from "../types.js";

export interface QueueWorkerTransport {
  pull(subscriptionId: string, input: { max_messages: number; wait_seconds: number; visibility_seconds: number }): Promise<{ items: QueueDelivery[] }>;
  settle(subscriptionId: string, items: QueueSettlement[]): Promise<{ items: QueueSettlementResult[] }>;
  extend(subscriptionId: string, items: Array<{ receipt: string; extension_seconds: number }>): Promise<{ items: QueueSettlementResult[] }>;
}

export interface QueueWorkerOptions {
  concurrency?: number;
  batchSize?: number;
  waitSeconds?: number;
  visibilitySeconds?: number;
  heartbeatSeconds?: number;
  retryDelaySeconds?: number;
  onError?: (error: unknown, delivery?: QueueDelivery) => void;
}

export class RelayHubQueueWorker implements ConsumerHandle {
  private stopping = false;
  private readonly active = new Set<Promise<void>>();
  private readonly loop: Promise<void>;
  private readonly options: Required<Omit<QueueWorkerOptions, "onError">> & { onError: QueueWorkerOptions["onError"] };

  constructor(private readonly transport: QueueWorkerTransport, private readonly subscriptionId: string, private readonly handler: QueueHandler, options: QueueWorkerOptions = {}) {
    this.options = {
      concurrency: bounded(options.concurrency ?? 4, 1, 100),
      batchSize: bounded(options.batchSize ?? 10, 1, 100),
      waitSeconds: bounded(options.waitSeconds ?? 20, 0, 30),
      visibilitySeconds: bounded(options.visibilitySeconds ?? 60, 1, 3600),
      heartbeatSeconds: bounded(options.heartbeatSeconds ?? 20, 1, 3600),
      retryDelaySeconds: bounded(options.retryDelaySeconds ?? 5, 0, 86400),
      onError: options.onError,
    };
    if (!subscriptionId || typeof handler !== "function") throw new TypeError("subscriptionId and handler are required");
    this.loop = this.run();
  }

  private async run(): Promise<void> {
    while (!this.stopping) {
      const capacity = this.options.concurrency - this.active.size;
      if (capacity <= 0) { await Promise.race(this.active); continue; }
      try {
        const batch = await this.transport.pull(this.subscriptionId, { max_messages: Math.min(capacity, this.options.batchSize), wait_seconds: this.options.waitSeconds, visibility_seconds: this.options.visibilitySeconds });
        for (const delivery of batch.items) {
          const task = this.process(delivery).finally(() => this.active.delete(task));
          this.active.add(task);
        }
      } catch (error) {
        this.options.onError?.(error);
        if (!this.stopping) await delay(250);
      }
    }
  }

  private async process(delivery: QueueDelivery): Promise<void> {
    const controller = new AbortController();
    let heartbeat: ReturnType<typeof setInterval> | undefined;
    let heartbeatInFlight = Promise.resolve();
    const heartbeatMs = this.options.heartbeatSeconds * 1000;
    if (heartbeatMs > 0) {
      heartbeat = setInterval(() => {
        heartbeatInFlight = heartbeatInFlight.then(async () => {
          if (controller.signal.aborted) return;
          const result = await this.transport.extend(this.subscriptionId, [{ receipt: delivery.receipt, extension_seconds: this.options.heartbeatSeconds }]);
          if (result.items.length !== 1 || result.items[0]?.receipt !== delivery.receipt || result.items[0]?.status !== 'extended') {
            throw new RelayHubError('Queue lease is no longer valid.', {code: 'invalid_receipt'});
          }
        }).catch(error => {controller.abort(); this.options.onError?.(error, delivery);});
      }, heartbeatMs);
    }
    let settlement: QueueSettlement = { receipt: delivery.receipt, disposition: "ack" };
    try {
      await this.handler(delivery, {signal: controller.signal});
    } catch (error) {
      this.options.onError?.(error, delivery);
      if (error instanceof DeadLetterDelivery) settlement = { receipt: delivery.receipt, disposition: "dead_letter", reason: safeReason(error.message) };
      else settlement = { receipt: delivery.receipt, disposition: "retry", delay_seconds: error instanceof RetryDelivery ? Math.ceil(error.delayMs / 1000) : this.options.retryDelaySeconds, reason: safeReason(error instanceof Error ? error.message : "handler_error") };
    } finally {
      if (heartbeat) clearInterval(heartbeat);
      await heartbeatInFlight;
    }
    if (controller.signal.aborted) return;
    try {
      const result = await this.transport.settle(this.subscriptionId, [settlement]);
      if (result.items.length !== 1 || result.items[0]?.receipt !== delivery.receipt || result.items[0]?.status === 'invalid_receipt') {
        throw new RelayHubError("Queue receipt expired before settlement.", { code: "invalid_receipt" });
      }
    } catch (error) { this.options.onError?.(error, delivery); }
  }

  async drain(options: { timeoutMs?: number } = {}): Promise<void> {
    this.stopping = true;
    const timeoutMs = bounded(options.timeoutMs ?? 30_000, 1, 300_000);
    const completion = (async () => { await this.loop; await Promise.allSettled([...this.active]); })();
    await Promise.race([completion, delay(timeoutMs).then(() => { throw new RelayHubError("Queue worker drain timed out.", { code: "drain_timeout" }); })]);
  }
}

function bounded(value: number, minimum: number, maximum: number): number {
  if (!Number.isInteger(value) || value < minimum || value > maximum) throw new TypeError(`value must be an integer between ${minimum} and ${maximum}`);
  return value;
}
function delay(milliseconds: number): Promise<void> { return new Promise((resolve) => setTimeout(resolve, milliseconds)); }
function safeReason(value: string): string { return value.slice(0, 1024); }
