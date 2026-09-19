import assert from 'node:assert/strict';
import test from 'node:test';
import { RelayHubQueueWorker, RetryDelivery } from '../dist/esm/node.js';

test('queue worker converts handler retry into settlement and drains', async () => {
  let delivered = false;
  let resolveSettled;
  const settled = new Promise((resolve) => { resolveSettled = resolve; });
  const transport = {
    async pull() {
      if (!delivered) {
        delivered = true;
        return { items: [{ id: 'qdl_1', subscription_id: 'sub_1', receipt: 'receipt', attempt: 1, generation: 1, lease_expires_at: new Date(Date.now() + 60_000).toISOString(), priority: 0, event: { id: 'evt_1', type: 'order.created', source_app_id: 'producer', target_app_ids: ['worker'], data: {}, created_at: new Date().toISOString() } }] };
      }
      await new Promise((resolve) => setTimeout(resolve, 5));
      return { items: [] };
    },
    async settle(_id, items) { resolveSettled(items[0]); return { items: [{ receipt: items[0].receipt, status: 'available' }] }; },
    async extend() { return { items: [] }; },
  };
  const worker = new RelayHubQueueWorker(transport, 'sub_1', async () => { throw new RetryDelivery('busy', { delayMs: 2500 }); }, { waitSeconds: 0 });
  const settlement = await settled;
  assert.deepEqual(settlement, { receipt: 'receipt', disposition: 'retry', delay_seconds: 3, reason: 'busy' });
  await worker.drain({ timeoutMs: 1000 });
});
