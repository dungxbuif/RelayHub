import assert from 'node:assert/strict';
import test from 'node:test';
import {RelayHubQueueWorker} from '../dist/esm/node.js';

test('lost lease aborts the handler and never settles the old receipt', async () => {
  let pulled = false, settlements = 0, aborted = false;
  const errors = [];
  const worker = new RelayHubQueueWorker({
    pull: async () => { if (!pulled) {pulled = true; return {items:[{id:'job',receipt:'old'}]};} await new Promise(r=>setTimeout(r,5)); return {items:[]}; },
    extend: async () => ({items:[{receipt:'old',status:'invalid_receipt'}]}),
    settle: async () => {settlements++;return {items:[]};},
  },'sub',async (_delivery, context) => {
    assert.ok(context?.signal, 'handler must receive a lease cancellation signal');
    await new Promise(resolve => context.signal.addEventListener('abort',()=>{aborted=true;resolve();},{once:true}));
  },{concurrency:1,batchSize:1,visibilitySeconds:5,heartbeatSeconds:1,onError:e=>errors.push(e)});
  await new Promise(resolve=>setTimeout(resolve,1200));
  await worker.drain({timeoutMs:2000});
  assert.equal(aborted,true);
  assert.equal(settlements,0);
  assert.ok(errors.length>0);
});
