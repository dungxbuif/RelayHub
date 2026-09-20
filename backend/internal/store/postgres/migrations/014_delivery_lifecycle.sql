CREATE TABLE delivery_lifecycle (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_id text NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
    generation bigint NOT NULL CHECK (generation > 0),
    type text NOT NULL CHECK (type IN (
        'delivery.created', 'outbox.dispatched', 'stream.assigned',
        'stream.acked', 'stream.nacked', 'stream.progress',
        'callback.started', 'callback.retrying', 'callback.delivered',
        'delivery.dead_lettered', 'operator.replayed'
    )),
    outcome text,
    reason text,
    attempt integer CHECK (attempt IS NULL OR attempt > 0),
    actor_type text,
    actor_id text,
    occurred_at timestamptz NOT NULL
);

CREATE INDEX delivery_lifecycle_timeline_idx
    ON delivery_lifecycle (delivery_id, occurred_at, id);

INSERT INTO delivery_lifecycle(delivery_id,generation,type,outcome,occurred_at)
SELECT id,generation,'delivery.created','pending',created_at FROM deliveries;

INSERT INTO delivery_lifecycle(delivery_id,generation,type,outcome,occurred_at)
SELECT delivery_id,generation,'outbox.dispatched','dispatched',dispatched_at
FROM outbox WHERE dispatched_at IS NOT NULL;

INSERT INTO delivery_lifecycle(delivery_id,generation,type,outcome,reason,occurred_at)
SELECT id,generation,
       CASE
           WHEN status='acked' THEN 'stream.acked'
           WHEN status='delivered' THEN 'callback.delivered'
           WHEN status='dead_letter' THEN 'delivery.dead_lettered'
           WHEN status='retrying' AND sink='stream' THEN 'stream.nacked'
           ELSE 'callback.retrying'
       END,
       status,callback_reason,updated_at
FROM deliveries
WHERE status IN ('acked','delivered','dead_letter','retrying') AND updated_at>created_at;
