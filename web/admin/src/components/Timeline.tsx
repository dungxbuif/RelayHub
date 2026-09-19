import type { TimelineItem } from "../api/adminReads";

const labels: Record<string, string> = {
  "event.created": "Event created", "delivery.created": "Delivery created", "outbox.dispatched": "Outbox dispatched",
  "stream.assigned": "Stream assigned", "stream.acked": "Stream acknowledged", "stream.nacked": "Stream released",
  "stream.progress": "Stream progress", "callback.started": "Callback started", "callback.retrying": "Callback retry scheduled",
  "callback.delivered": "Callback delivered", "delivery.dead_lettered": "Delivery dead-lettered", "operator.replayed": "Operator replayed delivery",
};

export function Timeline({ items }: { items: TimelineItem[] }) {
  if (items.length === 0) return <section className="empty-card"><h2>No lifecycle recorded</h2><p>This event has no persisted lifecycle transitions.</p></section>;
  return <ol className="timeline" aria-label="Event lifecycle">{items.map((item) => <li key={item.id}>
    <span className="timeline-dot" aria-hidden="true" /><div className="timeline-card"><div className="timeline-heading"><strong>{labels[item.type] ?? item.type}</strong><time dateTime={item.occurred_at}>{new Date(item.occurred_at).toLocaleString()}</time></div>
      <div className="badge-row">{item.generation ? <span>Generation {item.generation}</span> : null}{item.attempt ? <span>Attempt {item.attempt}</span> : null}{item.outcome ? <span>{item.outcome}</span> : null}</div>
      {item.delivery_id && <code>{item.delivery_id}</code>}{item.reason && <p className="muted">Reason: {item.reason}</p>}
    </div>
  </li>)}</ol>;
}
