import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "react-router-dom";
import { getEventTimeline } from "../api/adminReads";
import { useAuth } from "../auth/AuthProvider";
import { AsyncState } from "../components/AsyncState";
import { Timeline } from "../components/Timeline";

export function EventDetailPage() {
  const { eventID = "" } = useParams(); const { client } = useAuth();
  const query = useQuery({ queryKey: ["admin-event-timeline", eventID], queryFn: ({ signal }) => getEventTimeline(client, eventID, signal), enabled: Boolean(eventID), refetchInterval: 5000 });
  return <main className="page detail-page" id="main-content"><Link className="back-link" to="/events">← Events</Link><p className="eyebrow">EVENT LIFECYCLE</p><h1 tabIndex={-1}>Event {eventID}</h1>
    {query.isPending && <AsyncState title="Loading lifecycle">Reconstructing persisted transitions…</AsyncState>}{query.isError && <AsyncState title="Lifecycle unavailable" role="alert">{query.error.message}</AsyncState>}
    {query.data && <><section className="detail-grid" aria-label="Event detail"><div><span>Type</span><strong>{query.data.event.type}</strong></div><div><span>Source</span><code>{query.data.event.source_app_id}</code></div><div><span>Deliveries</span><strong>{query.data.event.delivery_count}</strong></div></section>
      <h2 className="section-heading">Deliveries</h2>
      <div className="table-wrap"><table aria-label="Event deliveries"><thead><tr><th>Delivery / Job</th><th>Target</th><th>Transport</th><th>Status</th><th>Generation</th><th>Attempts</th><th>Reason</th></tr></thead><tbody>
        {query.data.deliveries.map(delivery => <tr key={delivery.delivery_id}><td><code>{delivery.delivery_id}</code><br /><code>{delivery.job_id}</code></td><td><code>{delivery.target_app_id}</code></td><td>{delivery.sink}</td><td>{delivery.status}</td><td>{delivery.generation}</td><td>{delivery.attempts}</td><td>{delivery.reason || "-"}</td></tr>)}
        {query.data.deliveries.length === 0 && <tr><td colSpan={7}>No deliveries recorded.</td></tr>}
      </tbody></table></div>
      <h2 className="section-heading">Attempts</h2>
      <div className="table-wrap"><table aria-label="Delivery attempts"><thead><tr><th>Delivery</th><th>Generation / Attempt</th><th>Started</th><th>Elapsed</th><th>Outcome</th><th>Reason</th></tr></thead><tbody>
        {query.data.attempts.map(attempt => <tr key={`${attempt.delivery_id}:${attempt.generation}:${attempt.attempt}`}><td><code>{attempt.delivery_id}</code></td><td>{attempt.generation} / {attempt.attempt}</td><td><time dateTime={attempt.started_at}>{new Date(attempt.started_at).toLocaleString()}</time></td><td>{Math.max(0, Date.parse(attempt.updated_at) - Date.parse(attempt.started_at))} ms</td><td>{attempt.outcome}</td><td>{attempt.reason || "-"}</td></tr>)}
        {query.data.attempts.length === 0 && <tr><td colSpan={6}>No attempts recorded.</td></tr>}
      </tbody></table></div>
      <section className="payload-card"><h2>Event data</h2><pre>{JSON.stringify(query.data.event.data, null, 2)}</pre></section><h2 className="section-heading">Lifecycle</h2><Timeline items={query.data.items} /></>}
  </main>;
}
