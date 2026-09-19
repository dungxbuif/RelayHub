import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "react-router-dom";
import { getEventTimeline } from "../api/adminReads";
import { useAuth } from "../auth/AuthProvider";
import { AsyncState } from "../components/AsyncState";
import { Timeline } from "../components/Timeline";

export function EventDetailPage() {
  const { eventID = "" } = useParams(); const { client } = useAuth();
  const query = useQuery({ queryKey: ["admin-event-timeline", eventID], queryFn: ({ signal }) => getEventTimeline(client, eventID, signal), enabled: Boolean(eventID) });
  return <main className="page detail-page" id="main-content"><Link className="back-link" to="/events">← Events</Link><p className="eyebrow">EVENT LIFECYCLE</p><h1 tabIndex={-1}>Event {eventID}</h1>
    {query.isPending && <AsyncState title="Loading lifecycle">Reconstructing persisted transitions…</AsyncState>}{query.isError && <AsyncState title="Lifecycle unavailable" role="alert">{query.error.message}</AsyncState>}
    {query.data && <><section className="detail-grid" aria-label="Event detail"><div><span>Type</span><strong>{query.data.event.type}</strong></div><div><span>Source</span><code>{query.data.event.source_app_id}</code></div><div><span>Deliveries</span><strong>{query.data.event.delivery_count}</strong></div></section><section className="payload-card"><h2>Event data</h2><pre>{JSON.stringify(query.data.event.data, null, 2)}</pre></section><h2 className="section-heading">Lifecycle</h2><Timeline items={query.data.items} /></>}
  </main>;
}
