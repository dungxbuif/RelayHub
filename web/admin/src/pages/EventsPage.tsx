import { useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import { listEvents, type EventSummary } from "../api/adminReads";
import { useAuth } from "../auth/AuthProvider";
import { AsyncState } from "../components/AsyncState";
import { DataTable, type Column } from "../components/DataTable";

const columns: Column<EventSummary>[] = [
  { key: "id", label: "Event", render: (item) => <code>{item.id}</code> },
  { key: "type", label: "Type", render: (item) => item.type },
  { key: "source", label: "Source app", render: (item) => <code>{item.source_app_id}</code> },
  { key: "deliveries", label: "Deliveries", render: (item) => item.delivery_count },
  { key: "created", label: "Created", render: (item) => new Date(item.created_at).toLocaleString() },
];

export function EventsPage() {
  const { client } = useAuth();
  const [params, setParams] = useSearchParams();
  const queryString = params.toString();
  const query = useQuery({ queryKey: ["admin-events", queryString], queryFn: ({ signal }) => listEvents(client, new URLSearchParams(queryString), signal) });
  return <main className="page" id="main-content"><p className="eyebrow">DURABLE STATE</p><h1 tabIndex={-1}>Events</h1><p className="page-lede">Search cross-application event summaries. Payloads stay out of list responses.</p>
    <FilterForm params={params} onApply={setParams} />
    {query.isPending && <AsyncState title="Loading events">Reading PostgreSQL…</AsyncState>}
    {query.isError && <AsyncState title="Events unavailable" role="alert">{query.error.message}</AsyncState>}
    {query.data && <><DataTable caption="Durable events" columns={columns} rows={query.data.items} rowKey={(item) => item.id} /><NextPage cursor={query.data.next_cursor} params={params} setParams={setParams} /></>}
  </main>;
}

function FilterForm({ params, onApply }: { params: URLSearchParams; onApply(value: URLSearchParams): void }) {
  return <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); const data = new FormData(event.currentTarget); const next = new URLSearchParams(); for (const key of ["type", "source_app_id"]) { const value = String(data.get(key) ?? "").trim(); if (value) next.set(key, value); } onApply(next); }}>
    <label>Event type<input name="type" defaultValue={params.get("type") ?? ""} /></label><label>Source app<input name="source_app_id" defaultValue={params.get("source_app_id") ?? ""} /></label><button className="primary-button" type="submit">Apply filters</button>
  </form>;
}

export function NextPage({ cursor, params, setParams }: { cursor?: string; params: URLSearchParams; setParams(value: URLSearchParams): void }) {
  if (!cursor) return null;
  return <button className="quiet-button pagination-button" type="button" onClick={() => { const next = new URLSearchParams(params); next.set("cursor", cursor); setParams(next); }}>Next page</button>;
}
