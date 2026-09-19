import { useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import { listDeadLetters, type DeadLetterSummary } from "../api/adminReads";
import { useAuth } from "../auth/AuthProvider";
import { AsyncState } from "../components/AsyncState";
import { DataTable, type Column } from "../components/DataTable";
import { NextPage } from "./EventsPage";

const columns: Column<DeadLetterSummary>[] = [
  { key: "delivery", label: "Delivery", render: (item) => <code>{item.delivery_id}</code> },
  { key: "target", label: "Target app", render: (item) => <code>{item.target_app_id}</code> },
  { key: "sink", label: "Sink", render: (item) => item.sink },
  { key: "reason", label: "Reason", render: (item) => item.reason },
  { key: "attempts", label: "Attempts", render: (item) => item.attempts },
  { key: "updated", label: "Updated", render: (item) => new Date(item.updated_at).toLocaleString() },
];

export function DeadLettersPage() {
  const { client } = useAuth(); const [params, setParams] = useSearchParams(); const queryString = params.toString();
  const query = useQuery({ queryKey: ["admin-dlq", queryString], queryFn: ({ signal }) => listDeadLetters(client, new URLSearchParams(queryString), signal) });
  return <main className="page" id="main-content"><p className="eyebrow">FAILURE OPERATIONS</p><h1 tabIndex={-1}>Dead Letters</h1><p className="page-lede">Inspect terminal callback and stream deliveries. Replay actions land in the lifecycle phase and are not simulated here.</p>
    <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); const data = new FormData(event.currentTarget); const next = new URLSearchParams(); for (const key of ["target_app_id", "sink", "reason"]) { const value = String(data.get(key) ?? "").trim(); if (value) next.set(key, value); } setParams(next); }}>
      <label>Target app<input name="target_app_id" defaultValue={params.get("target_app_id") ?? ""} /></label><label>Sink<select name="sink" defaultValue={params.get("sink") ?? ""}><option value="">All</option><option value="callback">Callback</option><option value="stream">Stream</option></select></label><label>Reason<input name="reason" defaultValue={params.get("reason") ?? ""} /></label><button className="primary-button" type="submit">Apply filters</button>
    </form>
    {query.isPending && <AsyncState title="Loading dead letters">Reading PostgreSQL…</AsyncState>}{query.isError && <AsyncState title="Dead letters unavailable" role="alert">{query.error.message}</AsyncState>}
    {query.data && <><DataTable caption="Dead-letter deliveries" columns={columns} rows={query.data.items} rowKey={(item) => item.delivery_id} /><NextPage cursor={query.data.next_cursor} params={params} setParams={setParams} /></>}
  </main>;
}
