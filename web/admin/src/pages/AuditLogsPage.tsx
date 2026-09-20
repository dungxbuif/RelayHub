import { useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import { listAudit, type AuditSummary } from "../api/adminReads";
import { useAuth } from "../auth/AuthProvider";
import { AsyncState } from "../components/AsyncState";
import { DataTable, type Column } from "../components/DataTable";
import { NextPage } from "./EventsPage";

const columns: Column<AuditSummary>[] = [
  { key: "time", label: "Occurred", render: (item) => new Date(item.occurred_at).toLocaleString() },
  { key: "actor", label: "Actor", render: (item) => item.actor_id ? `${item.actor_type} · ${item.actor_id}` : item.actor_type },
  { key: "action", label: "Action", render: (item) => <code>{item.action}</code> },
  { key: "resource", label: "Resource", render: (item) => item.resource_id ? `${item.resource_type} · ${item.resource_id}` : item.resource_type },
  { key: "outcome", label: "Outcome", render: (item) => item.outcome },
];

export function AuditLogsPage() {
  const { client } = useAuth(); const [params, setParams] = useSearchParams(); const queryString = params.toString();
  const query = useQuery({ queryKey: ["admin-audit", queryString], queryFn: ({ signal }) => listAudit(client, new URLSearchParams(queryString), signal) });
  return <main className="page" id="main-content"><p className="eyebrow">APPEND-ONLY RECORD</p><h1 tabIndex={-1}>Audit Logs</h1><p className="page-lede">Review bounded operator and system actions without exposing credentials or request bodies.</p>
    <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); const data = new FormData(event.currentTarget); const next = new URLSearchParams(); for (const key of ["action", "resource_type", "outcome"]) { const value = String(data.get(key) ?? "").trim(); if (value) next.set(key, value); } setParams(next); }}>
      <label>Action<input name="action" defaultValue={params.get("action") ?? ""} /></label><label>Resource type<input name="resource_type" defaultValue={params.get("resource_type") ?? ""} /></label><label>Outcome<input name="outcome" defaultValue={params.get("outcome") ?? ""} /></label><button className="primary-button" type="submit">Apply filters</button>
    </form>
    {query.isPending && <AsyncState title="Loading audit records">Reading append-only history…</AsyncState>}{query.isError && <AsyncState title="Audit unavailable" role="alert">{query.error.message}</AsyncState>}
    {query.data && <><DataTable caption="Audit records" columns={columns} rows={query.data.items} rowKey={(item) => String(item.id)} /><NextPage cursor={query.data.next_cursor} params={params} setParams={setParams} /></>}
  </main>;
}
