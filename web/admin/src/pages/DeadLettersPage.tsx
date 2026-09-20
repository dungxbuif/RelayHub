import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { listDeadLetters, replayDeadLetters, type DeadLetterSummary } from "../api/adminReads";
import { useAuth } from "../auth/AuthProvider";
import { AsyncState } from "../components/AsyncState";
import { ConfirmationDialog } from "../components/ConfirmationDialog";
import { NextPage } from "./EventsPage";

export function DeadLettersPage() {
  const { client } = useAuth(); const [params, setParams] = useSearchParams(); const queryString = params.toString();
  const cache = useQueryClient(); const [selected, setSelected] = useState<string[]>([]); const [confirmation, setConfirmation] = useState<{ ids: string[]; key: string }>(); const [message, setMessage] = useState("");
  const query = useQuery({ queryKey: ["admin-dlq", queryString], queryFn: ({ signal }) => listDeadLetters(client, new URLSearchParams(queryString), signal) });
  const mutation = useMutation({ mutationFn: ({ ids, key }: { ids: string[]; key: string }) => replayDeadLetters(client, ids, key), onSuccess: async (result) => { setMessage(`${result.items.length} ${result.items.length === 1 ? "delivery" : "deliveries"} queued for replay.`); setSelected([]); setConfirmation(undefined); await cache.invalidateQueries({ queryKey: ["admin-dlq"] }); } });
  const openConfirmation = () => setConfirmation({ ids: [...selected].sort(), key: newReplayKey() });
  return <main className="page" id="main-content"><p className="eyebrow">FAILURE OPERATIONS</p><h1 tabIndex={-1}>Dead Letters</h1><p className="page-lede">Inspect terminal callback and stream deliveries, then replay an exact audited selection.</p>
    <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); const data = new FormData(event.currentTarget); const next = new URLSearchParams(); for (const key of ["target_app_id", "sink", "reason"]) { const value = String(data.get(key) ?? "").trim(); if (value) next.set(key, value); } setParams(next); }}>
      <label>Target app<input name="target_app_id" defaultValue={params.get("target_app_id") ?? ""} /></label><label>Sink<select name="sink" defaultValue={params.get("sink") ?? ""}><option value="">All</option><option value="callback">Callback</option><option value="stream">Stream</option></select></label><label>Reason<input name="reason" defaultValue={params.get("reason") ?? ""} /></label><button className="primary-button" type="submit">Apply filters</button>
    </form>
    <div className="selection-bar" aria-live="polite"><span>{selected.length} of 100 selected</span><button className="danger-button" type="button" disabled={selected.length === 0 || mutation.isPending} onClick={openConfirmation}>Replay selected ({selected.length})</button></div>
    {message && <p className="success-banner" role="status">{message}</p>}
    {query.isPending && <AsyncState title="Loading dead letters">Reading PostgreSQL…</AsyncState>}{query.isError && <AsyncState title="Dead letters unavailable" role="alert">{query.error.message}</AsyncState>}
    {query.data && <><DeadLetterTable rows={query.data.items} selected={selected} onToggle={(id) => setSelected((current) => current.includes(id) ? current.filter((value) => value !== id) : current.length < 100 ? [...current, id] : current)} /><NextPage cursor={query.data.next_cursor} params={params} setParams={setParams} /></>}
    {confirmation && <ConfirmationDialog title="Confirm replay" confirmLabel={`Replay ${confirmation.ids.length} ${confirmation.ids.length === 1 ? "delivery" : "deliveries"}`} pending={mutation.isPending} error={mutation.error?.message} onCancel={() => setConfirmation(undefined)} onConfirm={() => mutation.mutate(confirmation)}><p>RelayHub will create a new fenced delivery generation for exactly:</p><ul className="id-list">{confirmation.ids.map((id) => <li key={id}><code>{id}</code></li>)}</ul></ConfirmationDialog>}
  </main>;
}

function DeadLetterTable({ rows, selected, onToggle }: { rows: DeadLetterSummary[]; selected: string[]; onToggle(id: string): void }) {
  if (rows.length === 0) return <section className="empty-card"><h2>No results</h2><p>Try changing the filters or wait for new operational data.</p></section>;
  return <div className="table-wrap"><table><caption>Dead-letter deliveries</caption><thead><tr><th scope="col">Select</th><th scope="col">Delivery</th><th scope="col">Target app</th><th scope="col">Sink</th><th scope="col">Reason</th><th scope="col">Attempts</th><th scope="col">Updated</th></tr></thead><tbody>{rows.map((item) => <tr key={item.delivery_id}><td><input type="checkbox" aria-label={`Select delivery ${item.delivery_id}`} checked={selected.includes(item.delivery_id)} onChange={() => onToggle(item.delivery_id)} /></td><td><Link className="table-link" to={`/dead-letters/${encodeURIComponent(item.delivery_id)}`}><code>{item.delivery_id}</code></Link></td><td><code>{item.target_app_id}</code></td><td>{item.sink}</td><td>{item.reason}</td><td>{item.attempts}</td><td>{new Date(item.updated_at).toLocaleString()}</td></tr>)}</tbody></table></div>;
}

function newReplayKey(): string { return `admin-${typeof crypto.randomUUID === "function" ? crypto.randomUUID() : `${Date.now()}-${Math.random()}`}`; }
