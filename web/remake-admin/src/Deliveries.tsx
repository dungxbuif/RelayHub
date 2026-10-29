import { useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { api, ApiError, deliveryState, errorMessage, payloadText, workspacePath, type Accepted, type Delivery, type DeliveryPage, type ReplayAudit } from "./api";

function ConfirmReplay({ delivery, busy, onCancel, onConfirm }: { delivery: Delivery; busy: boolean; onCancel(): void; onConfirm(): void }) {
  const dialog = useRef<HTMLDialogElement>(null);
  useEffect(() => { dialog.current?.showModal(); }, []);
  return <dialog ref={dialog} aria-labelledby="replay-title" onCancel={event => { event.preventDefault(); if (!busy) onCancel(); }}>
    <h2 id="replay-title">Confirm replay</h2>
    <p>Replay delivery {delivery.delivery_id}? A new generation retains event {delivery.event_id} and its audit trail. Side effects may run again.</p>
    <button autoFocus disabled={busy} onClick={onCancel}>Cancel</button>
    <button disabled={busy} onClick={onConfirm}>Confirm replay</button>
  </dialog>;
}
export function Deliveries({ id }: { id: string }) {
  const path = workspacePath(id);
  const [filter, setFilter] = useState("");
  const [cursor, setCursor] = useState("");
  const [revision, setRevision] = useState(0);
  const [page, setPage] = useState<DeliveryPage | null>(null);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [loading, setLoading] = useState(true);
  const [denied, setDenied] = useState(false);
  const [selected, setSelected] = useState<Delivery | null>(null);
  const [busy, setBusy] = useState(false);
  const [needsRefresh, setNeedsRefresh] = useState(false);
  useEffect(() => {
    let active = true;
    setLoading(true); setPage(null); setError("");
    const query = new URLSearchParams({ limit: "25" });
    if (filter) query.set("state", filter);
    if (cursor) query.set("cursor", cursor);
    api.request<DeliveryPage>(`${path}/deliveries?${query}`).then(result => {
      if (active) { setPage(result); setNeedsRefresh(false); }
    }).catch(error => {
      if (active) {
        setError(errorMessage(error)); setMessage("");
        if (error instanceof ApiError && [401, 403].includes(error.status)) setDenied(true);
      }
    }).finally(() => { if (active) setLoading(false); });
    return () => { active = false; };
  }, [path, filter, cursor, revision]);
  async function replay() {
    if (!selected) return;
    setBusy(true); setMessage(""); setError("");
    let accepted = false;
    try {
      const receipt = await api.request<Accepted>(`${path}/deliveries/${encodeURIComponent(selected.delivery_id)}/replay`, "POST", {});
      accepted = true;
      const audit = await api.request<ReplayAudit[]>(`${path}/deliveries/${encodeURIComponent(receipt.delivery_id)}/audit`);
      const evidence = audit.find(item => item.source_delivery_id === selected.delivery_id && item.delivery_id === receipt.delivery_id && item.actor_user_id && item.created_at);
      if (!evidence || receipt.event_id !== selected.event_id || receipt.delivery_id === selected.delivery_id) {
        setError("Replay accepted; audit evidence unavailable. Refresh before any further replay."); return;
      }
      setMessage(`Audited replay: ${evidence.source_delivery_id} → ${evidence.delivery_id}. Actor: ${evidence.actor_user_id}. Recorded: ${evidence.created_at}.`);
    } catch (error) {
      setError(accepted ? `${errorMessage(error)} Replay accepted; audit evidence unavailable. Refresh before any further replay.` : errorMessage(error));
      if (error instanceof ApiError && [401, 403].includes(error.status)) { setDenied(true); setPage(null); }
    } finally { setNeedsRefresh(true); setBusy(false); setSelected(null); }
  }
  if (denied) return <p role="alert">{error}</p>;
  return <>
    <h1>Durable deliveries</h1><Link to={path}>Workspace</Link>
    <p>Retry terminal work with a confirmed replay. Refresh after replay to inspect the new generation.</p>
    <label>Delivery state<select value={filter} disabled={busy || loading} onChange={event => { setFilter(event.target.value); setCursor(""); setMessage(""); }}>
      <option value="">All states</option>{["pending", "in_flight", "retry_wait", "succeeded", "dlq", "expired", "cancelled"].map(state => <option key={state} value={state}>{deliveryState(state)}</option>)}
    </select></label>
    <button disabled={busy || loading} onClick={() => { setCursor(""); setRevision(value => value + 1); }}>Refresh deliveries</button>
    {error && <p role="alert">{error}</p>}{message && <p role="status">{message}</p>}
    {loading && <p>Loading deliveries…</p>}
    {page?.items.length === 0 && <p>No deliveries match this filter.</p>}
    {page?.items.map(delivery => <article key={delivery.delivery_id}>
      <h2>{delivery.delivery_id}</h2>
      <p>Event {delivery.event_id} · generation {delivery.generation} · attempts {delivery.attempts}</p>
      <p>{deliveryState(delivery.state)}</p>
      {delivery.input && <><h3>Payload text</h3><pre>{payloadText(delivery.input.body)}</pre></>}
      {["succeeded", "dlq", "cancelled"].includes(delivery.state) && <button disabled={busy || needsRefresh || !api.canWrite()} onClick={() => setSelected(delivery)}>Replay</button>}
    </article>)}
    <nav aria-label="Delivery pages">
      <button disabled={!cursor || busy || loading} onClick={() => setCursor("")}>First page</button>
      <button disabled={!page?.next_cursor || busy || loading} onClick={() => { setCursor(page!.next_cursor!); setMessage(""); }}>Next page</button>
    </nav>
    {selected && <ConfirmReplay delivery={selected} busy={busy} onCancel={() => setSelected(null)} onConfirm={() => void replay()} />}
  </>;
}
