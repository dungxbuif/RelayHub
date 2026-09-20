import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import { getDeadLetter, replayDeadLetter } from "../api/adminReads";
import { useAuth } from "../auth/AuthProvider";
import { AsyncState } from "../components/AsyncState";
import { ConfirmationDialog } from "../components/ConfirmationDialog";

export function DeadLetterDetailPage() {
  const { deliveryID = "" } = useParams(); const { client } = useAuth(); const cache = useQueryClient(); const [key, setKey] = useState(""); const [message, setMessage] = useState("");
  const query = useQuery({ queryKey: ["admin-dlq-detail", deliveryID], queryFn: ({ signal }) => getDeadLetter(client, deliveryID, signal), enabled: Boolean(deliveryID) });
  const mutation = useMutation({ mutationFn: () => replayDeadLetter(client, deliveryID, key), onSuccess: async () => { setMessage("Delivery queued for replay."); setKey(""); await cache.invalidateQueries({ queryKey: ["admin-dlq"] }); } });
  return <main className="page detail-page" id="main-content"><Link className="back-link" to="/dead-letters">← Dead Letters</Link><p className="eyebrow">DEAD-LETTER DETAIL</p><h1 tabIndex={-1}>Delivery {deliveryID}</h1>
    {query.isPending && <AsyncState title="Loading delivery">Reading terminal state…</AsyncState>}{query.isError && <AsyncState title="Delivery unavailable" role="alert">{query.error.message}</AsyncState>}
    {query.data && <><section className="detail-grid"><div><span>Event</span><Link to={`/events/${encodeURIComponent(query.data.event_id)}`}><code>{query.data.event_id}</code></Link></div><div><span>Sink</span><strong>{query.data.sink}</strong></div><div><span>Reason</span><strong>{query.data.reason}</strong></div><div><span>Attempts</span><strong>{query.data.attempts}</strong></div></section><button className="danger-button" type="button" onClick={() => setKey(newDetailReplayKey())}>Replay delivery</button></>}
    {message && <p className="success-banner" role="status">{message}</p>}
    {key && <ConfirmationDialog title="Confirm replay" confirmLabel="Replay 1 delivery" pending={mutation.isPending} error={mutation.error?.message} onCancel={() => setKey("")} onConfirm={() => mutation.mutate()}><p>Create a new fenced generation for <code>{deliveryID}</code>.</p></ConfirmationDialog>}
  </main>;
}
function newDetailReplayKey() { return `admin-${typeof crypto.randomUUID === "function" ? crypto.randomUUID() : `${Date.now()}-${Math.random()}`}`; }
