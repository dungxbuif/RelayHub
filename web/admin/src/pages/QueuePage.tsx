import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { useAuth } from "../auth/AuthProvider";
import { PermissionGate } from "../auth/PermissionGate";
import {
  createAdminQueueSchedule,
  createAdminSubscription,
  deleteAdminQueueDeadLetters,
  deleteAdminQueueSchedule,
  deleteAdminSubscription,
  drainAdminQueue,
  extendAdminQueueLeases,
  exportAdminQueueDeadLetters,
  getAdminQueueDepth,
  listAdminQueueCallbacks,
  listAdminQueueDeadLetters,
  listAdminQueueSchedules,
  listAdminSubscriptions,
  listApps,
  pauseAdminSubscription,
  pullAdminQueue,
  replayAdminQueueDeadLetters,
  resumeAdminSubscription,
  settleAdminQueue,
  type QueueDeadLetter,
  type QueueSchedule,
  type QueueSubscriptionInput,
} from "../api/controlPlane";
import { AsyncState } from "../components/AsyncState";
import { ConfirmationDialog } from "../components/ConfirmationDialog";
import { JsonEditor, validateJson } from "../components/JsonEditor";
import { MutationFeedback, useAdminMutationFeedback } from "../components/MutationFeedback";

const defaultScheduleData = "{}";
const defaultBatch = "[]";

export function QueuePage() {
  const { client } = useAuth();
  const queryClient = useQueryClient();
  const [appID, setAppID] = useState("");
  const [subscriptionID, setSubscriptionID] = useState("");
  const [selectedDeadLetters, setSelectedDeadLetters] = useState<string[]>([]);
  const [confirmation, setConfirmation] = useState<{ kind: "drain" | "delete-subscription" | "delete-dlq" | "delete-schedule"; ids?: string[]; schedule?: QueueSchedule }>();
  const [scheduleData, setScheduleData] = useState(defaultScheduleData);
  const [settlementItems, setSettlementItems] = useState(defaultBatch);
  const [leaseItems, setLeaseItems] = useState(defaultBatch);
  const feedback = useAdminMutationFeedback();

  const apps = useQuery({ queryKey: ["admin-apps"], queryFn: ({ signal }) => listApps(client, signal) });
  const subscriptions = useQuery({ queryKey: ["queue-subscriptions", appID], queryFn: ({ signal }) => listAdminSubscriptions(client, appID, signal), enabled: Boolean(appID), refetchInterval: 10_000 });
  useEffect(() => { if (!subscriptions.data?.items.some((item) => item.id === subscriptionID)) setSubscriptionID(subscriptions.data?.items[0]?.id ?? ""); }, [subscriptions.data, subscriptionID]);

  const enabled = Boolean(appID && subscriptionID);
  const selected = subscriptions.data?.items.find((item) => item.id === subscriptionID);
  const depth = useQuery({ queryKey: ["queue-depth", appID, subscriptionID], queryFn: ({ signal }) => getAdminQueueDepth(client, appID, subscriptionID, signal), enabled, refetchInterval: 5_000 });
  const schedules = useQuery({ queryKey: ["queue-schedules", appID, subscriptionID], queryFn: ({ signal }) => listAdminQueueSchedules(client, appID, subscriptionID, signal), enabled, refetchInterval: 10_000 });
  const callbacks = useQuery({ queryKey: ["queue-callbacks", appID, subscriptionID], queryFn: ({ signal }) => listAdminQueueCallbacks(client, appID, subscriptionID, signal), enabled, refetchInterval: 5_000 });
  const deadLetters = useQuery({ queryKey: ["queue-dead-letters", appID, subscriptionID], queryFn: ({ signal }) => listAdminQueueDeadLetters(client, appID, subscriptionID, "", signal), enabled, refetchInterval: 10_000 });
  const invalidateQueue = async () => { await Promise.all([queryClient.invalidateQueries({ queryKey: ["queue-subscriptions", appID] }), queryClient.invalidateQueries({ queryKey: ["queue-schedules", appID, subscriptionID] }), queryClient.invalidateQueries({ queryKey: ["queue-dead-letters", appID, subscriptionID] }), queryClient.invalidateQueries({ queryKey: ["queue-depth", appID, subscriptionID] })]); };

  const subscriptionInput = (data: FormData): QueueSubscriptionInput => ({ name: String(data.get("subscription_name") ?? "").trim(), max_in_flight: Number(data.get("max_in_flight") ?? 10), max_batch_size: Number(data.get("max_batch_size") ?? 10), ordering_mode: "none" });
  const createSubscription = useMutation({ mutationFn: (input: QueueSubscriptionInput) => createAdminSubscription(client, appID, input), onSuccess: async () => { feedback.succeed("Subscription created."); await invalidateQueue(); }, onError: feedback.fail });
  const pause = useMutation({ mutationFn: () => pauseAdminSubscription(client, appID, subscriptionID), onSuccess: async () => { feedback.succeed("Subscription paused."); await invalidateQueue(); }, onError: feedback.fail });
  const resume = useMutation({ mutationFn: () => resumeAdminSubscription(client, appID, subscriptionID), onSuccess: async () => { feedback.succeed("Subscription resumed."); await invalidateQueue(); }, onError: feedback.fail });
  const drain = useMutation({ mutationFn: () => drainAdminQueue(client, appID, subscriptionID), onSuccess: async () => { feedback.succeed("Drain started."); setConfirmation(undefined); await invalidateQueue(); }, onError: feedback.fail });
  const deleteSubscription = useMutation({ mutationFn: () => deleteAdminSubscription(client, appID, subscriptionID), onSuccess: async () => { feedback.succeed("Subscription deleted."); setConfirmation(undefined); setSubscriptionID(""); await invalidateQueue(); }, onError: feedback.fail });
  const createSchedule = useMutation({ mutationFn: (input: Parameters<typeof createAdminQueueSchedule>[3]) => createAdminQueueSchedule(client, appID, subscriptionID, input), onSuccess: async () => { feedback.succeed("Schedule created."); await invalidateQueue(); }, onError: feedback.fail });
  const deleteSchedule = useMutation({ mutationFn: (scheduleID: string) => deleteAdminQueueSchedule(client, appID, subscriptionID, scheduleID), onSuccess: async () => { feedback.succeed("Schedule deleted."); setConfirmation(undefined); await invalidateQueue(); }, onError: feedback.fail });
  const replayDeadLetters = useMutation({ mutationFn: (ids: string[]) => replayAdminQueueDeadLetters(client, appID, subscriptionID, ids), onSuccess: async (result) => { feedback.succeed(`${result.replayed} ${result.replayed === 1 ? "dead letter" : "dead letters"} queued for replay.`); setSelectedDeadLetters([]); await invalidateQueue(); }, onError: feedback.fail });
  const deleteDeadLetters = useMutation({ mutationFn: (ids: string[]) => deleteAdminQueueDeadLetters(client, appID, subscriptionID, ids), onSuccess: async (result) => { feedback.succeed(`${result.deleted} ${result.deleted === 1 ? "dead letter" : "dead letters"} deleted.`); setSelectedDeadLetters([]); setConfirmation(undefined); await invalidateQueue(); }, onError: feedback.fail });
  const exportDeadLetters = useMutation({ mutationFn: () => exportAdminQueueDeadLetters(client, appID, subscriptionID), onSuccess: (result) => { downloadJSON("relayhub-dlq.json", result); feedback.succeed("Dead-letter export prepared."); }, onError: feedback.fail });
  const pull = useMutation({ mutationFn: (input: { max_messages: number; wait_seconds?: number; visibility_seconds?: number }) => pullAdminQueue(client, appID, subscriptionID, input), onSuccess: (result) => feedback.succeed(`${result.items.length} ${result.items.length === 1 ? "delivery" : "deliveries"} leased.`), onError: feedback.fail });
  const settle = useMutation({ mutationFn: (items: Array<{ receipt: string; disposition: "acked" | "available" | "dead_letter"; delay_seconds?: number; reason?: string }>) => settleAdminQueue(client, appID, subscriptionID, items), onSuccess: (result) => feedback.succeed(`${result.items.length} settlement result${result.items.length === 1 ? "" : "s"} received.`), onError: feedback.fail });
  const extend = useMutation({ mutationFn: (items: Array<{ receipt: string; extension_seconds: number }>) => extendAdminQueueLeases(client, appID, subscriptionID, items), onSuccess: (result) => feedback.succeed(`${result.items.length} lease result${result.items.length === 1 ? "" : "s"} received.`), onError: feedback.fail });

  const parseBatch = (raw: string): unknown[] | null => { const result = validateJson(raw, 64 * 1024); if (!result.valid) { feedback.fail(new Error(result.error)); return null; } const parsed: unknown = JSON.parse(raw); if (!Array.isArray(parsed)) { feedback.fail(new Error("Batch input must be a JSON array.")); return null; } return parsed; };
  const confirmAction = () => {
    if (!confirmation) return;
    if (confirmation.kind === "drain") drain.mutate();
    if (confirmation.kind === "delete-subscription") deleteSubscription.mutate();
    if (confirmation.kind === "delete-schedule" && confirmation.schedule) deleteSchedule.mutate(confirmation.schedule.id);
    if (confirmation.kind === "delete-dlq" && confirmation.ids) deleteDeadLetters.mutate(confirmation.ids);
  };

  return <main className="page" id="main-content"><p className="eyebrow">DURABLE PULL OPERATIONS</p><h1 tabIndex={-1}>Queue</h1><p className="page-lede">Inspect subscription policy, bounded depth, recurring schedules, priority aging and generation-fenced result callbacks.</p>
    <section className="studio-controls"><label>Application<select value={appID} onChange={(event) => { setAppID(event.target.value); setSubscriptionID(""); setSelectedDeadLetters([]); }}><option value="">Select app</option>{apps.data?.filter((app) => app.enabled).map((app) => <option key={app.id} value={app.id}>{app.name}</option>)}</select></label><label>Subscription<select value={subscriptionID} onChange={(event) => setSubscriptionID(event.target.value)} disabled={!appID}><option value="">Select subscription</option>{subscriptions.data?.items.map((item) => <option key={item.id} value={item.id}>{item.name}</option>)}</select></label>
      {selected && <PermissionGate mutation><button className="quiet-button" type="button" disabled={pause.isPending || resume.isPending} onClick={() => (selected.paused_at ? resume.mutate() : pause.mutate())}>{selected.paused_at ? "Resume subscription" : "Pause subscription"}</button><button className="danger-button" type="button" disabled={Boolean(selected.draining_at) || drain.isPending} onClick={() => setConfirmation({ kind: "drain" })}>Drain subscription</button><button className="danger-button" type="button" onClick={() => setConfirmation({ kind: "delete-subscription" })}>Delete subscription</button></PermissionGate>}
    </section>
    <MutationFeedback feedback={feedback.feedback} />
    {subscriptions.isError && <AsyncState title="Queue unavailable" role="alert">{subscriptions.error.message}</AsyncState>}
    {appID && <PermissionGate mutation fallback={null}><form className="control-form" onSubmit={(event) => { event.preventDefault(); createSubscription.mutate(subscriptionInput(new FormData(event.currentTarget))); }}><h2>Create subscription</h2><label>Subscription name<input name="subscription_name" required pattern="[A-Za-z][A-Za-z0-9_.-]{0,63}" /></label><label>Max in flight<input name="max_in_flight" type="number" min="1" max="10000" defaultValue="10" /></label><label>Max batch size<input name="max_batch_size" type="number" min="1" max="100" defaultValue="10" /></label><button className="primary-button" disabled={createSubscription.isPending}>Create subscription</button></form></PermissionGate>}
    {selected && <section className="detail-grid"><article><h2>Policy</h2><dl><dt>Status</dt><dd>{selected.drained_at ? "Drained" : selected.draining_at ? "Draining" : selected.paused_at ? "Paused" : "Active"}</dd><dt>Ordering</dt><dd>{selected.ordering_mode}</dd><dt>In-flight cap</dt><dd>{selected.max_in_flight}</dd><dt>Batch cap</dt><dd>{selected.max_batch_size}</dd><dt>Fairness</dt><dd>Priority ages one class/minute to a bounded maximum.</dd></dl></article><article><h2>Depth</h2><dl><dt>Available</dt><dd>{depth.data?.available ?? "—"}</dd><dt>In flight</dt><dd>{depth.data?.in_flight ?? "—"}</dd><dt>Acknowledged</dt><dd>{depth.data?.acknowledged ?? "—"}</dd><dt>Dead letter</dt><dd>{depth.data?.dead_letter ?? "—"}</dd></dl></article></section>}
    {enabled && <><QueueActions onPull={(input) => pull.mutate(input)} onSettle={(raw) => { const parsed = parseBatch(raw); if (parsed) settle.mutate(parsed as Array<{ receipt: string; disposition: "acked" | "available" | "dead_letter"; delay_seconds?: number; reason?: string }>); }} onExtend={(raw) => { const parsed = parseBatch(raw); if (parsed) extend.mutate(parsed as Array<{ receipt: string; extension_seconds: number }>); }} settlementItems={settlementItems} setSettlementItems={setSettlementItems} leaseItems={leaseItems} setLeaseItems={setLeaseItems} />
      <ScheduleSection schedules={schedules.data?.items ?? []} onCreate={(input) => createSchedule.mutate(input)} onDelete={(schedule) => setConfirmation({ kind: "delete-schedule", schedule })} scheduleData={scheduleData} setScheduleData={setScheduleData} />
      <DeadLetterSection rows={deadLetters.data?.items ?? []} selected={selectedDeadLetters} setSelected={setSelectedDeadLetters} onReplay={() => replayDeadLetters.mutate(selectedDeadLetters)} onDelete={() => setConfirmation({ kind: "delete-dlq", ids: [...selectedDeadLetters].sort() })} onExport={() => exportDeadLetters.mutate()} pending={replayDeadLetters.isPending || deleteDeadLetters.isPending} />
      <div className="table-wrap"><table><caption>Result callback outcomes</caption><thead><tr><th>Delivery</th><th>Outcome</th><th>Status</th><th>Attempts</th><th>Reason</th></tr></thead><tbody>{callbacks.data?.items.map((item) => <tr key={item.id}><td><code>{item.delivery_id}</code></td><td>{item.outcome}</td><td>{item.status}</td><td>{item.attempts}</td><td>{item.reason ?? "—"}</td></tr>)}</tbody></table></div></>}
    {confirmation && <ConfirmationDialog title={confirmation.kind === "delete-dlq" ? "Confirm dead-letter deletion" : confirmation.kind === "delete-schedule" ? "Confirm schedule deletion" : confirmation.kind === "delete-subscription" ? "Confirm subscription deletion" : "Confirm queue drain"} confirmLabel={confirmation.kind === "delete-dlq" ? `Delete ${confirmation.ids?.length ?? 0} dead letter${confirmation.ids?.length === 1 ? "" : "s"}` : confirmation.kind === "delete-schedule" ? "Delete schedule" : confirmation.kind === "delete-subscription" ? "Delete subscription" : "Start drain"} pending={drain.isPending || deleteSubscription.isPending || deleteSchedule.isPending || deleteDeadLetters.isPending} error={feedback.feedback?.kind === "error" ? feedback.feedback.message : undefined} onCancel={() => setConfirmation(undefined)} onConfirm={confirmAction}><p>This changes durable queue state and is recorded in the Admin audit log.</p>{confirmation.ids?.map((id) => <code className="id-list" key={id}>{id}</code>)}</ConfirmationDialog>}
  </main>;
}

function QueueActions({ onPull, onSettle, onExtend, settlementItems, setSettlementItems, leaseItems, setLeaseItems }: { onPull(input: { max_messages: number; wait_seconds?: number; visibility_seconds?: number }): void; onSettle(raw: string): void; onExtend(raw: string): void; settlementItems: string; setSettlementItems(value: string): void; leaseItems: string; setLeaseItems(value: string): void }) {
  return <section className="control-form"><h2>Manual delivery actions</h2><form onSubmit={(event) => { event.preventDefault(); const data = new FormData(event.currentTarget); onPull({ max_messages: Number(data.get("max_messages") ?? 10), wait_seconds: Number(data.get("wait_seconds") ?? 0), visibility_seconds: Number(data.get("visibility_seconds") ?? 30) }); }}><label>Max messages<input name="max_messages" type="number" min="1" max="100" defaultValue="10" /></label><label>Wait seconds<input name="wait_seconds" type="number" min="0" max="30" defaultValue="0" /></label><label>Visibility seconds<input name="visibility_seconds" type="number" min="0" max="3600" defaultValue="30" /></label><button className="quiet-button" type="submit">Pull deliveries</button></form><JsonEditor label="Settlement items JSON" value={settlementItems} onChange={setSettlementItems} maxBytes={64 * 1024} /><button className="quiet-button" type="button" onClick={() => onSettle(settlementItems)}>Settle deliveries</button><JsonEditor label="Lease extensions JSON" value={leaseItems} onChange={setLeaseItems} maxBytes={64 * 1024} /><button className="quiet-button" type="button" onClick={() => onExtend(leaseItems)}>Extend leases</button></section>;
}

function ScheduleSection({ schedules, onCreate, onDelete, scheduleData, setScheduleData }: { schedules: QueueSchedule[]; onCreate(input: Parameters<typeof createAdminQueueSchedule>[3]): void; onDelete(schedule: QueueSchedule): void; scheduleData: string; setScheduleData(value: string): void }) {
  return <><form className="control-form" onSubmit={(event) => { event.preventDefault(); const data = new FormData(event.currentTarget); const raw = scheduleData; const result = validateJson(raw, 1 << 20); if (!result.valid) return; onCreate({ name: String(data.get("schedule_name") ?? "").trim(), cron_expression: String(data.get("schedule_cron") ?? "").trim(), timezone: String(data.get("schedule_timezone") ?? "UTC").trim(), event_type: String(data.get("schedule_event_type") ?? "").trim(), data: JSON.parse(raw) }); }}><h2>Create schedule</h2><label>Schedule name<input name="schedule_name" aria-label="Schedule name" required pattern="[A-Za-z][A-Za-z0-9_.-]{0,63}" /></label><label>Schedule cron<input name="schedule_cron" aria-label="Schedule cron" required placeholder="0 * * * *" /></label><label>Schedule timezone<input name="schedule_timezone" defaultValue="UTC" /></label><label>Schedule event type<input name="schedule_event_type" aria-label="Schedule event type" required /></label><JsonEditor label="Schedule data" value={scheduleData} onChange={setScheduleData} maxBytes={1 << 20} /><button className="primary-button" type="submit">Create schedule</button></form><div className="table-wrap"><table><caption>Recurring schedules</caption><thead><tr><th>Name</th><th>Cron</th><th>Timezone</th><th>Event</th><th>Next run</th><th>Status</th><th>Actions</th></tr></thead><tbody>{schedules.map((item) => <tr key={item.id}><td>{item.name}</td><td><code>{item.cron_expression}</code></td><td>{item.timezone}</td><td><code>{item.event_type}</code></td><td>{new Date(item.next_run_at).toLocaleString()}</td><td>{item.enabled ? "Enabled" : "Disabled"}</td><td><PermissionGate mutation><button className="quiet-button" type="button" onClick={() => onDelete(item)}>Delete</button></PermissionGate></td></tr>)}</tbody></table></div></>;
}

function DeadLetterSection({ rows, selected, setSelected, onReplay, onDelete, onExport, pending }: { rows: QueueDeadLetter[]; selected: string[]; setSelected(value: string[]): void; onReplay(): void; onDelete(): void; onExport(): void; pending: boolean }) {
  const toggle = (id: string) => setSelected(selected.includes(id) ? selected.filter((value) => value !== id) : selected.length < 100 ? [...selected, id] : selected);
  return <section><div className="selection-bar"><h2>Queue dead letters</h2><span>{selected.length} selected</span><PermissionGate mutation><button className="quiet-button" type="button" disabled={selected.length === 0 || pending} onClick={onReplay}>Replay selected</button><button className="danger-button" type="button" disabled={selected.length === 0 || pending} onClick={onDelete}>Delete selected dead letters ({selected.length})</button><button className="quiet-button" type="button" onClick={onExport}>Export JSON</button></PermissionGate></div>{rows.length === 0 ? <section className="empty-card"><h3>No queue dead letters</h3><p>The selected subscription has no terminal deliveries.</p></section> : <div className="table-wrap"><table><caption>Queue dead letters</caption><thead><tr><th>Select</th><th>Delivery</th><th>Event</th><th>Attempts</th><th>Generation</th><th>Reason</th><th>Updated</th></tr></thead><tbody>{rows.map((item) => <tr key={item.delivery_id}><td><input type="checkbox" aria-label={`Select dead letter ${item.delivery_id}`} checked={selected.includes(item.delivery_id)} onChange={() => toggle(item.delivery_id)} /></td><td><code>{item.delivery_id}</code></td><td><code>{item.event_id}</code></td><td>{item.attempts}</td><td>{item.generation}</td><td>{item.reason ?? "—"}</td><td>{new Date(item.updated_at).toLocaleString()}</td></tr>)}</tbody></table></div>}</section>;
}

function downloadJSON(filename: string, value: unknown) { if (typeof document === "undefined" || typeof URL.createObjectURL !== "function") return; const link = document.createElement("a"); const url = URL.createObjectURL(new Blob([JSON.stringify(value, null, 2)], { type: "application/json" })); link.href = url; link.download = filename; link.click(); URL.revokeObjectURL(url); }
