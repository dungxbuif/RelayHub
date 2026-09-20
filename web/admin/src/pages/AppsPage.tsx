import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { createApp, disableApp, listApps, rotateApp, updateApp, type App, type AppInput, type Credentials } from "../api/controlPlane";
import { useAuth } from "../auth/AuthProvider";
import { AsyncState } from "../components/AsyncState";
import { CredentialsDialog } from "../components/CredentialsDialog";

export function AppsPage() {
  const { client } = useAuth(); const cache = useQueryClient(); const [editing, setEditing] = useState<App>(); const [credentials, setCredentials] = useState<Credentials>(); const query = useQuery({ queryKey: ["admin-apps"], queryFn: ({ signal }) => listApps(client, signal) });
  const refresh = () => cache.invalidateQueries({ queryKey: ["admin-apps"] });
  const create = useMutation({ mutationFn: (input: AppInput) => createApp(client, input), onSuccess: async (value) => { setCredentials(value); await refresh(); } });
  const update = useMutation({ mutationFn: ({ id, input }: { id: string; input: Partial<AppInput> }) => updateApp(client, id, input), onSuccess: async () => { setEditing(undefined); await refresh(); } });
  const disable = useMutation({ mutationFn: (id: string) => disableApp(client, id), onSuccess: refresh }); const rotate = useMutation({ mutationFn: (id: string) => rotateApp(client, id), onSuccess: setCredentials });
  return <main className="page" id="main-content"><p className="eyebrow">CONTROL PLANE</p><h1 tabIndex={-1}>Apps</h1><p className="page-lede">Provision integrations and manage delivery configuration. Credentials are shown once.</p><AppForm title="Create app" pending={create.isPending} onSubmit={(input) => create.mutate(input)} />
    {create.error && <p className="form-error" role="alert">{create.error.message}</p>}{query.isPending && <AsyncState title="Loading apps">Reading applications…</AsyncState>}{query.isError && <AsyncState title="Apps unavailable" role="alert">{query.error.message}</AsyncState>}
    {query.data && <div className="table-wrap"><table><caption>Applications</caption><thead><tr><th>Name</th><th>ID</th><th>Mode</th><th>Status</th><th>Actions</th></tr></thead><tbody>{query.data.map((app) => <tr key={app.id}><td>{app.name}</td><td><code>{app.id}</code></td><td>{app.delivery_mode}</td><td>{app.enabled ? "Enabled" : "Disabled"}</td><td className="action-cell"><button className="quiet-button" onClick={() => setEditing(app)}>Edit</button><button className="quiet-button" disabled={!app.enabled} onClick={() => rotate.mutate(app.id)}>Rotate</button><button className="quiet-button" disabled={!app.enabled} onClick={() => disable.mutate(app.id)}>Disable</button></td></tr>)}</tbody></table></div>}
    {editing && <div className="dialog-backdrop"><section className="confirmation-dialog" role="dialog" aria-modal="true" aria-label="Edit application"><AppForm title={`Edit ${editing.name}`} initial={editing} pending={update.isPending} onCancel={() => setEditing(undefined)} onSubmit={(input) => update.mutate({ id: editing.id, input })} /></section></div>}{credentials && <CredentialsDialog credentials={credentials} onClose={() => setCredentials(undefined)} />}
  </main>;
}

function AppForm({ title, initial, pending, onSubmit, onCancel }: { title: string; initial?: App; pending: boolean; onSubmit(input: AppInput): void; onCancel?(): void }) {
  return <form className="control-form" onSubmit={(event) => { event.preventDefault(); const data = new FormData(event.currentTarget); const callback = String(data.get("callback_url") ?? "").trim(); onSubmit({ name: String(data.get("name") ?? "").trim(), delivery_mode: String(data.get("delivery_mode")) as AppInput["delivery_mode"], callback_url: callback || null }); }}><h2>{title}</h2><label>Name<input name="name" required maxLength={128} defaultValue={initial?.name} /></label><label>Delivery mode<select name="delivery_mode" defaultValue={initial?.delivery_mode ?? "queue"}><option value="queue">Queue</option><option value="websocket">WebSocket</option><option value="callback">Callback</option><option value="all">All</option></select></label><label>Callback URL<input name="callback_url" type="url" defaultValue={initial?.callback_url} placeholder="https://receiver.example/events" /></label><div className="form-actions">{onCancel && <button className="quiet-button" type="button" onClick={onCancel}>Cancel</button>}<button className="primary-button" disabled={pending}>{pending ? "Saving…" : "Save app"}</button></div></form>;
}
