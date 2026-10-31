import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { listApps, deleteAdminFunction, invokeAdminFunction, listAdminFunctions, registerAdminFunction, updateAdminFunction, type App, type FunctionDefinition } from "../api/controlPlane";
import { useAuth } from "../auth/AuthProvider";
import { PermissionGate } from "../auth/PermissionGate";
import { AsyncState } from "../components/AsyncState";
import { ResourceSelector } from "../components/ResourceSelector";

export function FunctionsPage() {
  const { client } = useAuth();
  const cache = useQueryClient();
  const apps = useQuery({ queryKey: ["admin-apps", "functions"], queryFn: ({ signal }) => listApps(client, signal) });
  const [appID, setAppID] = useState("");
  const [name, setName] = useState("");
  const [timeoutSeconds, setTimeoutSeconds] = useState("10");
  const [message, setMessage] = useState("");
  useEffect(() => { if (!appID && apps.data?.[0]) setAppID(apps.data[0].id); }, [appID, apps.data]);
  const functions = useQuery({ queryKey: ["admin-functions", appID], queryFn: ({ signal }) => listAdminFunctions(client, appID, signal), enabled: Boolean(appID) });
  const refresh = () => cache.invalidateQueries({ queryKey: ["admin-functions", appID] });
  const register = useMutation({ mutationFn: () => registerAdminFunction(client, appID, { name: name.trim(), timeout_seconds: Number(timeoutSeconds) }), onSuccess: async () => { setName(""); setMessage("Function registered."); await refresh(); } });
  const update = useMutation({ mutationFn: ({ id, enabled }: { id: string; enabled: boolean }) => updateAdminFunction(client, appID, id, { enabled }), onSuccess: refresh });
  const remove = useMutation({ mutationFn: (id: string) => deleteAdminFunction(client, appID, id), onSuccess: refresh });
  const invoke = useMutation({ mutationFn: (id: string) => invokeAdminFunction(client, appID, id, {}), onSuccess: (result) => setMessage(result.ok ? "Invocation completed" : "Invocation returned a handler error.") });
  const selectedApp = apps.data?.find((app) => app.id === appID);
  return <main className="page" id="main-content"><p className="eyebrow">FUNCTIONS</p><h1 tabIndex={-1}>Functions</h1><p className="page-lede">Register bounded application functions, inspect their state and invoke them through the authenticated Admin boundary.</p>
    {apps.isPending && <AsyncState title="Loading applications">Reading applications…</AsyncState>}{apps.isError && <AsyncState title="Applications unavailable" role="alert">{apps.error.message}</AsyncState>}
    {apps.data && <ResourceSelector label="Application" value={appID} options={apps.data.filter((app) => app.enabled).map((app) => ({ id: app.id, label: app.name }))} onChange={setAppID} />}
    {selectedApp && <PermissionGate mutation fallback={<p className="status-card" role="status">This session is read-only. You can inspect functions but cannot register or invoke them.</p>}><form className="control-form" onSubmit={(event) => { event.preventDefault(); register.mutate(); }}><h2>Register function</h2><label>Function name<input aria-label="Function name" value={name} onChange={(event) => setName(event.target.value)} required maxLength={64} /></label><label>Timeout seconds<input type="number" min={1} max={30} value={timeoutSeconds} onChange={(event) => setTimeoutSeconds(event.target.value)} required /></label><button className="primary-button" disabled={register.isPending}>{register.isPending ? "Registering…" : "Register function"}</button></form></PermissionGate>}
    {message && <p className="success-banner" role="status">{message}</p>}{register.error && <p className="form-error" role="alert">{register.error.message}</p>}{functions.isPending && <AsyncState title="Loading functions">Reading function catalog…</AsyncState>}{functions.isError && <AsyncState title="Functions unavailable" role="alert">{functions.error.message}</AsyncState>}
    {functions.data && <FunctionTable functions={functions.data} onToggle={(id, enabled) => update.mutate({ id, enabled })} onDelete={(id) => remove.mutate(id)} onInvoke={(id) => invoke.mutate(id)} />}
  </main>;
}

function FunctionTable({ functions, onToggle, onDelete, onInvoke }: { functions: FunctionDefinition[]; onToggle(id: string, enabled: boolean): void; onDelete(id: string): void; onInvoke(id: string): void }) {
  return <div className="table-wrap"><table><caption>Application functions</caption><thead><tr><th scope="col">Name</th><th scope="col">Timeout</th><th scope="col">Status</th><th scope="col">Actions</th></tr></thead><tbody>{functions.map((item) => <tr key={item.id}><td><code>{item.name}</code></td><td>{item.timeout_seconds}s</td><td>{item.enabled ? "Enabled" : "Disabled"}</td><td className="action-cell"><PermissionGate mutation fallback={null}><button className="quiet-button" type="button" onClick={() => onToggle(item.id, !item.enabled)}>{item.enabled ? "Disable" : "Enable"}</button><button className="quiet-button" type="button" disabled={!item.enabled} onClick={() => onInvoke(item.id)}>Invoke {item.name}</button><button className="quiet-button" type="button" onClick={() => onDelete(item.id)}>Delete</button></PermissionGate></td></tr>)}</tbody></table></div>;
}
