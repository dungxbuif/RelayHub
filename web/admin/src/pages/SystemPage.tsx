import { getDashboard } from "../api/adminReads";
import { redactText } from "../api/client";
import { useAuth } from "../auth/AuthProvider";
import { AsyncState } from "../components/AsyncState";
import { usePausableRefresh } from "../hooks/usePausableRefresh";

export function SystemPage() {
  const { client } = useAuth();
  const system = usePausableRefresh(["admin-system"], (signal) => getDashboard(client, signal));
  return <main className="page" id="main-content"><p className="eyebrow">PLATFORM HEALTH</p><div className="page-heading"><div><h1 tabIndex={-1}>System</h1><p className="page-lede">Inspect current dependency and replica state reported by the control plane.</p></div><button className="quiet-button" type="button" onClick={system.togglePaused}>{system.paused ? "Resume refresh" : "Pause refresh"}</button></div>
    {system.isPending && <AsyncState title="Loading system health">Reading current cluster state…</AsyncState>}
    {system.isError && <AsyncState title="System health unavailable" role="alert">{redactText(system.error instanceof Error ? system.error.message : "The system health request failed.")}</AsyncState>}
    {system.data && <SystemContent data={system.data} />}
  </main>;
}

function SystemContent({ data }: { data: Awaited<ReturnType<typeof getDashboard>> }) {
  const degraded = data.degraded_components;
  return <>
    <section className="status-card" aria-label="System status"><span className="status-indicator" /><div><h2>{degraded.length > 0 ? "Degraded" : "Healthy"}</h2><p>{degraded.length > 0 ? "Some dependencies are unavailable or reporting stale state." : "All reported dependencies are healthy."}</p></div></section>
    <section className="chart-card"><h2>Degraded components</h2>{degraded.length === 0 ? <p className="muted">No degraded components reported.</p> : <ul>{degraded.map((component) => <li key={component}><code>{component}</code></li>)}</ul>}</section>
    <section className="table-wrap"><table><caption>API replicas</caption><thead><tr><th scope="col">Instance</th><th scope="col">Connections</th><th scope="col">Realtime broker</th><th scope="col">Heartbeat</th></tr></thead><tbody>{data.instances.map((instance) => <tr key={instance.instance_id}><td><code>{instance.instance_id}</code></td><td>{instance.connections}</td><td>{instance.nats_connected ? "Connected" : "Degraded"}</td><td>{new Date(instance.heartbeat_at).toLocaleString()}</td></tr>)}</tbody></table></section>
  </>;
}
