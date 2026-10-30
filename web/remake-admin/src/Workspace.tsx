import { useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { api, ApiError, deliveryState, encodeBody, errorMessage, workspacePath, type Accepted, type AgentPresence, type Application, type CreatedEndpoint, type Credential, type Endpoint, type EndpointPolicy, type EventInput, type EventStatus } from "./api";

export function SecretDialog({ secret, onClose }: { secret: string; onClose(): void }) {
  const dialog = useRef<HTMLDialogElement>(null);
  const [stored, setStored] = useState(false);
  useEffect(() => { dialog.current?.showModal(); }, []);
  return <dialog ref={dialog} aria-labelledby="secret-title" onCancel={event => event.preventDefault()}>
    <h2 id="secret-title">Store secret securely</h2>
    <p>Displayed once. Copy this value into your secret manager. It cannot be retrieved later.</p>
    <pre>{secret}</pre>
    <label><input autoFocus type="checkbox" checked={stored} onChange={event => setStored(event.target.checked)} /> I stored this secret securely.</label>
    <button disabled={!stored} onClick={onClose}>Close secret</button>
  </dialog>;
}

export function Workspace({ id }: { id: string }) {
  const path = workspacePath(id);
  const [apps, setApps] = useState<Application[]>([]);
  const [endpoints, setEndpoints] = useState<Endpoint[]>([]);
  const [agents, setAgents] = useState<AgentPresence[]>([]);
  const [appId, setAppId] = useState("");
  const [endpointId, setEndpointId] = useState("");
  const [policy, setPolicy] = useState<EndpointPolicy>({ mode: "async" });
  const [loading, setLoading] = useState(true);
  const [denied, setDenied] = useState(false);
  const [busy, setBusy] = useState(false);
  const [secret, setSecret] = useState("");
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [channel, setChannel] = useState("dashboard-test");
  const [payload, setPayload] = useState("Hello from RelayHub");
  const [accepted, setAccepted] = useState<Accepted | null>(null);
  const pendingTest = useRef<{ input: string; key: string } | null>(null);
  useEffect(() => {
    let active = true;
    async function load() {
      try {
        const applications = await api.request<Application[]>(`${path}/apps`);
        const [endpoints, agents] = await Promise.all([
          api.request<Endpoint[]>("/dashboard/endpoints"), api.request<AgentPresence[]>("/dashboard/agents"),
        ]);
        if (!active) return;
        setApps(applications); setAppId(applications[0]?.id ?? "");
        setEndpoints(endpoints); setEndpointId(endpoints[0]?.id ?? "");
        setPolicy(endpoints[0]?.policy ?? { mode: "async" }); setAgents(agents);
      } catch (error) { if (active) { setError(errorMessage(error)); setDenied(true); } }
      finally { if (active) setLoading(false); }
    }
    void load(); return () => { active = false; };
  }, [path]);
  async function run(action: () => Promise<void>) {
    setBusy(true); setError(""); setMessage("");
    try { await action(); }
    catch (error) {
      setError(errorMessage(error));
      if (error instanceof ApiError && [401, 403].includes(error.status)) { setDenied(true); setSecret(""); }
    } finally { setBusy(false); }
  }
  const blocked = busy || Boolean(secret) || !api.canWrite();
  async function refreshStatus(eventId: string) {
    const status = await api.request<EventStatus>(`/dashboard/events/${encodeURIComponent(eventId)}`);
    setMessage(`Durable event ${status.event_id}: ${deliveryState(status.state)} · generation ${status.generation} · attempts ${status.attempts}`);
  }
  if (loading) return <p>Loading workspace…</p>;
  if (denied) return <p role="alert">{error}</p>;
  return <>
    <h1>Workspace</h1><p>{id}</p><Link to={`${path}/deliveries`}>Deliveries</Link>
    {error && <p role="alert">{error}</p>}{message && <p role="status">{message}</p>}
    <section aria-labelledby="apps-title"><h2 id="apps-title">Apps and credentials</h2>
      <form onSubmit={event => {
        event.preventDefault(); const form = event.currentTarget;
        const name = String(new FormData(form).get("name")).trim();
        void run(async () => {
          const app = await api.request<Application>(`${path}/apps`, "POST", { name });
          setApps(previous => [...previous.filter(item => item.id !== app.id), app]); setAppId(app.id); form.reset(); setMessage("App created.");
        });
      }}>
        <label>App name<input name="name" required maxLength={200} /></label>
        <button disabled={blocked}>Create app</button>
      </form>
      <form onSubmit={event => {
        event.preventDefault(); const kind = String(new FormData(event.currentTarget).get("kind"));
        void run(async () => {
          const credential = await api.request<Credential>(`${path}/credentials`, "POST", { app_id: appId, kind });
          setSecret(credential.token);
          setMessage(`Credential created for app ${credential.app_id}${credential.agent_id ? ` · agent ${credential.agent_id}` : ""}.`);
        });
      }}>
        <label>Application<select value={appId} required onChange={event => setAppId(event.target.value)}>{apps.map(app => <option key={app.id} value={app.id}>{app.name}</option>)}</select></label>
        <label>Credential kind<select name="kind"><option value="application">Application</option><option value="agent">Agent</option></select></label>
        <button disabled={blocked || !appId}>Create credential</button>
      </form>
    </section>
    <section aria-labelledby="endpoints-title"><h2 id="endpoints-title">HTTP webhook endpoints</h2>
      <p>New endpoints use a shared bearer secret and asynchronous delivery.</p>
      <button disabled={blocked || !appId} onClick={() => void run(async () => {
        const ingressSecret = crypto.randomUUID();
        const endpoint = await api.request<CreatedEndpoint>("/dashboard/endpoints", "POST", { app_id: appId, policy: { mode: "async" }, auth: { kind: "bearer", secret: ingressSecret } });
        // Store only public configuration; the ingress secret lives in the dialog.
        const config: Endpoint = { id: endpoint.id, app_id: endpoint.app_id, policy: endpoint.policy, auth: endpoint.auth };
        setEndpoints(previous => [...previous.filter(item => item.id !== config.id), config]);
        setEndpointId(config.id); setPolicy(config.policy);
        setSecret(ingressSecret);
        setMessage("Endpoint created.");
      })}>Create endpoint</button>
      {endpoints.length > 0 && <form onSubmit={event => {
        event.preventDefault(); void run(async () => {
          if (policy.mode === "sync" && (!Number.isInteger(policy.timeout_seconds) || policy.timeout_seconds < 1 || policy.timeout_seconds > 30)) return;
          const saved = await api.request<Endpoint>(`/dashboard/endpoints/${encodeURIComponent(endpointId)}`, "PATCH", { policy });
          setEndpoints(previous => previous.map(item => item.id === endpointId ? saved : item)); setPolicy(saved.policy); setMessage("Policy saved.");
        });
      }}>
        <label>Endpoint<select value={endpointId} disabled={busy} onChange={event => {
          setEndpointId(event.target.value); setPolicy(endpoints.find(item => item.id === event.target.value)!.policy);
        }}>{endpoints.map(endpoint => <option key={endpoint.id} value={endpoint.id}>{endpoint.id}</option>)}</select></label>
        <p>Ingress path: <code>/hooks/{endpointId}</code>. Authentication is independent of app keys.</p>
        <label>Webhook mode<select value={policy.mode} disabled={busy} onChange={event => setPolicy(event.target.value === "async" ? { mode: "async" } : { mode: "sync", timeout_seconds: 15, offline: "queue", timeout: "continue" })}><option value="async">Async</option><option value="sync">Sync</option></select></label>
        {policy.mode === "sync" && <>
          <label>Sync timeout (seconds)<input type="number" min={1} max={30} required value={policy.timeout_seconds} disabled={busy} onChange={event => setPolicy({ ...policy, timeout_seconds: event.target.valueAsNumber })} /></label>
          <label>Offline policy<select value={policy.offline} disabled={busy} onChange={event => setPolicy({ ...policy, offline: event.target.value as "queue" | "reject" })}><option value="queue">Queue · 202 after commit</option><option value="reject">Reject · 503, no enqueue</option></select></label>
          <label>Timeout policy<select value={policy.timeout} disabled={busy} onChange={event => setPolicy({ ...policy, timeout: event.target.value as "continue" | "expire_pending" })}><option value="continue">Continue · 202</option><option value="expire_pending">Expire pending · 504</option></select></label>
          <p>Expire pending cancels only unclaimed work. Claimed work may still complete.</p>
        </>}
        <button disabled={blocked}>Save policy</button>
      </form>}
    </section>
    <section aria-labelledby="agents-title"><h2 id="agents-title">Agent presence</h2>
      <p>Presence is ephemeral; a connected agent is not proof of processing.</p>
      {agents.length === 0 ? <p>No agents observed.</p> : <ul>{agents.map(agent => <li key={agent.agent_id}>{agent.agent_id} · <strong>{agent.online ? "Online" : "Offline"}</strong></li>)}</ul>}
      <button disabled={busy} onClick={() => void run(async () => setAgents(await api.request<AgentPresence[]>(`${path}/agents`)))}>Refresh presence</button>
    </section>
    <section aria-labelledby="tests-title"><h2 id="tests-title">Test messages</h2>
      <label>Test payload<textarea value={payload} maxLength={1_048_576} onChange={event => setPayload(event.target.value)} /></label>
      <h3>Realtime · ephemeral</h3>
      <label>Realtime channel<input value={channel} maxLength={200} onChange={event => setChannel(event.target.value)} /></label>
      <button disabled={blocked || !channel.trim()} onClick={() => void run(async () => {
        const receipt = await api.realtime(id, appId, channel, payload);
        setMessage(`Realtime: ${receipt.observed_recipients} recipients observed; not a processing ACK. No offline replay.`);
      })}>Send realtime test</button>
      <h3>Durable · at least once</h3>
      <p>Uses the selected endpoint, normal persistence and agent ACK. Consumer side effects require event_id idempotency.</p>
      <button disabled={blocked || !endpointId} onClick={() => void run(async () => {
        const input: EventInput = { endpoint_id: endpointId, event_type: "dashboard.test", method: "POST", path: "/", headers: [], body: encodeBody(payload) };
        const serialized = JSON.stringify(input);
        // An uncertain acceptance must retry with the same idempotency key.
        if (pendingTest.current?.input !== serialized) pendingTest.current = { input: serialized, key: crypto.randomUUID() };
        const receipt = await api.request<Accepted>("/dashboard/events", "POST", input, { "Idempotency-Key": pendingTest.current.key });
        pendingTest.current = null;
        setAccepted(receipt); await refreshStatus(receipt.event_id);
      })}>Send durable test</button>
      {accepted && <button disabled={busy} onClick={() => void run(() => refreshStatus(accepted.event_id))}>Refresh durable status</button>}
    </section>
    {secret && <SecretDialog secret={secret} onClose={() => setSecret("")} />}
  </>;
}
