import { useEffect, useState, type ReactNode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter, Link, Navigate, Route, Routes, useNavigate, useParams } from "react-router-dom";
import { api, errorMessage, type Principal, type ProvisionedUser } from "./api";
import { Workspace, SecretDialog } from "./Workspace";
import { Deliveries } from "./Deliveries";

function Login({ onLogin }: { onLogin(principal: Principal): void }) {
  const navigate = useNavigate();
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  return <form onSubmit={async event => {
    event.preventDefault();
    const form = event.currentTarget;
    const data = new FormData(form);
    setBusy(true); setError("");
    try {
      const principal = await api.login(String(data.get("email")), String(data.get("password")));
      form.reset(); onLogin(principal);
      navigate(principal.kind === "system_admin" ? "/admin/users" : principal.kind === "workspace_owner" ? `/workspaces/${principal.workspace_id}` : "/login", { replace: true });
    } catch (error) { setError(errorMessage(error)); }
    finally { setBusy(false); }
  }}>
    <h1>Sign in</h1>
    <label>Email<input name="email" type="email" autoComplete="username" required maxLength={254} /></label>
    <label>Password<input name="password" type="password" autoComplete="current-password" required maxLength={1024} /></label>
    {error && <p role="alert">{error}</p>}
    <button disabled={busy}>Sign in</button>
  </form>;
}

function Users() {
  const navigate = useNavigate();
  const [secret, setSecret] = useState("");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  return <>
    <h1>Users</h1>
    <form onSubmit={async event => {
      event.preventDefault(); const form = event.currentTarget;
      const email = String(new FormData(form).get("email"));
      setBusy(true); setError(""); setMessage("");
      try {
        const user = await api.request<ProvisionedUser>("/admin/users", "POST", { email });
        setSecret(user.setup_token); setMessage(`User created. Workspace: ${user.workspace_id}. Setup token expires in ${user.expires_in_seconds / 3600} hours.`); form.reset();
      } catch (error) { setError(errorMessage(error)); }
      finally { setBusy(false); }
    }}>
      <label>New user email<input type="email" name="email" required maxLength={254} /></label>
      <button disabled={busy || Boolean(secret) || !api.canWrite()}>Create user</button>
    </form>
    <form onSubmit={event => {
      event.preventDefault(); const id = String(new FormData(event.currentTarget).get("workspace"));
      navigate(`/workspaces/${encodeURIComponent(id)}`);
    }}>
      <label>Workspace ID<input name="workspace" required pattern="[0-9a-fA-F-]{36}" /></label>
      <button>Open workspace</button>
    </form>
    {message && <p role="status">{message}</p>}{error && <p role="alert">{error}</p>}
    {secret && <SecretDialog secret={secret} onClose={() => setSecret("")} />}
  </>;
}

function WorkspaceRoute({ deliveries = false }: { deliveries?: boolean }) {
  const { id = "" } = useParams();
  // Remount on workspace changes so prior data, errors and secrets cannot linger.
  return deliveries ? <Deliveries key={id} id={id} /> : <Workspace key={id} id={id} />;
}
export function App() {
  const [principal, setPrincipal] = useState<Principal | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  useEffect(() => {
    let active = true;
    api.request<Principal>("/session").then(value => { if (active) setPrincipal(value); }).catch(() => {}).finally(() => { if (active) setLoading(false); });
    return () => { active = false; };
  }, []);
  if (loading) return <main><p>Loading session…</p></main>;
  const signedIn = principal?.kind === "system_admin" || principal?.kind === "workspace_owner";
  function protect(content: ReactNode, adminOnly = false) {
    if (!signedIn) return <Navigate to="/login" replace />;
    if (adminOnly && principal?.kind !== "system_admin") return <p role="alert">403 — Access denied.</p>;
    return content;
  }
  return <main>
    <header><strong>RelayHub · Workspace</strong><nav aria-label="Main navigation">
      <Link to="/login">Sign in</Link>
      {principal?.kind === "system_admin" && <Link to="/admin/users">Users</Link>}
      {principal?.kind === "workspace_owner" && <Link to={`/workspaces/${principal.workspace_id}`}>My workspace</Link>}
      {signedIn && api.canWrite() && <button onClick={async () => {
        try { await api.logout(); setPrincipal(null); setError(""); }
        catch (error) { setError(errorMessage(error)); }
      }}>Sign out</button>}
    </nav></header>
    {signedIn && !api.canWrite() && <p><Link to="/login">Sign in to enable changes</Link>.</p>}
    {error && <p role="alert">{error}</p>}
    <Routes>
      <Route path="/login" element={<Login onLogin={setPrincipal} />} />
      <Route path="/admin/users" element={protect(<Users />, true)} />
      <Route path="/workspaces/:id" element={protect(<WorkspaceRoute />)} />
      <Route path="/workspaces/:id/deliveries" element={protect(<WorkspaceRoute deliveries />)} />
      <Route path="*" element={<Navigate to="/login" replace />} />
    </Routes>
  </main>;
}
createRoot(document.getElementById("root")!).render(<BrowserRouter><App /></BrowserRouter>);
