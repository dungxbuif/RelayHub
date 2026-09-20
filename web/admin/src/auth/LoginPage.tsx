import { useState } from "react";
import type { FormEvent } from "react";
import { useAuth } from "./AuthProvider";

export function LoginPage() {
  const { status, error, login } = useAuth();
  const [token, setToken] = useState("");
  async function submit(event: FormEvent) {
    event.preventDefault();
    const submitted = token;
    setToken("");
    try { await login(submitted); } catch { /* The provider exposes a redacted error. */ }
  }
  return (
    <main className="login-page" id="main-content"><section className="login-card" aria-labelledby="login-title">
      <div className="brand-mark login-mark" aria-hidden="true">RH</div><p className="eyebrow">RELAYHUB CONTROL PLANE</p>
      <h1 id="login-title">Sign in to Admin</h1><p className="lede">Exchange the bootstrap operator token for a revocable browser session. The token is never saved in browser storage.</p>
      <form onSubmit={submit} className="login-form"><label htmlFor="admin-token">Bootstrap Admin token</label>
        <input id="admin-token" type="password" autoComplete="off" value={token} onChange={(event) => setToken(event.target.value)} required />
        {error ? <p className="form-error" role="alert">{error}</p> : null}
        <button className="primary-button" type="submit" disabled={status === "authenticating"}>{status === "authenticating" ? "Signing in…" : "Sign in"}</button>
      </form>
    </section></main>
  );
}
