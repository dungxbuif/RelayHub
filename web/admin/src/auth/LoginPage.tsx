import { useState } from "react";
import type { FormEvent } from "react";
import { useAuth } from "./AuthProvider";

export function LoginPage() {
  const { status, error, login } = useAuth();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  async function submit(event: FormEvent) {
    event.preventDefault();
    const submittedEmail = email;
    const submittedPassword = password;
    setPassword("");
    try { await login(submittedEmail, submittedPassword); } catch { /* The provider exposes a redacted error. */ }
  }
  return (
    <main className="login-page" id="main-content"><section className="login-card" aria-labelledby="login-title">
      <div className="brand-mark login-mark" aria-hidden="true">RH</div><p className="eyebrow">RELAYHUB CONTROL PLANE</p>
      <h1 id="login-title">Sign in to Admin</h1><p className="lede">Use your Admin account to open a revocable browser session. Credentials are never saved in browser storage.</p>
      <form onSubmit={submit} className="login-form"><label htmlFor="admin-email">Email</label>
        <input id="admin-email" type="email" autoComplete="username" value={email} onChange={(event) => setEmail(event.target.value)} required />
        <label htmlFor="admin-password">Password</label>
        <input id="admin-password" type="password" autoComplete="current-password" value={password} onChange={(event) => setPassword(event.target.value)} required />
        {error ? <p className="form-error" role="alert">{error}</p> : null}
        <button className="primary-button" type="submit" disabled={status === "authenticating"}>{status === "authenticating" ? "Signing in…" : "Sign in"}</button>
      </form>
    </section></main>
  );
}
