import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import type { PropsWithChildren } from "react";
import { AdminApiClient, ApiError, redactText } from "../api/client";
import type { AdminSessionResponse } from "../api/types";

type AuthStatus = "unauthenticated" | "authenticating" | "authenticated";
type AuthContextValue = { status: AuthStatus; error: string; client: AdminApiClient; login(email: string, password: string): Promise<void>; logout(): Promise<void> };
const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: PropsWithChildren) {
  const [status, setStatus] = useState<AuthStatus>("authenticating");
  const [error, setError] = useState("");
  const csrfRef = useRef("");
  const invalidate = useCallback(() => { csrfRef.current = ""; setStatus("unauthenticated"); }, []);
  const client = useMemo(() => new AdminApiClient(() => csrfRef.current, invalidate), [invalidate]);

  const acceptSession = useCallback((session: AdminSessionResponse) => {
    if (!session.csrf_token || !session.expires_at || !session.user?.email) {
      throw new ApiError(502, "invalid_response", "RelayHub returned an invalid Admin session.");
    }
    csrfRef.current = session.csrf_token;
    setStatus("authenticated");
  }, []);

  useEffect(() => {
    let cancelled = false;
    client.request<AdminSessionResponse>("/api/v1/admin/session")
      .then((session) => { if (!cancelled) acceptSession(session); })
      .catch(() => { if (!cancelled) invalidate(); });
    return () => { cancelled = true; };
  }, [acceptSession, client, invalidate]);

  const login = useCallback(async (email: string, password: string) => {
    setStatus("authenticating"); setError("");
    try {
      const session = await client.request<AdminSessionResponse>("/api/v1/admin/session", { method: "POST", body: { email, password } });
      acceptSession(session);
    } catch (caught) {
      invalidate(); setError(caught instanceof Error ? redactText(caught.message) : "Authentication failed."); throw caught;
    }
  }, [acceptSession, client, invalidate]);
  const logout = useCallback(async () => { try { await client.request("/api/v1/admin/session", { method: "DELETE" }); } finally { invalidate(); } }, [client, invalidate]);
  const value = useMemo(() => ({ status, error, client, login, logout }), [status, error, client, login, logout]);
  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  const value = useContext(AuthContext);
  if (!value) throw new Error("useAuth must be used inside AuthProvider");
  return value;
}
