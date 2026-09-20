export type AdminUserPrincipal = { id: string; email: string; role: "admin" | "user" };
export type AdminSessionResponse = { csrf_token: string; expires_at: string; user: AdminUserPrincipal };
export type ErrorEnvelope = { error?: { code?: string; message?: string } };
