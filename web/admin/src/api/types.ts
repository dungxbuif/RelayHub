export type AdminSessionResponse = { csrf_token: string; expires_at: string };
export type ErrorEnvelope = { error?: { code?: string; message?: string } };
