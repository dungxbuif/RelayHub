import { createHash, createHmac } from "node:crypto";

export interface SigningInput { secret: string; timestamp: string; method: string; requestTarget: string; body: Uint8Array }

export function canonicalRequest(input: SigningInput): string {
  const bodyHash = createHash("sha256").update(input.body).digest("hex");
  return `${input.timestamp}\n${input.method.toUpperCase()}\n${input.requestTarget}\n${bodyHash}`;
}

export function signRequest(input: SigningInput): string {
  return createHmac("sha256", input.secret).update(canonicalRequest(input)).digest("hex");
}
