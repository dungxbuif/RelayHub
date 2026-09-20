import type { JSONValue, RealtimeEncryptionEnvelope, RealtimeEncryptionKeyProvider } from "../types.js";

const keyIdPattern = /^[A-Za-z0-9_.:-]{1,128}$/;

export async function encryptRealtimePayload(provider: RealtimeEncryptionKeyProvider, channel: string, data: Record<string, JSONValue>): Promise<RealtimeEncryptionEnvelope> {
  if (!privateChannel(channel)) throw new TypeError("encryption requires a private realtime channel");
  if (!data || typeof data !== "object" || Array.isArray(data)) throw new TypeError("invalid realtime encryption payload");
  const plaintext = new TextEncoder().encode(JSON.stringify(data));
  if (plaintext.byteLength > 48 * 1024) throw new TypeError("invalid realtime encryption payload");
  const selected = await provider.encryptionKey(channel);
  validateKey(selected.key);
  if (!keyIdPattern.test(selected.keyId)) throw new TypeError("invalid realtime encryption key ID");
  const nonce = crypto.getRandomValues(new Uint8Array(12));
  const key = await crypto.subtle.importKey("raw", selected.key, "AES-GCM", false, ["encrypt"]);
  const ciphertext = await crypto.subtle.encrypt({name: "AES-GCM", iv: nonce, additionalData: aad(channel, selected.keyId)}, key, plaintext);
  return {algorithm: "aes-256-gcm", key_id: selected.keyId, nonce: encodeBase64Url(nonce), ciphertext: encodeBase64Url(new Uint8Array(ciphertext))};
}

export async function decryptRealtimeEnvelope(provider: RealtimeEncryptionKeyProvider, channel: string, envelope: RealtimeEncryptionEnvelope): Promise<Record<string, JSONValue>> {
  validateEnvelope(channel, envelope);
  const rawKey = await provider.decryptionKey(channel, envelope.key_id);
  validateKey(rawKey);
  try {
    const key = await crypto.subtle.importKey("raw", rawKey, "AES-GCM", false, ["decrypt"]);
    const plaintext = await crypto.subtle.decrypt({name: "AES-GCM", iv: decodeBase64Url(envelope.nonce), additionalData: aad(channel, envelope.key_id)}, key, decodeBase64Url(envelope.ciphertext));
    const value: unknown = JSON.parse(new TextDecoder().decode(plaintext));
    if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("invalid payload");
    return value as Record<string, JSONValue>;
  } catch {
    throw new TypeError("realtime payload decryption failed");
  }
}

export function validateRealtimeEncryptionEnvelope(channel: string, envelope: RealtimeEncryptionEnvelope): void {
  validateEnvelope(channel, envelope);
}

function validateEnvelope(channel: string, envelope: RealtimeEncryptionEnvelope): void {
  if (!privateChannel(channel) || envelope.algorithm !== "aes-256-gcm" || !keyIdPattern.test(envelope.key_id)) throw new TypeError("invalid realtime encryption envelope");
  const nonce = decodeBase64Url(envelope.nonce);
  const ciphertext = decodeBase64Url(envelope.ciphertext);
  if (nonce.byteLength !== 12 || ciphertext.byteLength < 16 || ciphertext.byteLength > 48 * 1024) throw new TypeError("invalid realtime encryption envelope");
}

function privateChannel(channel: string): boolean { return channel.startsWith("private:") && channel.length > 8; }
function validateKey(key: Uint8Array): void { if (!(key instanceof Uint8Array) || key.byteLength !== 32) throw new TypeError("realtime encryption keys must be 32 bytes"); }
function aad(channel: string, keyId: string): Uint8Array { return new TextEncoder().encode(`${channel}\n${keyId}`); }
function encodeBase64Url(value: Uint8Array): string {
  let binary = "";
  for (const byte of value) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
function decodeBase64Url(value: string): Uint8Array {
  if (!/^[A-Za-z0-9_-]+$/.test(value)) throw new TypeError("invalid realtime encryption envelope");
  const padded = value.replace(/-/g, "+").replace(/_/g, "/") + "=".repeat((4 - value.length % 4) % 4);
  const binary = atob(padded);
  return Uint8Array.from(binary, character => character.charCodeAt(0));
}
