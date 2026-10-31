import { useId, useState } from "react";

export function validateJson(value: string, maxBytes: number): { valid: boolean; error?: string } {
  const bytes = new TextEncoder().encode(value).byteLength;
  if (bytes > maxBytes) return { valid: false, error: `JSON must be no larger than ${maxBytes} bytes.` };
  try {
    const parsed: unknown = JSON.parse(value);
    if (parsed === null || typeof parsed !== "object") return { valid: false, error: "JSON must be an object or array." };
    return { valid: true };
  } catch {
    return { valid: false, error: "Enter valid JSON." };
  }
}

export function JsonEditor({ label, value, onChange, maxBytes = 64 * 1024 }: { label: string; value: string; onChange(value: string): void; maxBytes?: number }) {
  const id = useId();
  const [tooLarge, setTooLarge] = useState(false);
  const result = tooLarge ? { valid: false, error: `JSON must be no larger than ${maxBytes} bytes.` } : validateJson(value, maxBytes);
  const bytes = new TextEncoder().encode(value).byteLength;
  return <div className="json-editor"><label htmlFor={id}>{label}</label>
    <textarea id={id} aria-invalid={!result.valid} value={value} onChange={(event) => {
      const next = event.target.value;
      const exceeds = new TextEncoder().encode(next).byteLength > maxBytes;
      setTooLarge(exceeds);
      if (!exceeds) onChange(next);
    }} />
    <small>{bytes}/{maxBytes} bytes</small>
    {!result.valid && <span className="form-error" role="alert">{result.error}</span>}
  </div>;
}
