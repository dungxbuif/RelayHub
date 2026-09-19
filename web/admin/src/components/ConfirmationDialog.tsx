import { useEffect, useRef, type ReactNode } from "react";

export function ConfirmationDialog({ title, children, confirmLabel, pending, error, onConfirm, onCancel }: { title: string; children: ReactNode; confirmLabel: string; pending: boolean; error?: string; onConfirm(): void; onCancel(): void }) {
  const cancelRef = useRef<HTMLButtonElement>(null);
  const dialogRef = useRef<HTMLElement>(null);
  useEffect(() => { cancelRef.current?.focus(); }, []);
  return <div className="dialog-backdrop" onMouseDown={(event) => { if (event.target === event.currentTarget && !pending) onCancel(); }}>
    <section ref={dialogRef} className="confirmation-dialog" role="dialog" aria-modal="true" aria-labelledby="confirmation-title" onKeyDown={(event) => {
      if (event.key === "Escape" && !pending) onCancel();
      if (event.key === "Tab") { const controls = [...(dialogRef.current?.querySelectorAll<HTMLElement>('button:not([disabled]), [href], input:not([disabled])') ?? [])]; if (controls.length > 0) { const next = event.shiftKey ? controls.at(-1) : controls[0]; const edge = event.shiftKey ? controls[0] : controls.at(-1); if (document.activeElement === edge) { event.preventDefault(); next?.focus(); } } }
    }}>
      <p className="eyebrow">OPERATOR ACTION</p><h2 id="confirmation-title">{title}</h2>{children}
      {error && <p className="form-error" role="alert">{error}</p>}
      <div className="dialog-actions"><button ref={cancelRef} className="quiet-button" type="button" disabled={pending} onClick={onCancel}>Cancel</button><button className="danger-button" type="button" disabled={pending} onClick={onConfirm}>{pending ? "Replaying…" : confirmLabel}</button></div>
    </section>
  </div>;
}
