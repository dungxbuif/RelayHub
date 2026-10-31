import { useCallback, useState } from "react";
import { redactText } from "../api/client";

export type MutationFeedbackState = { kind: "success" | "error"; message: string } | null;

export function useAdminMutationFeedback() {
  const [feedback, setFeedback] = useState<MutationFeedbackState>(null);
  const succeed = useCallback((message: string) => setFeedback({ kind: "success", message: redactText(message) }), []);
  const fail = useCallback((error: unknown) => setFeedback({ kind: "error", message: redactText(error instanceof Error ? error.message : "The operation failed.") }), []);
  const clear = useCallback(() => setFeedback(null), []);
  return { feedback, succeed, fail, clear };
}

export function MutationFeedback({ feedback }: { feedback: MutationFeedbackState }) {
  if (!feedback) return null;
  return <p className={feedback.kind === "error" ? "form-error" : "success-banner"} role={feedback.kind === "error" ? "alert" : "status"}>{feedback.message}</p>;
}
