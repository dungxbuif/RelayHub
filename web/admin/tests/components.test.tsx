import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { ApiError, redactText, shouldRetryAdminRequest } from "../src/api/client";
import { ConfirmationDialog } from "../src/components/ConfirmationDialog";
import { JsonEditor, validateJson } from "../src/components/JsonEditor";
import { MutationFeedback, useAdminMutationFeedback } from "../src/components/MutationFeedback";
import { ResourceSelector } from "../src/components/ResourceSelector";

describe("Admin shared components", () => {
  it("redacts structured secrets and never retries a forbidden response", () => {
    const text = redactText('{"token":"socket-secret","password":"pw-secret"} /ws?access_token=url-secret');
    expect(text).not.toContain("socket-secret");
    expect(text).not.toContain("pw-secret");
    expect(text).not.toContain("url-secret");
    expect(shouldRetryAdminRequest(0, new ApiError(403, "forbidden", "denied"))).toBe(false);
    expect(shouldRetryAdminRequest(0, new ApiError(401, "unauthorized", "denied"))).toBe(false);
  });

  it("validates JSON by UTF-8 byte size", () => {
    expect(validateJson('{"ok":true}', 32).valid).toBe(true);
    expect(validateJson("{bad", 32).valid).toBe(false);
    expect(validateJson("é".repeat(20), 20).error).toContain("bytes");
    render(<JsonEditor label="Payload" value="{bad" onChange={() => undefined} maxBytes={32} />);
    expect(screen.getByRole("textbox", { name: "Payload" })).toHaveAttribute("aria-invalid", "true");
  });

  it("renders a bounded resource selector and redacted mutation feedback", () => {
    render(<>
      <ResourceSelector label="Application" value="app_1" options={[{ id: "app_1", label: "Orders" }]} onChange={() => undefined} />
      <MutationFeedback feedback={{ kind: "success", message: "Saved" }} />
    </>);
    expect(screen.getByRole("combobox", { name: "Application" })).toHaveValue("app_1");
    expect(screen.getByRole("status")).toHaveTextContent("Saved");
  });

  it("shows mutation errors through the feedback hook without leaking secrets", () => {
    function Probe() {
      const feedback = useAdminMutationFeedback();
      return <>
        <button onClick={() => feedback.fail(new Error("Authorization: Bearer secret-value"))}>Fail</button>
        <MutationFeedback feedback={feedback.feedback} />
      </>;
    }
    render(<Probe />);
    fireEvent.click(screen.getByRole("button", { name: "Fail" }));
    expect(screen.getByRole("alert")).toHaveTextContent("[REDACTED]");
    expect(screen.getByRole("alert")).not.toHaveTextContent("secret-value");
  });

  it("keeps focus inside confirmation dialogs", () => {
    render(<ConfirmationDialog title="Confirm" confirmLabel="Delete" pending={false} onConfirm={() => undefined} onCancel={() => undefined}><button>Details</button></ConfirmationDialog>);
    const cancel = screen.getByRole("button", { name: "Cancel" });
    const confirm = screen.getByRole("button", { name: "Delete" });
    const details = screen.getByRole("button", { name: "Details" });
    details.focus();
    fireEvent.keyDown(screen.getByRole("dialog"), { key: "Tab", shiftKey: true });
    expect(document.activeElement).toBe(confirm);
    confirm.focus();
    fireEvent.keyDown(screen.getByRole("dialog"), { key: "Tab" });
    expect(document.activeElement).toBe(details);
    expect(cancel).toBeInTheDocument();
  });
});
