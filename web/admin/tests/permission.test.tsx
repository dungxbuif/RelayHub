import { render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { AuthProvider, useAdminPrincipal } from "../src/auth/AuthProvider";
import { PermissionGate, canMutate } from "../src/auth/PermissionGate";

function SessionProbe() {
  const principal = useAdminPrincipal();
  return <>
    <span data-testid="principal-role">{principal?.role ?? "loading"}</span>
    <PermissionGate mutation fallback={<span>Read-only access</span>}><button>Rotate secret</button></PermissionGate>
  </>;
}

describe("Admin permission primitives", () => {
  beforeEach(() => {
    vi.spyOn(globalThis, "fetch").mockImplementation(async () => new Response(JSON.stringify({
      csrf_token: "csrf",
      expires_at: "2026-09-20T22:00:00Z",
      user: { id: "user_1", email: "operator@example.com", role: "user" },
    }), { status: 200, headers: { "Content-Type": "application/json" } }));
  });

  it("exposes the session principal and hides mutations for read-only users", async () => {
    render(<AuthProvider><SessionProbe /></AuthProvider>);
    await waitFor(() => expect(screen.getByTestId("principal-role")).toHaveTextContent("user"));
    expect(screen.getByText("Read-only access")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Rotate secret" })).not.toBeInTheDocument();
  });

  it("allows mutations only for Admin principals", () => {
    expect(canMutate({ id: "a", email: "a@example.com", role: "admin" })).toBe(true);
    expect(canMutate({ id: "u", email: "u@example.com", role: "user" })).toBe(false);
    expect(canMutate(null)).toBe(false);
  });
});
