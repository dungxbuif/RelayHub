import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { App } from "../src/app/App";
import { AppProviders } from "../src/app/providers";

describe("Admin authentication", () => {
  beforeEach(() => history.replaceState({}, "", "/admin/"));

  it("exchanges the bootstrap token without persisting it", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify({
      csrf_token: "csrf-secret-value",
      expires_at: "2026-09-20T22:00:00Z",
    }), { status: 200, headers: { "Content-Type": "application/json" } }));
    const user = userEvent.setup();
    render(<AppProviders><App /></AppProviders>);

    const token = screen.getByLabelText("Bootstrap Admin token");
    await user.type(token, "bootstrap-secret-value");
    await user.click(screen.getByRole("button", { name: "Sign in" }));

    await screen.findByRole("navigation", { name: "Admin sections" });
    expect(token).toHaveValue("");
    expect(localStorage.length).toBe(0);
    expect(sessionStorage.length).toBe(0);
    expect(document.body.textContent).not.toContain("bootstrap-secret-value");
    expect(fetchMock).toHaveBeenCalledWith("/api/v1/admin/session", expect.objectContaining({
      method: "POST",
      credentials: "same-origin",
    }));
    expect(new Headers(fetchMock.mock.calls[0]?.[1]?.headers).get("Authorization")).toBe("Bearer bootstrap-secret-value");
  });

  it("shows bounded login errors and clears the submitted secret", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify({ error: { code: "unauthorized", message: "Authentication failed." } }), {
      status: 401,
      headers: { "Content-Type": "application/json" },
    }));
    const user = userEvent.setup();
    render(<AppProviders><App /></AppProviders>);
    const token = screen.getByLabelText("Bootstrap Admin token");
    await user.type(token, "wrong-secret");
    await user.click(screen.getByRole("button", { name: "Sign in" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Authentication failed.");
    expect(token).toHaveValue("");
    expect(document.body.textContent).not.toContain("wrong-secret");
  });

  it("adds CSRF only to mutations and returns to login on 401", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response(JSON.stringify({ csrf_token: "csrf-token", expires_at: "2026-09-20T22:00:00Z" }), { status: 200, headers: { "Content-Type": "application/json" } }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: { code: "unauthorized", message: "Authentication failed." } }), { status: 401, headers: { "Content-Type": "application/json" } }));
    const user = userEvent.setup();
    render(<AppProviders><App /></AppProviders>);
    await user.type(screen.getByLabelText("Bootstrap Admin token"), "bootstrap");
    await user.click(screen.getByRole("button", { name: "Sign in" }));
    await screen.findByRole("navigation", { name: "Admin sections" });
    await user.click(screen.getByRole("button", { name: "Sign out" }));
    await screen.findByRole("button", { name: "Sign in" });
    expect(fetchMock.mock.calls[1]?.[1]).toEqual(expect.objectContaining({ method: "DELETE" }));
    expect(new Headers(fetchMock.mock.calls[1]?.[1]?.headers).get("X-RelayHub-CSRF")).toBe("csrf-token");

    // A later unauthorized API response must invalidate all in-memory auth state.
    const { AdminApiClient } = await import("../src/api/client");
    let unauthorized = false;
    const client = new AdminApiClient(() => "csrf-token", () => { unauthorized = true; });
    await expect(client.request("/api/v1/admin/dashboard")).rejects.toMatchObject({ status: 401 });
    await waitFor(() => expect(unauthorized).toBe(true));
    expect(new Headers(fetchMock.mock.calls[2]?.[1]?.headers).has("X-RelayHub-CSRF")).toBe(false);
  });
});
