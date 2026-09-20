import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { App } from "../src/app/App";
import { AppProviders } from "../src/app/providers";

describe("Admin authentication", () => {
  beforeEach(() => history.replaceState({}, "", "/admin/"));

  it("exchanges email and password without persisting credentials", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      if (String(input) === "/api/v1/admin/session" && init?.method === "POST") {
        return new Response(JSON.stringify({
          csrf_token: "csrf-secret-value",
          expires_at: "2026-09-20T22:00:00Z",
          user: { id: "adm_1", email: "dungbui.dungbui.00@gmail.com", role: "admin" },
        }), { status: 200, headers: { "Content-Type": "application/json" } });
      }
      return new Response(JSON.stringify({ error: { code: "unauthorized", message: "Authentication failed." } }), { status: 401, headers: { "Content-Type": "application/json" } });
    });
    const user = userEvent.setup();
    render(<AppProviders><App /></AppProviders>);

    const password = await screen.findByLabelText("Password");
    await user.type(screen.getByLabelText("Email"), "dungbui.dungbui.00@gmail.com");
    await user.type(password, "bootstrap-secret-value");
    await user.click(screen.getByRole("button", { name: "Sign in" }));

    await screen.findByRole("navigation", { name: "Admin sections" });
    expect(password).toHaveValue("");
    expect(localStorage.length).toBe(0);
    expect(sessionStorage.length).toBe(0);
    expect(document.body.textContent).not.toContain("bootstrap-secret-value");
    const loginCall = fetchMock.mock.calls.find(([path, init]) => String(path) === "/api/v1/admin/session" && init?.method === "POST");
    expect(loginCall?.[1]).toEqual(expect.objectContaining({ method: "POST", credentials: "same-origin" }));
    expect(new Headers(loginCall?.[1]?.headers).has("Authorization")).toBe(false);
  });

  it("shows bounded login errors and clears the submitted secret", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(async () => new Response(JSON.stringify({ error: { code: "unauthorized", message: "Authentication failed." } }), {
      status: 401,
      headers: { "Content-Type": "application/json" },
    }));
    const user = userEvent.setup();
    render(<AppProviders><App /></AppProviders>);
    const password = await screen.findByLabelText("Password");
    await user.type(screen.getByLabelText("Email"), "dungbui.dungbui.00@gmail.com");
    await user.type(password, "wrong-secret");
    await user.click(screen.getByRole("button", { name: "Sign in" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Authentication failed.");
    expect(password).toHaveValue("");
    expect(document.body.textContent).not.toContain("wrong-secret");
  });

  it("adds CSRF only to mutations and returns to login on 401", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const path = String(input);
      if (path === "/api/v1/admin/session" && init?.method === "GET") {
        return new Response(JSON.stringify({ error: { code: "unauthorized", message: "Authentication failed." } }), { status: 401, headers: { "Content-Type": "application/json" } });
      }
      if (path === "/api/v1/admin/session" && init?.method === "POST") {
        return new Response(JSON.stringify({ csrf_token: "csrf-token", expires_at: "2026-09-20T22:00:00Z", user: { id: "adm_1", email: "dungbui.dungbui.00@gmail.com", role: "admin" } }), { status: 200, headers: { "Content-Type": "application/json" } });
      }
      if (path === "/api/v1/admin/session" && init?.method === "DELETE") return new Response(null, { status: 204 });
      if (path.startsWith("/api/v1/admin/dashboard?")) {
        return new Response(JSON.stringify({ generated_at: "2026-09-20T03:00:00Z", window_seconds: 900, step_seconds: 60, series: [], instances: [], active_connections: 0, degraded_components: [], durable: { pending: 0, retrying: 0, dead_letter: 0 } }), { status: 200, headers: { "Content-Type": "application/json" } });
      }
      return new Response(JSON.stringify({ error: { code: "unauthorized", message: "Authentication failed." } }), { status: 401, headers: { "Content-Type": "application/json" } });
    });
    const user = userEvent.setup();
    render(<AppProviders><App /></AppProviders>);
    await user.type(await screen.findByLabelText("Email"), "dungbui.dungbui.00@gmail.com");
    await user.type(screen.getByLabelText("Password"), "bootstrap");
    await user.click(screen.getByRole("button", { name: "Sign in" }));
    await screen.findByRole("navigation", { name: "Admin sections" });
    await user.click(screen.getByRole("button", { name: "Sign out" }));
    await screen.findByRole("button", { name: "Sign in" });
    const logoutCall = fetchMock.mock.calls.find(([path, init]) => String(path) === "/api/v1/admin/session" && init?.method === "DELETE");
    expect(logoutCall?.[1]).toEqual(expect.objectContaining({ method: "DELETE" }));
    expect(new Headers(logoutCall?.[1]?.headers).get("X-RelayHub-CSRF")).toBe("csrf-token");

    // A later unauthorized API response must invalidate all in-memory auth state.
    const { AdminApiClient } = await import("../src/api/client");
    let unauthorized = false;
    const client = new AdminApiClient(() => "csrf-token", () => { unauthorized = true; });
    await expect(client.request("/api/v1/admin/dashboard")).rejects.toMatchObject({ status: 401 });
    await waitFor(() => expect(unauthorized).toBe(true));
    const unauthorizedCall = fetchMock.mock.calls.at(-1);
    expect(new Headers(unauthorizedCall?.[1]?.headers).has("X-RelayHub-CSRF")).toBe(false);
  });
});
