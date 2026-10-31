import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, it, vi } from "vitest";
import { App } from "../src/app/App";
import { AppProviders } from "../src/app/providers";

beforeEach(() => {
  history.replaceState({}, "", "/admin/functions");
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    const url = String(input);
    if (url.endsWith("/api/v1/admin/session")) return new Response(JSON.stringify({ csrf_token: "csrf", expires_at: "2026-09-20T22:00:00Z", user: { id: "adm_1", email: "admin@example.com", role: "admin" } }), { status: 200, headers: { "Content-Type": "application/json" } });
    if (url === "/api/v1/apps") return new Response(JSON.stringify([{ id: "app_1", name: "Orders", delivery_mode: "queue", enabled: true, created_at: "2026-09-20T00:00:00Z", updated_at: "2026-09-20T00:00:00Z" }]), { status: 200, headers: { "Content-Type": "application/json" } });
    if (url === "/api/v1/admin/apps/app_1/functions" && (!init?.method || init.method === "GET")) return new Response(JSON.stringify([{ id: "fn_1", app_id: "app_1", name: "reconcile", timeout_seconds: 10, enabled: true, created_at: "2026-09-20T00:00:00Z", updated_at: "2026-09-20T00:00:00Z" }]), { status: 200, headers: { "Content-Type": "application/json" } });
    if (url === "/api/v1/admin/apps/app_1/functions" && init?.method === "POST") return new Response(JSON.stringify({ id: "fn_2", app_id: "app_1", name: "notify", timeout_seconds: 5, enabled: true, created_at: "2026-09-20T00:00:00Z", updated_at: "2026-09-20T00:00:00Z" }), { status: 201, headers: { "Content-Type": "application/json" } });
    if (url === "/api/v1/admin/apps/app_1/functions/fn_1/invoke") return new Response(JSON.stringify({ invocation_id: "inv_1", ok: true, result: { "done": true } }), { status: 200, headers: { "Content-Type": "application/json" } });
    return new Response(JSON.stringify({ error: { code: "function_timeout", message: "Function timed out." } }), { status: 504, headers: { "Content-Type": "application/json" } });
  });
});

it("lists, registers and invokes functions for the selected app", async () => {
  const fetchMock = vi.mocked(globalThis.fetch);
  const user = userEvent.setup();
  render(<AppProviders><App /></AppProviders>);
  expect(await screen.findByRole("heading", { name: "Functions" })).toBeInTheDocument();
  expect(await screen.findByText("reconcile")).toBeInTheDocument();
  await user.type(screen.getByLabelText("Function name"), "notify");
  await user.click(screen.getByRole("button", { name: "Register function" }));
  expect(fetchMock.mock.calls.some(([path, init]) => String(path) === "/api/v1/admin/apps/app_1/functions" && init?.method === "POST")).toBe(true);
  await user.click(screen.getByRole("button", { name: "Invoke reconcile" }));
  expect(await screen.findByText("Invocation completed")).toBeInTheDocument();
});
