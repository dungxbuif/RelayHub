import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, it, vi } from "vitest";

import { App } from "../src/app/App";
import { AppProviders } from "../src/app/providers";

beforeEach(() => {
  history.replaceState({}, "", "/admin/");
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const url = String(input);
    const body = url.endsWith("/api/v1/admin/session")
      ? { csrf_token: "csrf", expires_at: "2026-09-20T22:00:00Z", user: { id: "adm_1", email: "dungbui.dungbui.00@gmail.com", role: "admin" } }
      : url.startsWith("/api/v1/admin/dashboard?")
        ? { generated_at: "2026-09-20T03:00:00Z", window_seconds: 900, step_seconds: 60, series: [], instances: [], active_connections: 0, degraded_components: [], durable: { pending: 0, retrying: 0, dead_letter: 0 } }
        : { items: [] };
    return new Response(JSON.stringify(body), { status: 200, headers: { "Content-Type": "application/json" } });
  });
});

it("provides every locked navigation destination and restores page focus", async () => {
  const user = userEvent.setup();
  render(<AppProviders><App /></AppProviders>);
  const navigation = await screen.findByRole("navigation", { name: "Admin sections" });
  for (const label of ["Overview", "Events", "Dead Letters", "Queue", "Apps", "Routing Rules", "Realtime Studio", "Audit Logs", "System"]) {
    expect(navigation).toHaveTextContent(label);
  }
  await user.click(screen.getByRole("link", { name: "Dead Letters" }));
  const heading = await screen.findByRole("heading", { name: "Dead Letters" });
  expect(heading).toHaveFocus();
  expect(location.pathname).toBe("/admin/dead-letters");
});

it("normalizes legacy dashboard deep links without appending nested 404 paths", async () => {
  history.replaceState({}, "", "/admin/dashboard/404/404");
  render(<AppProviders><App /></AppProviders>);

  await screen.findByRole("heading", { name: "Overview" });
  expect(location.pathname).toBe("/admin");
  expect(location.pathname).not.toContain("/404");
});
