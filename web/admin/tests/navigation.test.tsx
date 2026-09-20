import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, it, vi } from "vitest";

import { App } from "../src/app/App";
import { AppProviders } from "../src/app/providers";

beforeEach(() => {
  history.replaceState({}, "", "/admin/");
  vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify({ csrf_token: "csrf", expires_at: "2026-09-20T22:00:00Z" }), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  }));
});

it("provides every locked navigation destination and restores page focus", async () => {
  const user = userEvent.setup();
  render(<AppProviders><App /></AppProviders>);
  await user.type(screen.getByLabelText("Bootstrap Admin token"), "bootstrap");
  await user.click(screen.getByRole("button", { name: "Sign in" }));
  const navigation = await screen.findByRole("navigation", { name: "Admin sections" });
  for (const label of ["Overview", "Events", "Dead Letters", "Queue v2", "Apps", "Routing Rules", "Realtime Studio", "Audit Logs", "System"]) {
    expect(navigation).toHaveTextContent(label);
  }
  await user.click(screen.getByRole("link", { name: "Dead Letters" }));
  const heading = await screen.findByRole("heading", { name: "Dead Letters" });
  expect(heading).toHaveFocus();
  expect(location.pathname).toBe("/admin/dead-letters");
});
