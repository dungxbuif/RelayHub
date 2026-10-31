import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, it, vi } from "vitest";

import { App } from "../src/app/App";
import { AppProviders } from "../src/app/providers";

const json = (value: unknown) => new Response(JSON.stringify(value), { status: 200, headers: { "Content-Type": "application/json" } });

beforeEach(() => history.replaceState({}, "", "/admin/events/evt_1"));

it("redacts sensitive event payload fields before rendering the bounded detail view", async () => {
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const url = String(input);
    if (url.endsWith("/api/v1/admin/session")) return json({ csrf_token: "csrf", expires_at: "2026-09-20T22:00:00Z", user: { id: "adm_1", email: "admin@example.com", role: "admin" } });
    if (url === "/api/v1/admin/events/evt_1/timeline") return json({
      event: { id: "evt_1", type: "order.created", source_app_id: "source", target_count: 1, delivery_count: 1, created_at: "2026-09-20T08:00:00Z", target_app_ids: ["target"], data: { safe: true, token: "do-not-render", nested: { password: "also-do-not-render" } } },
      deliveries: [],
      attempts: [],
      items: [],
    });
    throw new Error(`unexpected fetch ${url}`);
  });

  render(<AppProviders><App /></AppProviders>);
  expect(await screen.findByRole("heading", { name: "Event evt_1" })).toBeInTheDocument();
  expect(await screen.findByText(/safe/)).toBeInTheDocument();
  expect(document.body).not.toHaveTextContent("do-not-render");
  expect(document.body).not.toHaveTextContent("also-do-not-render");
});

it("keeps event pagination bounded and carries only the server cursor", async () => {
  const urls: string[] = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const url = String(input); urls.push(url);
    if (url.endsWith("/api/v1/admin/session")) return json({ csrf_token: "csrf", expires_at: "2026-09-20T22:00:00Z", user: { id: "adm_1", email: "admin@example.com", role: "admin" } });
    if (url.startsWith("/api/v1/admin/events?")) return json({ items: [], next_cursor: "cursor-2" });
    throw new Error(`unexpected fetch ${url}`);
  });
  history.replaceState({}, "", "/admin/events");
  const user = userEvent.setup();
  render(<AppProviders><App /></AppProviders>);
  await user.click(await screen.findByRole("button", { name: "Next page" }));
  expect(urls.some((url) => url.includes("limit=25") && url.includes("cursor=cursor-2"))).toBe(true);
  expect(urls.some((url) => url.includes("payload") || url.includes("token"))).toBe(false);
});
