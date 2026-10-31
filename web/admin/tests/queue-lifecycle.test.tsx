import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, it, vi } from "vitest";

import { App } from "../src/app/App";
import { AppProviders } from "../src/app/providers";

const json = (value: unknown, status = 200, headers: Record<string, string> = { "Content-Type": "application/json" }) => new Response(JSON.stringify(value), { status, headers });

beforeEach(() => history.replaceState({}, "", "/admin/queue"));

it("supports subscription pause, schedule creation and confirmed queue DLQ deletion", async () => {
  const calls: Array<{ url: string; init?: RequestInit }> = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    const url = String(input);
    calls.push({ url, init });
    if (url.endsWith("/api/v1/admin/session")) return json({ csrf_token: "csrf", expires_at: "2026-09-20T22:00:00Z", user: { id: "adm_1", email: "admin@example.com", role: "admin" } });
    if (url === "/api/v1/apps") return json([{ id: "app_1", name: "Orders", delivery_mode: "queue", enabled: true, created_at: "2026-09-20T00:00:00Z", updated_at: "2026-09-20T00:00:00Z" }]);
    if (url === "/api/v1/admin/apps/app_1/subscriptions") return json({ items: [{ id: "sub_1", app_id: "app_1", name: "orders", enabled: true, max_in_flight: 10, max_batch_size: 5, ordering_mode: "key", policy_version: 1 }] });
    if (url.endsWith("/metrics")) return json({ available: 3, in_flight: 1, acknowledged: 8, dead_letter: 1 });
    if (url.endsWith("/schedules")) return json({ items: [] });
    if (url.endsWith("/callbacks")) return json({ items: [] });
    if (url.endsWith("/dead-letters?limit=50")) return json({ items: [{ delivery_id: "qdl_1", subscription_id: "sub_1", event_id: "evt_1", attempts: 3, generation: 1, reason: "timeout", updated_at: "2026-09-20T00:00:00Z" }] });
    if (url.endsWith("/pause")) return json({ id: "sub_1", app_id: "app_1", name: "orders", enabled: true, paused_at: "2026-09-20T00:00:00Z", max_in_flight: 10, max_batch_size: 5, ordering_mode: "key", policy_version: 2 });
    if (url.endsWith("/schedules") && init?.method === "POST") return json({ id: "qsch_1" }, 201);
    if (url.endsWith("/dead-letters/delete")) return json({ deleted: 1 });
    throw new Error(`unexpected fetch ${url}`);
  });

  const user = userEvent.setup();
  render(<AppProviders><App /></AppProviders>);
  await user.click(await screen.findByRole("option", { name: "Orders" }));
  await user.selectOptions(screen.getByLabelText("Application"), "app_1");

  expect(await screen.findByRole("button", { name: "Pause subscription" })).toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Pause subscription" }));
  expect(await screen.findByText("Subscription paused.")).toBeInTheDocument();
  expect(calls.some((call) => call.url.endsWith("/pause"))).toBe(true);

  await user.type(screen.getByLabelText("Schedule name"), "hourly");
  await user.type(screen.getByLabelText("Schedule cron"), "0 * * * *");
  await user.type(screen.getByLabelText("Schedule event type"), "orders.tick");
  await user.click(screen.getByRole("button", { name: "Create schedule" }));
  expect(await screen.findByText("Schedule created.")).toBeInTheDocument();

  await user.click(screen.getByRole("checkbox", { name: "Select dead letter qdl_1" }));
  await user.click(screen.getByRole("button", { name: "Delete selected dead letters (1)" }));
  expect(screen.getByRole("dialog", { name: "Confirm dead-letter deletion" })).toHaveTextContent("qdl_1");
  await user.click(screen.getByRole("button", { name: "Delete 1 dead letter" }));
  expect(await screen.findByText("1 dead letter deleted.")).toBeInTheDocument();
  const deletion = calls.find((call) => call.url.endsWith("/dead-letters/delete"));
  expect(deletion).toBeDefined();
  expect(deletion?.init?.body).toBe(JSON.stringify({ delivery_ids: ["qdl_1"], confirm: true }));
});
