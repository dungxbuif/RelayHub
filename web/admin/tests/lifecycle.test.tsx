import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, it, vi } from "vitest";

import { App } from "../src/app/App";
import { AppProviders } from "../src/app/providers";

const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } });

beforeEach(() => { history.replaceState({}, "", "/admin/"); });

it("selects an explicit DLQ batch, confirms exact IDs and sends one idempotent replay", async () => {
  const calls: Array<{ url: string; init?: RequestInit }> = [];
  let finishReplay!: () => void;
  const replayPending = new Promise<Response>((resolve) => { finishReplay = () => resolve(json({ items: [{ delivery_id: "dlv_1", generation: 2, status: "pending" }] })); });
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    const url = String(input); calls.push({ url, init });
    if (url.endsWith("/api/v1/admin/session")) return json({ csrf_token: "csrf", expires_at: "2026-09-20T22:00:00Z" });
    if (url.startsWith("/api/v1/admin/dlq?")) return json({ items: [
      { delivery_id: "dlv_1", job_id: "job_1", event_id: "evt_1", source_app_id: "source", target_app_id: "target", sink: "callback", reason: "http_permanent", attempts: 3, created_at: "2026-09-20T08:00:00Z", updated_at: "2026-09-20T08:03:00Z" },
      { delivery_id: "dlv_2", job_id: "job_2", event_id: "evt_2", source_app_id: "source", target_app_id: "target", sink: "stream", reason: "attempts_exhausted", attempts: 6, created_at: "2026-09-20T08:00:00Z", updated_at: "2026-09-20T08:04:00Z" },
    ] });
    if (url === "/api/v1/admin/dlq/replay") return replayPending;
    throw new Error(`unexpected fetch ${url}`);
  });
  const user = userEvent.setup();
  render(<AppProviders><App /></AppProviders>);
  await user.type(screen.getByLabelText("Bootstrap Admin token"), "bootstrap");
  await user.click(screen.getByRole("button", { name: "Sign in" }));
  await user.click(await screen.findByRole("link", { name: "Dead Letters" }));
  await user.click(await screen.findByRole("checkbox", { name: "Select delivery dlv_1" }));
  await user.click(screen.getByRole("button", { name: "Replay selected (1)" }));
  const dialog = screen.getByRole("dialog", { name: "Confirm replay" });
  expect(dialog).toHaveTextContent("dlv_1");
  await user.click(screen.getByRole("button", { name: "Replay 1 delivery" }));
  expect(screen.getByRole("button", { name: "Replaying…" })).toBeDisabled();
  expect(calls.filter((call) => call.url === "/api/v1/admin/dlq/replay")).toHaveLength(1);
  const replay = calls.find((call) => call.url === "/api/v1/admin/dlq/replay")!;
  expect(new Headers(replay.init?.headers).get("Idempotency-Key")).toBeTruthy();
  expect(new Headers(replay.init?.headers).get("X-RelayHub-CSRF")).toBe("csrf");
  expect(replay.init?.body).toBe(JSON.stringify({ delivery_ids: ["dlv_1"] }));
  finishReplay();
  expect(await screen.findByText("1 delivery queued for replay.")).toBeInTheDocument();
});

it("renders a chronological persisted event lifecycle without secret fields", async () => {
  history.replaceState({}, "", "/admin/events/evt_1");
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const url = String(input);
    if (url.endsWith("/api/v1/admin/session")) return json({ csrf_token: "csrf", expires_at: "2026-09-20T22:00:00Z" });
    if (url === "/api/v1/admin/events/evt_1/timeline") return json({
      event: { id: "evt_1", type: "order.created", source_app_id: "source", target_count: 1, delivery_count: 1, created_at: "2026-09-20T08:00:00Z", target_app_ids: ["target"], data: { safe: true } },
      deliveries: [{ delivery_id: "dlv_1", job_id: "job_1", event_id: "evt_1", target_app_id: "target", sink: "callback", status: "delivered", generation: 1, attempts: 1, created_at: "2026-09-20T08:00:00Z", updated_at: "2026-09-20T08:00:02Z" }],
      attempts: [{ delivery_id: "dlv_1", generation: 1, attempt: 1, outcome: "delivered", reason: "http_success", started_at: "2026-09-20T08:00:01Z", updated_at: "2026-09-20T08:00:02Z" }],
      items: [
        { id: "event:evt_1", type: "event.created", occurred_at: "2026-09-20T08:00:00Z", event_id: "evt_1", outcome: "accepted" },
        { id: "lifecycle:2", type: "callback.delivered", occurred_at: "2026-09-20T08:00:02Z", event_id: "evt_1", delivery_id: "dlv_1", generation: 1, attempt: 1, outcome: "delivered" },
      ],
    });
    throw new Error(`unexpected fetch ${url}`);
  });
  const user = userEvent.setup();
  render(<AppProviders><App /></AppProviders>);
  await user.type(screen.getByLabelText("Bootstrap Admin token"), "bootstrap");
  await user.click(screen.getByRole("button", { name: "Sign in" }));
  expect(await screen.findByRole("heading", { name: "Event evt_1" })).toBeInTheDocument();
  expect(await screen.findByText("Event created")).toBeInTheDocument();
  expect(screen.getByText("Callback delivered")).toBeInTheDocument();
  expect(document.body).not.toHaveTextContent("callback_token");
  await waitFor(() => expect(location.pathname).toBe("/admin/events/evt_1"));
});
