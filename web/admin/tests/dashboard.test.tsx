import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "../src/app/App";
import { AppProviders } from "../src/app/providers";

const jsonResponse = (value: unknown) => new Response(JSON.stringify(value), { status: 200, headers: { "Content-Type": "application/json" } });

describe("live Admin read views", () => {
  beforeEach(() => history.replaceState({}, "", "/admin/"));

  it("renders real dashboard cards and supports pausing refresh", async () => {
    vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(jsonResponse({ csrf_token: "csrf-token", expires_at: "2026-09-20T22:00:00Z" }))
      .mockResolvedValueOnce(jsonResponse({
        generated_at: "2026-09-20T03:00:00Z", window_seconds: 900, step_seconds: 60,
        series: [{ at: "2026-09-20T03:00:00Z", values: { request_total: 90, status_2xx: 80, status_5xx: 10, event_published: 4 } }],
        instances: [{ instance_id: "api_1", connections: 7, nats_connected: true, nats_changed_at: "2026-09-20T02:00:00Z", heartbeat_at: "2026-09-20T03:00:00Z" }],
        active_connections: 7, degraded_components: [], durable: { pending: 2, retrying: 1, dead_letter: 3, delivery_latency_p50_ms: 125, delivery_latency_p95_ms: 480, delivery_latency_p99_ms: 1500 },
      }));
    const user = userEvent.setup();
    render(<AppProviders><App /></AppProviders>);
    await user.type(screen.getByLabelText("Bootstrap Admin token"), "bootstrap");
    await user.click(screen.getByRole("button", { name: "Sign in" }));
    expect(await screen.findByText("0.10/s")).toBeVisible();
    expect(screen.getByText("7", { selector: "strong" })).toBeVisible();
    expect(screen.getByRole("heading", { name: "HTTP status breakdown" })).toBeVisible();
    expect(screen.getByLabelText("Persisted delivery latency percentiles")).toHaveTextContent("125ms");
    expect(screen.getByLabelText("Persisted delivery latency percentiles")).toHaveTextContent("1.50s");
    await user.click(screen.getByRole("button", { name: "Pause live refresh" }));
    expect(screen.getByRole("button", { name: "Resume live refresh" })).toBeVisible();
    expect(screen.getByRole("status")).toHaveTextContent("Live refresh paused");
  });

  it("keeps event filters in the URL and sends only allowlisted values", async () => {
    history.replaceState({}, "", "/admin/events");
    const fetchMock = vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(jsonResponse({ csrf_token: "csrf-token", expires_at: "2026-09-20T22:00:00Z" }))
      .mockResolvedValue(jsonResponse({ items: [{ id: "evt_1", type: "invoice.created", source_app_id: "app_1", target_count: 1, delivery_count: 1, created_at: "2026-09-20T03:00:00Z" }] }));
    const user = userEvent.setup();
    render(<AppProviders><App /></AppProviders>);
    await user.type(screen.getByLabelText("Bootstrap Admin token"), "bootstrap");
    await user.click(screen.getByRole("button", { name: "Sign in" }));
    expect(await screen.findByText("evt_1")).toBeVisible();
    await user.type(screen.getByLabelText("Event type"), "invoice.created");
    await user.click(screen.getByRole("button", { name: "Apply filters" }));
    await waitFor(() => expect(location.search).toBe("?type=invoice.created"));
    const lastURL = String(fetchMock.mock.calls.at(-1)?.[0]);
    expect(lastURL).toContain("type=invoice.created");
    expect(lastURL).toContain("limit=25");
  });
});
