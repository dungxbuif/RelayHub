import { render, screen } from "@testing-library/react";
import { beforeEach, expect, it, vi } from "vitest";
import { App } from "../src/app/App";
import { AppProviders } from "../src/app/providers";

beforeEach(() => {
  history.replaceState({}, "", "/admin/system");
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const url = String(input);
    if (url.endsWith("/api/v1/admin/session")) return new Response(JSON.stringify({ csrf_token: "csrf", expires_at: "2026-09-20T22:00:00Z", user: { id: "adm_1", email: "admin@example.com", role: "admin" } }), { status: 200, headers: { "Content-Type": "application/json" } });
    if (url.startsWith("/api/v1/admin/dashboard?")) return new Response(JSON.stringify({ generated_at: "2026-09-20T03:00:00Z", window_seconds: 900, step_seconds: 60, series: [], instances: [{ instance_id: "api-1", connections: 3, nats_connected: false, nats_changed_at: "2026-09-20T02:59:00Z", heartbeat_at: "2026-09-20T03:00:00Z" }], active_connections: 3, degraded_components: ["NATS"], durable: { pending: 4, retrying: 1, dead_letter: 0 } }), { status: 200, headers: { "Content-Type": "application/json" } });
    return new Response(JSON.stringify({ error: { code: "not_found", message: "not found" } }), { status: 404, headers: { "Content-Type": "application/json" } });
  });
});

it("shows honest system health and degraded dependencies", async () => {
  render(<AppProviders><App /></AppProviders>);
  expect(await screen.findByRole("heading", { name: "System" })).toBeInTheDocument();
  expect(await screen.findByText("NATS")).toBeInTheDocument();
  expect(screen.getByRole("heading", { name: "Degraded" })).toBeInTheDocument();
  expect(screen.getByText("api-1")).toBeInTheDocument();
});
