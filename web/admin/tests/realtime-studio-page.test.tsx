import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, it, vi } from "vitest";

import { App } from "../src/app/App";
import { AppProviders } from "../src/app/providers";

const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } });

class FakeWebSocket {
  static readonly OPEN = 1;
  readyState = FakeWebSocket.OPEN;
  protocol = "";
  onopen: (() => void) | null = null;
  onclose: ((event: { code: number }) => void) | null = null;
  onerror: (() => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;

  constructor() {
    queueMicrotask(() => this.onopen?.());
  }

  send() {}
  close() {
    this.readyState = 3;
    this.onclose?.({ code: 1000 });
  }
}

beforeEach(() => {
  history.replaceState({}, "", "/admin/realtime-studio");
  vi.restoreAllMocks();
  vi.stubGlobal("WebSocket", FakeWebSocket);
});

it("surfaces a failed HTTP publish in mutation feedback", async () => {
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    const url = String(input);
    if (url.endsWith("/api/v1/admin/session")) return json({ csrf_token: "csrf", expires_at: "2026-09-20T22:00:00Z", user: { id: "adm_1", email: "admin@example.com", role: "admin" } });
    if (url === "/api/v1/apps") return json([{ id: "app_1", name: "Orders", delivery_mode: "all", enabled: true, created_at: "2026-09-20T00:00:00Z", updated_at: "2026-09-20T00:00:00Z" }]);
    if (url.includes("/api/v1/admin/connections")) return json({ connections: [] });
    if (url.endsWith("/api/v1/admin/studio/token") && init?.method === "POST") return json({ token: "short-lived", expires_at: "2026-09-20T00:05:00Z" }, 201);
    if (url.endsWith("/api/v1/admin/studio/publish") && init?.method === "POST") return json({ error: { code: "not_ready", message: "Publisher unavailable." } }, 503);
    throw new Error(`unexpected fetch ${url}`);
  });

  const user = userEvent.setup();
  render(<AppProviders><App /></AppProviders>);
  await screen.findByRole("option", { name: "Orders" });
  await user.selectOptions(await screen.findByLabelText("Application"), "app_1");
  await user.selectOptions(screen.getByLabelText("Protocol"), "realtime");
  await user.click(screen.getByRole("button", { name: "Connect" }));
  await user.click(await screen.findByRole("button", { name: "HTTP publish" }));
  expect(await screen.findByRole("alert")).toHaveTextContent("Publisher unavailable.");
});
