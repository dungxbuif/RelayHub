import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, it, vi } from "vitest";

import { App } from "../src/app/App";
import { AppProviders } from "../src/app/providers";

const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } });

beforeEach(() => { history.replaceState({}, "", "/admin/realtime-resources"); vi.restoreAllMocks(); });

it("inspects realtime resources, confirms disconnect and never renders push tokens", async () => {
  const calls: Array<{ url: string; init?: RequestInit }> = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    const url = String(input); calls.push({ url, init });
    if (url.endsWith("/api/v1/admin/session")) return json({ csrf_token: "csrf", expires_at: "2026-09-20T22:00:00Z", user: { id: "adm_1", email: "admin@example.com", role: "admin" } });
    if (url === "/api/v1/apps") return json([{ id: "app_1", name: "Orders", delivery_mode: "all", enabled: true, created_at: "2026-09-20T00:00:00Z", updated_at: "2026-09-20T00:00:00Z" }]);
    if (url.endsWith("/apps/app_1/connections?limit=100")) return json({ connections: [{ app_id: "app_1", client_id: "client_1", connection_id: "conn_1", instance_id: "api_1", protocol: "relayhub.realtime.v2", channels: ["room"], connected_at: "2026-09-20T00:00:00Z", last_seen_at: "2026-09-20T00:01:00Z" }] });
    if (url.includes("/channels/room/history")) return json({ type: "history.result", channel: "room", items: [{ type: "channel.message", message_id: "msg_1", channel: "room", cursor: "MTIzNC0w", data: { safe: true } }], next_cursor: "" });
    if (url.includes("/push/devices")) return json({ devices: [{ id: "device_1", app_id: "app_1", provider: "fcm", created_at: "2026-09-20T00:00:00Z", updated_at: "2026-09-20T00:00:00Z" }] });
    if (url.endsWith("/connections/conn_1") && init?.method === "DELETE") return new Response(null, { status: 204 });
    if (url.endsWith("/push/devices") && init?.method === "POST") return json({ id: "device_2", app_id: "app_1", provider: "fcm", created_at: "2026-09-20T00:00:00Z", updated_at: "2026-09-20T00:00:00Z" }, 201);
    throw new Error(`unexpected fetch ${url}`);
  });

  const user = userEvent.setup();
  render(<AppProviders><App /></AppProviders>);
  await screen.findByRole("option", { name: "Orders" });
  await user.selectOptions(await screen.findByLabelText("Application"), "app_1");
  expect(await screen.findByText("conn_1")).toBeInTheDocument();
  expect(await screen.findByText("msg_1")).toBeInTheDocument();
  expect(screen.getByText("MTIzNC0w")).toBeInTheDocument();
  expect(await screen.findByText("device_1")).toBeInTheDocument();

  await user.click(screen.getByRole("button", { name: "Disconnect conn_1" }));
  expect(screen.getByRole("dialog", { name: "Confirm connection disconnect" })).toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Disconnect connection" }));
  expect(calls.some((call) => call.url.endsWith("/connections/conn_1") && call.init?.method === "DELETE")).toBe(true);

  await user.type(screen.getByLabelText("Device token"), "push-token-must-not-render");
  await user.click(screen.getByRole("button", { name: "Register device" }));
  expect(await screen.findByText("Push device registered.")).toBeInTheDocument();
  expect(document.body).not.toHaveTextContent("push-token-must-not-render");
  const register = calls.find((call) => call.url.endsWith("/push/devices") && call.init?.method === "POST");
  expect(register?.init?.body).toContain("push-token-must-not-render");
});

it("does not expose push mutation forms to read-only users", async () => {
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const url = String(input);
    if (url.endsWith("/api/v1/admin/session")) return json({ csrf_token: "csrf", expires_at: "2026-09-20T22:00:00Z", user: { id: "usr_1", email: "user@example.com", role: "user" } });
    if (url === "/api/v1/apps") return json([{ id: "app_1", name: "Orders", delivery_mode: "all", enabled: true, created_at: "2026-09-20T00:00:00Z", updated_at: "2026-09-20T00:00:00Z" }]);
    if (url.endsWith("/apps/app_1/connections?limit=100")) return json({ connections: [] });
    if (url.includes("/channels/room/history")) return json({ type: "history.result", channel: "room", items: [], next_cursor: "" });
    if (url.includes("/push/devices")) return json({ devices: [] });
    throw new Error(`unexpected fetch ${url}`);
  });

  const user = userEvent.setup();
  render(<AppProviders><App /></AppProviders>);
  await screen.findByRole("option", { name: "Orders" });
  await user.selectOptions(await screen.findByLabelText("Application"), "app_1");
  expect(screen.queryByLabelText("Device token")).not.toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "Register device" })).not.toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "Publish notification" })).not.toBeInTheDocument();
});
