import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, it, vi } from "vitest";
import { App } from "../src/app/App";
import { AppProviders } from "../src/app/providers";

const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } });
beforeEach(() => history.replaceState({}, "", "/admin/apps"));

it("shows new credentials once and requires explicit secure-storage acknowledgement", async () => {
  const secret = "rhs_SENTINEL_ONCE"; let apps: unknown[] = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    const url = String(input); const method = init?.method ?? "GET";
    if (url.endsWith("/api/v1/admin/session")) return json({ csrf_token: "csrf", expires_at: "2026-09-20T22:00:00Z", user: { id: "adm_1", email: "dungbui.dungbui.00@gmail.com", role: "admin" } });
    if (url === "/api/v1/apps" && method === "GET") return json(apps);
    if (url === "/api/v1/apps" && method === "POST") { apps = [{ id: "app_1", name: "Orders", delivery_mode: "queue", enabled: true, created_at: "2026-09-20T00:00:00Z", updated_at: "2026-09-20T00:00:00Z" }]; return json({ app_id: "app_1", api_key: "rhk_once", hmac_secret: secret }, 201); }
    throw new Error(`unexpected ${method} ${url}`);
  });
  const user = userEvent.setup(); render(<AppProviders><App /></AppProviders>); await user.type(await screen.findByLabelText("Name"), "Orders"); await user.click(screen.getByRole("button", { name: "Save app" }));
  expect(await screen.findByRole("dialog", { name: "Store application credentials" })).toHaveTextContent(secret); const close = screen.getByRole("button", { name: "Close credentials" }); expect(close).toBeDisabled(); await user.click(screen.getByLabelText("I stored these credentials securely.")); await user.click(close); expect(document.body).not.toHaveTextContent(secret);
});
