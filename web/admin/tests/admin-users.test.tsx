import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, it, vi } from "vitest";
import { App } from "../src/app/App";
import { AppProviders } from "../src/app/providers";

beforeEach(() => {
  history.replaceState({}, "", "/admin/users");
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    const url = String(input);
    if (url.endsWith("/api/v1/admin/session")) return new Response(JSON.stringify({ csrf_token: "csrf", expires_at: "2026-09-20T22:00:00Z", user: { id: "adm_1", email: "admin@example.com", role: "admin" } }), { status: 200, headers: { "Content-Type": "application/json" } });
    if (url === "/api/v1/admin/users" && (!init?.method || init.method === "GET")) return new Response(JSON.stringify([{ id: "usr_1", email: "operator@example.com", role: "user", enabled: true, created_at: "2026-09-20T00:00:00Z", updated_at: "2026-09-20T00:00:00Z" }]), { status: 200, headers: { "Content-Type": "application/json" } });
    if (url === "/api/v1/admin/users" && init?.method === "POST") return new Response(JSON.stringify({ id: "usr_2", email: "new@example.com", role: "user", enabled: true, created_at: "2026-09-20T00:00:00Z", updated_at: "2026-09-20T00:00:00Z" }), { status: 201, headers: { "Content-Type": "application/json" } });
    return new Response(JSON.stringify({ error: { code: "not_found", message: "not found" } }), { status: 404, headers: { "Content-Type": "application/json" } });
  });
});

it("lists users and creates one without retaining the password", async () => {
  const fetchMock = vi.mocked(globalThis.fetch);
  const user = userEvent.setup();
  render(<AppProviders><App /></AppProviders>);
  expect(await screen.findByRole("heading", { name: "Admin users" })).toBeInTheDocument();
  expect(await screen.findByText("operator@example.com")).toBeInTheDocument();
  await user.type(screen.getByLabelText("Email"), "new@example.com");
  await user.type(screen.getByLabelText("Temporary password"), "one-time-password");
  await user.selectOptions(screen.getByLabelText("Role"), "user");
  await user.click(screen.getByRole("button", { name: "Create user" }));
  const call = fetchMock.mock.calls.find(([path, init]) => String(path) === "/api/v1/admin/users" && init?.method === "POST");
  expect(call?.[1]?.body).toContain("new@example.com");
  expect(document.body.textContent).not.toContain("one-time-password");
});
