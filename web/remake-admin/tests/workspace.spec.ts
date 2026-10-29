import { test, expect, type Page } from "@playwright/test";

const workspace = "11111111-1111-4111-8111-111111111111";
const other = "22222222-2222-4222-8222-222222222222";
const app = "33333333-3333-4333-8333-333333333333";
const endpoint = "44444444-4444-4444-8444-444444444444";
const event = "55555555-5555-4555-8555-555555555555";
const delivery = "66666666-6666-4666-8666-666666666666";
const replayed = "77777777-7777-4777-8777-777777777777";
const actor = "88888888-8888-4888-8888-888888888888";
const base = `/api/v1/workspaces/${workspace}`;
const hostile = '<img src=x onerror="window.pwned=true">';
const input = { endpoint_id: endpoint, event_type: "dashboard.test", method: "POST", path: "/", headers: [], body: Buffer.from(hostile).toString("base64") };
const original = { delivery_id: delivery, event_id: event, generation: 1, state: "dlq", attempts: 5, input };
type Call = { path: string; method: string; body: any; headers: Record<string, string> };

async function backend(page: Page, admin = false) {
  const state = {
    calls: [] as Call[], policy: { mode: "async" } as Record<string, unknown>,
    ack: false, auditForbidden: false, eventUnavailable: false, noAudit: false, expiredToken: false, forbidden: false, failReplay: false,
  };
  await page.addInitScript(() => {
    const fetch = window.fetch.bind(window);
    (window as any).fetchOptions = [];
    window.fetch = (url, init) => {
      (window as any).fetchOptions.push({ credentials: init?.credentials, url: String(url) });
      return fetch(url, init);
    };
  });
  await page.route("**/api/v1/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname;
    const method = request.method();
    const body = request.postData() ? request.postDataJSON() : undefined;
    state.calls.push({ path: path + url.search, method, body, headers: request.headers() });
    const reply = (json: unknown, status = 200) => route.fulfill({ status, json, headers: { "Cache-Control": "no-store" } });
    if (path === "/api/v1/session") {
      if (method === "POST") return reply({ token: "SESSION-SECRET", csrf_token: "test-csrf", expires_in_seconds: 86400 });
      if (method === "DELETE") return route.fulfill({ status: 204 });
      return reply(admin ? { kind: "system_admin", user_id: actor } : { kind: "workspace_owner", user_id: actor, workspace_id: workspace });
    }
    if (path.includes(other) || state.forbidden) return reply({ error: "forbidden SECRET-IN-ERROR" }, 403);
    if (method !== "GET" && request.headers()["x-csrf-token"] !== "test-csrf") return reply({ error: "csrf_required" }, 403);
    if (path === "/api/v1/admin/users") return reply({ user_id: actor, workspace_id: other, setup_token: "SETUP-SECRET", expires_in_seconds: 86400 }, 201);
    if (path === `${base}/apps`) return reply(method === "POST" ? { id: app, workspace_id: workspace, name: body.name } : [{ id: app, workspace_id: workspace, name: "Example app" }], method === "POST" ? 201 : 200);
    if (path === `${base}/credentials`) return reply({ id: actor, workspace_id: workspace, app_id: app, kind: body.kind, agent_id: null, token: "APP-SECRET" }, 201);
    if (path === `${base}/endpoints`) return reply(method === "POST" ? { id: endpoint, app_id: app, policy: body.policy, auth: "bearer", ingress_secret: "INGRESS-SECRET" } : [{ id: endpoint, app_id: app, policy: state.policy, auth: "bearer" }]);
    if (path === `${base}/endpoints/${endpoint}/policy`) { state.policy = body; return reply({ id: endpoint, app_id: app, policy: body, auth: "bearer" }); }
    if (path === `${base}/agents`) return reply([{ agent_id: actor, app_id: app, online: false }]);
    if (path === `${base}/deliveries`) return reply({ items: url.searchParams.has("cursor") ? [{ ...original, delivery_id: replayed, generation: 2, state: "succeeded" }] : [original], next_cursor: url.searchParams.has("cursor") ? null : "opaque/+?=" });
    if (path === `${base}/deliveries/${delivery}/replay`) return state.failReplay ? reply({ error: "conflict" }, 409) : reply({ event_id: event, delivery_id: replayed, status: 202 }, 202);
    if (path === `${base}/deliveries/${replayed}/audit` && state.auditForbidden) return reply({ error: "forbidden" }, 403);
    if (path === `${base}/deliveries/${replayed}/audit`) return reply(state.noAudit ? [] : [{ delivery_id: replayed, source_delivery_id: delivery, actor_user_id: actor, created_at: "2026-10-29T00:00:00Z" }]);
    if (path === `${base}/realtime-tokens`) return reply({ token: "SCOPED-SECRET", workspace_id: workspace, channel: body.channel, expires_in_seconds: state.expiredToken ? 0 : 60 });
    if (path === "/api/v1/realtime") return reply({ status: 202, observed_recipients: 0 }, 202);
    if (path === `${base}/events` && state.eventUnavailable) return reply({ error: "unavailable" }, 503);
    if (path === `${base}/events`) return reply({ event_id: event, delivery_id: delivery, status: 202 }, 202);
    if (path === `/api/v1/events/${event}`) return reply({ ...original, state: state.ack ? "succeeded" : "pending", attempts: state.ack ? 1 : 0, response_state: "none", response: null });
    return reply({ error: "unmocked_route" }, 404);
  });
  return state;
}

async function login(page: Page) {
  await page.goto("/login");
  await page.getByLabel("Email", { exact: true }).fill("owner@example.test");
  await page.getByLabel("Password", { exact: true }).fill("test-password-123");
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
}
async function acknowledge(page: Page, secret: string) {
  const dialog = page.getByRole("dialog", { name: "Store secret securely" });
  await expect(dialog).toContainText(secret);
  await expect(dialog.getByRole("button", { name: "Close secret" })).toBeDisabled();
  await page.keyboard.press("Escape");
  await expect(dialog).toBeVisible();
  await dialog.getByLabel("I stored this secret securely.").check();
  await dialog.getByRole("button", { name: "Close secret" }).click();
  await expect(page.locator("body")).not.toContainText(secret);
  expect(await page.evaluate(() => [JSON.stringify(localStorage), JSON.stringify(sessionStorage)])).toEqual(["{}", "{}"]);
  expect(page.url()).not.toContain(secret);
}

test("user_cannot_open_other_workspace", async ({ page }) => {
  const state = await backend(page);
  await login(page);
  await expect(page.getByRole("heading", { name: "Workspace", exact: true })).toBeVisible();
  await page.goto(`/workspaces/${other}`);
  await expect(page.getByRole("alert")).toContainText("403");
  await expect(page.getByRole("button", { name: "Create app", exact: true })).toHaveCount(0);
  await expect(page.locator("body")).not.toContainText("SECRET-IN-ERROR");
  await page.goto(`/workspaces/${other}/deliveries`);
  await expect(page.getByRole("alert")).toContainText("403");
  await page.goto("/admin/users");
  await expect(page.getByRole("alert")).toContainText("403");
  expect(state.calls.filter(c => c.path.includes("/admin/users"))).toHaveLength(0);
});

test("admin_creates_user", async ({ page }) => {
  const state = await backend(page, true);
  await login(page);
  await page.getByLabel("New user email").fill("new@example.test");
  await page.getByRole("button", { name: "Create user", exact: true }).click();
  await expect(page.getByRole("status")).toContainText(other);
  await acknowledge(page, "SETUP-SECRET");
  expect(state.calls.find(c => c.path === "/api/v1/admin/users")?.body).toEqual({ email: "new@example.test" });
  await page.getByLabel("Workspace ID").fill(other);
  await page.getByRole("button", { name: "Open workspace", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("403");
  await page.getByRole("button", { name: "Sign out", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Sign in", exact: true })).toBeVisible();
  expect(state.calls.find(c => c.method === "DELETE")?.headers["x-csrf-token"]).toBe("test-csrf");
  expect(await page.evaluate(() => (window as any).fetchOptions.every((o: any) => o.credentials === "include"))).toBe(true);
});

test("configure_sync_offline_policy", async ({ page }) => {
  const state = await backend(page);
  await login(page);
  await expect(page.getByText("Offline", { exact: true })).toBeVisible();
  await page.getByLabel("App name").fill("New app");
  await page.getByRole("button", { name: "Create app", exact: true }).click();
  await expect(page.getByRole("status")).toContainText("App created");
  await page.getByRole("button", { name: "Create credential", exact: true }).click();
  await acknowledge(page, "APP-SECRET");
  await page.getByRole("button", { name: "Create endpoint", exact: true }).click();
  await acknowledge(page, "INGRESS-SECRET");
  await page.getByLabel("Webhook mode").selectOption("sync");
  await page.getByLabel("Sync timeout (seconds)").fill("30");
  await page.getByLabel("Offline policy").selectOption("reject");
  await page.getByLabel("Timeout policy").selectOption("expire_pending");
  await page.getByRole("button", { name: "Save policy", exact: true }).click();
  await expect(page.getByRole("status")).toContainText("Policy saved");
  expect(state.policy).toEqual({ mode: "sync", timeout_seconds: 30, offline: "reject", timeout: "expire_pending" });
  await page.getByLabel("Offline policy").selectOption("queue");
  await page.getByLabel("Timeout policy").selectOption("continue");
  await page.getByRole("button", { name: "Save policy", exact: true }).click();
  await expect.poll(() => state.policy.offline).toBe("queue");
  await page.getByLabel("Sync timeout (seconds)").fill("31");
  const count = state.calls.filter(c => c.method === "PUT").length;
  await page.getByRole("button", { name: "Save policy", exact: true }).click();
  expect(state.calls.filter(c => c.method === "PUT")).toHaveLength(count);
  await page.reload();
  await expect(page.getByRole("link", { name: "Sign in to enable changes" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Create credential", exact: true })).toBeDisabled();
});

test("replay_is_audited", async ({ page }) => {
  const state = await backend(page);
  await login(page);
  await page.getByRole("link", { name: "Deliveries", exact: true }).click();
  await expect(page.getByText(hostile, { exact: true })).toBeVisible();
  expect(await page.evaluate(() => (window as any).pwned)).toBeUndefined();
  await page.getByRole("button", { name: "Next page", exact: true }).click();
  await expect(page.getByRole("article").getByText("ACK (succeeded)", { exact: true })).toBeVisible();
  const cursorCall = state.calls.find(c => c.path.includes("cursor="));
  expect(new URL(cursorCall!.path, "http://local").searchParams.get("cursor")).toBe("opaque/+?=");
  await page.getByLabel("Delivery state").selectOption("dlq");
  await expect(page.getByRole("article").getByText("dlq", { exact: true })).toBeVisible();
  expect(state.calls.at(-1)?.path).not.toContain("cursor=");
  await page.getByRole("button", { name: "Replay", exact: true }).click();
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  expect(state.calls.filter(c => c.path.endsWith("/replay"))).toHaveLength(0);
  await page.getByRole("button", { name: "Replay", exact: true }).click();
  await page.getByRole("button", { name: "Confirm replay", exact: true }).click();
  await expect(page.getByRole("status")).toContainText(`Audited replay: ${delivery} → ${replayed}`);
  await expect(page.getByRole("status")).toContainText(actor);
  await expect(page.getByRole("button", { name: "Replay", exact: true })).toBeDisabled();
  await page.getByRole("button", { name: "Refresh deliveries", exact: true }).click();
  state.noAudit = true;
  await page.getByRole("button", { name: "Replay", exact: true }).click();
  await page.getByRole("button", { name: "Confirm replay", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("audit evidence unavailable");
  await expect(page.getByRole("status")).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Replay", exact: true })).toBeDisabled();
  await page.getByRole("button", { name: "Refresh deliveries", exact: true }).click();
  state.failReplay = true;
  await page.getByRole("button", { name: "Replay", exact: true }).click();
  await page.getByRole("button", { name: "Confirm replay", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("409");
  state.failReplay = false;
  state.auditForbidden = true;
  await page.getByRole("button", { name: "Refresh deliveries", exact: true }).click();
  await page.getByRole("button", { name: "Replay", exact: true }).click();
  await page.getByRole("button", { name: "Confirm replay", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("403");
  await expect(page.getByRole("article")).toHaveCount(0);
});

test("offline_realtime_test_shows_zero", async ({ page }) => {
  const state = await backend(page);
  await login(page);
  await page.getByLabel("Realtime channel").fill("dashboard-test");
  await page.getByLabel("Test payload").fill("hello");
  await page.getByRole("button", { name: "Send realtime test", exact: true }).click();
  await expect(page.getByRole("status")).toContainText("0 recipients");
  await expect(page.getByRole("status")).toContainText("not a processing ACK");
  const publish = state.calls.find(c => c.path === "/api/v1/realtime")!;
  expect(publish.headers.authorization).toBe("Bearer SCOPED-SECRET");
  expect(publish.body).toMatchObject({ type: "realtime", version: 1, channel: "dashboard-test", body: "aGVsbG8=" });
  expect(state.calls.find(c => c.path.endsWith("/realtime-tokens"))?.body).toEqual({ channel: "dashboard-test", expires_in_seconds: 60 });
  expect(state.calls.some(c => c.path.includes("/credentials") || c.path.endsWith("/events"))).toBe(false);
  await expect(page.locator("body")).not.toContainText("SCOPED-SECRET");
  expect(state.calls.some(c => c.path.includes("SECRET"))).toBe(false);
  state.expiredToken = true;
  await page.getByRole("button", { name: "Send realtime test", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("Invalid realtime token scope or expiry");
  expect(state.calls.filter(c => c.path === "/api/v1/realtime")).toHaveLength(1);
});

test("durable_test_moves_pending_to_ack", async ({ page }) => {
  const state = await backend(page);
  await login(page);
  await page.getByLabel("Test payload").fill("durable hello");
  state.eventUnavailable = true;
  await page.getByRole("button", { name: "Send durable test", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("503");
  state.eventUnavailable = false;
  await page.getByRole("button", { name: "Send durable test", exact: true }).click();
  await expect(page.getByRole("status")).toContainText("pending");
  await expect(page.getByRole("status")).toContainText(event);
  const publish = state.calls.find(c => c.path === `${base}/events`)!;
  expect(publish.body).toEqual({ ...input, body: Buffer.from("durable hello").toString("base64") });
  expect(publish.headers["idempotency-key"]).toMatch(/^[0-9a-f-]{36}$/);
  expect(publish.headers.authorization).toBeUndefined();
  state.ack = true;
  await page.getByRole("button", { name: "Refresh durable status", exact: true }).click();
  await expect(page.getByRole("status")).toContainText("ACK (succeeded)");
  expect(state.calls.filter(c => c.path === `${base}/events`)).toHaveLength(2);
  expect(state.calls.filter(c => c.path === `${base}/events`).map(c => c.headers["idempotency-key"])).toEqual([publish.headers["idempotency-key"], publish.headers["idempotency-key"]]);
  expect(state.calls.some(c => c.path.includes("realtime"))).toBe(false);
});
