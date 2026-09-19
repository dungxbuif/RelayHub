import { expect, test, type Page } from "@playwright/test";

const adminToken = process.env.RELAYHUB_E2E_ADMIN_TOKEN;
async function signIn(page: Page) { if (!adminToken) throw new Error("RELAYHUB_E2E_ADMIN_TOKEN is required"); await page.getByLabel("Bootstrap Admin token").fill(adminToken); await page.getByRole("button", { name: "Sign in" }).click(); }

test.beforeEach(async ({ context }) => { await context.clearCookies(); });

test("confirms an exact DLQ batch and renders a safe lifecycle", async ({ page, isMobile }) => {
  const rows = ["dlv_acceptance_1", "dlv_acceptance_2"].map((delivery_id, index) => ({ delivery_id, job_id: `job_${index}`, event_id: "evt_acceptance", source_app_id: "source", target_app_id: "target", sink: index ? "stream" : "callback", reason: "acceptance_failure", attempts: 3, created_at: "2026-09-20T08:00:00Z", updated_at: "2026-09-20T08:01:00Z" }));
  let replayRequests = 0;
  await page.route("**/api/v1/admin/dlq?**", (route) => route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: rows }) }));
  await page.route("**/api/v1/admin/dlq/replay", async (route) => { replayRequests++; const request = route.request(); expect(request.headers()["idempotency-key"]).toBeTruthy(); expect(request.headers()["x-relayhub-csrf"]).toBeTruthy(); expect(request.postDataJSON()).toEqual({ delivery_ids: ["dlv_acceptance_1", "dlv_acceptance_2"] }); await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: rows.map((row) => ({ delivery_id: row.delivery_id, generation: 2, status: "pending" })) }) }); });
  await page.goto("/admin/dead-letters"); await signIn(page);
  await page.getByRole("checkbox", { name: "Select delivery dlv_acceptance_1" }).check(); await page.getByRole("checkbox", { name: "Select delivery dlv_acceptance_2" }).check();
  await page.getByRole("button", { name: "Replay selected (2)" }).click();
  const dialog = page.getByRole("dialog", { name: "Confirm replay" }); await expect(dialog).toContainText("dlv_acceptance_1"); await expect(dialog).toContainText("dlv_acceptance_2"); await expect(dialog.getByRole("button", { name: "Cancel" })).toBeFocused();
  await dialog.getByRole("button", { name: "Replay 2 deliveries" }).click(); await expect(page.getByRole("status")).toContainText("2 deliveries queued for replay"); expect(replayRequests).toBe(1);
  if (isMobile) expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth + 1)).toBe(true);
});
