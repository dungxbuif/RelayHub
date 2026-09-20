import { expect, test, type Page } from "@playwright/test";

const adminToken = process.env.RELAYHUB_E2E_ADMIN_TOKEN;

async function signIn(page: Page): Promise<void> {
  if (!adminToken) throw new Error("RELAYHUB_E2E_ADMIN_TOKEN is required");
  await page.getByLabel("Bootstrap Admin token").fill(adminToken);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("navigation", { name: "Admin sections" })).toBeVisible();
}

test.beforeEach(async ({ context }) => {
  await context.clearCookies();
});

test("renders live cluster metrics and pauses automatic refresh", async ({ page }) => {
  const dashboardResponse = page.waitForResponse((response) => response.url().includes("/api/v1/admin/dashboard?") && response.status() === 200);
  await page.goto("/admin/");
  await signIn(page);
  const response = await dashboardResponse;
  const body = JSON.stringify(await response.json()).toLowerCase();
  for (const forbidden of ["api_key", "authorization", "cookie", "callback_url", "password"]) expect(body).not.toContain(forbidden);

  await expect(page.getByRole("heading", { name: "Request rate" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "HTTP status breakdown" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Delivery latency" })).toBeVisible();
  await page.getByRole("button", { name: "Pause live refresh" }).click();
  await expect(page.getByRole("button", { name: "Resume live refresh" })).toBeVisible();
  await expect(page.getByRole("status")).toContainText("paused");

  const hasHorizontalOverflow = await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth + 1);
  expect(hasHorizontalOverflow).toBe(false);
});

test("keeps operational filters addressable and bounded", async ({ page, isMobile }) => {
  await page.goto("/admin/events");
  await signIn(page);
  await expect(page.getByRole("heading", { name: "Events" })).toBeVisible();
  await page.getByLabel("Event type").fill("acceptance.no-match");
  const filteredEvents = page.waitForResponse((response) => response.url().includes("/api/v1/admin/events?") && response.url().includes("type=acceptance.no-match"));
  await page.getByRole("button", { name: "Apply filters" }).click();
  await filteredEvents;
  await expect(page).toHaveURL(/type=acceptance.no-match/);

  if (isMobile) await page.getByRole("button", { name: "Menu" }).click();
  await page.getByRole("link", { name: "Dead Letters" }).click();
  await expect(page.getByRole("heading", { name: "Dead Letters" })).toBeFocused();
  await page.getByLabel("Sink").selectOption("callback");
  await page.getByRole("button", { name: "Apply filters" }).click();
  await expect(page).toHaveURL(/sink=callback/);

  if (isMobile) await page.getByRole("button", { name: "Menu" }).click();
  await page.getByRole("link", { name: "Audit Logs" }).click();
  await expect(page.getByRole("heading", { name: "Audit Logs" })).toBeFocused();
  await page.getByLabel("Outcome").fill("success");
  await page.getByRole("button", { name: "Apply filters" }).click();
  await expect(page).toHaveURL(/outcome=success/);
});
