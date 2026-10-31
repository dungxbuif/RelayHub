import { expect, test } from "@playwright/test";
import { readOnlyCredentials, signIn, signInAs } from "./auth";

test.beforeEach(async ({ context }) => {
  await context.clearCookies();
});

test("keeps a read-only session away from mutation controls", async ({ page }) => {
  await page.goto("/admin/routing-rules");
  await signInAs(page, readOnlyCredentials());
  await expect(page.getByText("Read-only access")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Create rule" })).not.toBeVisible();
});

test("covers every advertised Admin feature group", async ({ page, isMobile }) => {
  await page.goto("/admin/");
  await signIn(page);

  const featureRoutes = [
    ["Overview", "/admin/"],
    ["Events", "/admin/events"],
    ["Dead Letters", "/admin/dead-letters"],
    ["Queue", "/admin/queue"],
    ["Apps", "/admin/apps"],
    ["Functions", "/admin/functions"],
    ["Routing Rules", "/admin/routing-rules"],
    ["Realtime Studio", "/admin/realtime-studio"],
    ["Realtime resources", "/admin/realtime-resources"],
    ["Audit Logs", "/admin/audit-logs"],
    ["Admin users", "/admin/users"],
    ["System", "/admin/system"],
  ] as const;

  for (const [label, route] of featureRoutes) {
    if (isMobile) await page.getByRole("button", { name: "Menu" }).click();
    await page.getByRole("link", { name: label, exact: true }).click();
    const expectedPath = route.replace(/\/$/, "");
    await expect.poll(() => new URL(page.url()).pathname.replace(/\/$/, "")).toBe(expectedPath);
    await expect(page.locator("main#main-content h1")).toBeVisible();
  }
});
