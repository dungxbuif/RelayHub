import { expect, request, test, type Page } from "@playwright/test";

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

test("authenticates without exposing the bootstrap secret", async ({ page }) => {
  const diagnostics: string[] = [];
  page.on("console", (message) => diagnostics.push(message.text()));
  page.on("requestfailed", (request) => diagnostics.push(`${request.method()} ${new URL(request.url()).pathname} ${request.failure()?.errorText ?? "failed"}`));
  await page.goto("/admin/");
  await signIn(page);

  const leaked = await page.evaluate((secret) => ({
    url: location.href.includes(secret),
    body: document.body.textContent?.includes(secret) ?? false,
    storage: Object.values({ ...localStorage, ...sessionStorage }).some((value) => value.includes(secret)),
  }), adminToken ?? "secret-must-not-exist");
  expect(leaked).toEqual({ url: false, body: false, storage: false });
  expect(diagnostics.some((value) => adminToken ? value.includes(adminToken) : false)).toBe(false);
});

test("serves a deep link and preserves it through explicit reauthentication", async ({ page }) => {
  await page.goto("/admin/events");
  await expect(page.getByRole("heading", { name: "Sign in to Admin" })).toBeVisible();
  await signIn(page);
  await expect(page.getByRole("heading", { name: "Events" })).toBeVisible();

  await page.reload();
  await expect(page.getByRole("heading", { name: "Sign in to Admin" })).toBeVisible();
  await signIn(page);
  await expect(page).toHaveURL(/\/admin\/events$/);
});

test("rejects wrong CSRF and revokes a logged-out cookie", async ({ page, baseURL }) => {
  await page.goto("/admin/");
  await signIn(page);
  const sessionCookie = (await page.context().cookies()).find((cookie) => cookie.name === "__Host-relayhub_admin");
  expect(sessionCookie).toBeTruthy();

  const wrongCSRF = await page.evaluate(async () => {
    const response = await fetch("/api/v1/admin/session", {
      method: "DELETE",
      headers: { "X-RelayHub-CSRF": "deliberately-wrong" },
      credentials: "same-origin",
    });
    return response.status;
  });
  expect(wrongCSRF).toBe(403);

  await page.getByRole("button", { name: "Sign out" }).click();
  await expect(page.getByRole("button", { name: "Sign in" })).toBeVisible();

  const stale = await request.newContext({
    baseURL,
    extraHTTPHeaders: { Cookie: `${sessionCookie?.name}=${sessionCookie?.value}` },
  });
  const response = await stale.get("/api/v1/apps");
  expect(response.status()).toBe(401);
  await stale.dispose();
});

test("supports keyboard navigation", async ({ page, isMobile }) => {
  test.skip(isMobile, "desktop sidebar owns the keyboard navigation contract");
  await page.goto("/admin/");
  await signIn(page);
  await page.keyboard.press("Tab");
  await expect(page.getByRole("link", { name: "Skip to content" })).toBeFocused();
  await page.keyboard.press("Tab");
  await expect(page.getByRole("link", { name: "RelayHub Admin home" })).toBeFocused();
});

test("navigates through the responsive application menu", async ({ page, isMobile }) => {
  await page.goto("/admin/");
  await signIn(page);
  if (isMobile) {
    await page.getByRole("button", { name: "Menu" }).click();
  }
  await page.getByRole("link", { name: "Events" }).click();
  await expect(page.getByRole("heading", { name: "Events" })).toBeVisible();
  await expect(page).toHaveURL(/\/admin\/events$/);
});
