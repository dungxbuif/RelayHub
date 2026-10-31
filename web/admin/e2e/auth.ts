import { expect, type Page } from "@playwright/test";

export function adminCredentials(): { email: string; password: string } {
  const email = process.env.RELAYHUB_E2E_ADMIN_EMAIL;
  const password = process.env.RELAYHUB_E2E_ADMIN_PASSWORD;
  if (!email || !password) throw new Error("RELAYHUB_E2E_ADMIN_EMAIL and RELAYHUB_E2E_ADMIN_PASSWORD are required");
  return { email, password };
}

export function readOnlyCredentials(): { email: string; password: string } {
  const email = process.env.RELAYHUB_E2E_READONLY_EMAIL;
  const password = process.env.RELAYHUB_E2E_READONLY_PASSWORD;
  if (!email || !password) throw new Error("RELAYHUB_E2E_READONLY_EMAIL and RELAYHUB_E2E_READONLY_PASSWORD are required");
  return { email, password };
}

export async function signInAs(page: Page, credentials: { email: string; password: string }): Promise<void> {
  await page.getByLabel("Email").fill(credentials.email);
  await page.getByLabel("Password").fill(credentials.password);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("navigation", { name: "Admin sections" })).toBeVisible();
}

export async function signIn(page: Page): Promise<void> {
  await signInAs(page, adminCredentials());
}
