import { defineConfig } from "@playwright/test";
import { existsSync } from "node:fs";

export default defineConfig({
  testDir: "./tests",
  fullyParallel: true,
  forbidOnly: Boolean(process.env.CI),
  retries: 0,
  workers: 2,
  use: {
    baseURL: "http://127.0.0.1:4178",
    channel: existsSync("/usr/bin/google-chrome") ? "chrome" : undefined,
    trace: "off",
    screenshot: "off",
    video: "off",
  },
  webServer: {
    command: "npm run dev -- --port 4178 --strictPort",
    cwd: import.meta.dirname,
    url: "http://127.0.0.1:4178",
    reuseExistingServer: false,
  },
});
