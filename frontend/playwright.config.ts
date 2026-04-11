import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "e2e",
  timeout: 30000,
  use: {
    baseURL: "http://localhost:8084",
    headless: true,
  },
  // Don't start a web server — assumes `datasight run` is already running
  webServer: undefined,
});
