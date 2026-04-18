// Captures docs screenshots into docs/assets/screenshots/.
//
// Prerequisites:
//   1. `datasight run` is running at http://localhost:8084
//   2. For views that show content (chat, SQL editor, dashboard, chart),
//      load a project first — typically the EIA demo:
//          datasight demo eia-generation ~/datasight-eia-demo
//          datasight run --project-dir ~/datasight-eia-demo
//      and run a couple of queries + pin at least one card to the dashboard
//      so the screenshots aren't empty shells.
//
//   The `landing` test intercepts /api/project and mocks "no project loaded",
//   so it works against the same running server.
//
// Run all:              npm run capture-screenshots
// Run a single view:    npx playwright test screenshots --grep landing

import { test, expect } from "@playwright/test";
import { mkdirSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT_DIR = path.resolve(__dirname, "../../docs/assets/screenshots");

test.use({ viewport: { width: 1280, height: 800 } });

test.beforeAll(() => {
  mkdirSync(OUT_DIR, { recursive: true });
});

// Seed dark mode before the app boots. index.html reads this on first paint.
test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => {
    try {
      localStorage.setItem("datasight-theme", "dark");
    } catch {}
  });
});

async function waitForProjectLoaded(page: import("@playwright/test").Page) {
  // View tabs only render once a project is loaded.
  await expect(page.getByRole("button", { name: "Chat", exact: true })).toBeVisible({
    timeout: 5000,
  });
}

test.describe("screenshots", () => {

test("landing", async ({ page }) => {
  // Force the landing page even if the server has a project loaded.
  await page.route("**/api/project", (route) =>
    route.fulfill({ contentType: "application/json", body: JSON.stringify({ loaded: false }) }),
  );
  await page.goto("/");
  await expect(page.locator(".landing-page")).toBeVisible({ timeout: 5000 });
  // Give the radial gradient background a beat to paint.
  await page.waitForTimeout(300);
  await page.screenshot({ path: path.join(OUT_DIR, "01-landing.png"), fullPage: false });
});

test("chat-view", async ({ page }) => {
  await page.goto("/");
  await waitForProjectLoaded(page);
  await page.getByRole("button", { name: "Chat", exact: true }).click();
  await page.screenshot({ path: path.join(OUT_DIR, "02-chat-view.png") });
});

test("sql-editor", async ({ page }) => {
  await page.goto("/");
  await waitForProjectLoaded(page);
  await page.getByRole("button", { name: "SQL", exact: true }).click();
  // Let CodeMirror render.
  await page.waitForTimeout(400);
  await page.screenshot({ path: path.join(OUT_DIR, "03-sql-editor.png") });
});

test("chart-result", async ({ page }) => {
  // Playwright opens a fresh browser context with an empty chatStore, so we
  // load the most recent saved conversation (expected to contain a chart).
  await page.goto("/");
  await waitForProjectLoaded(page);
  await page.getByRole("button", { name: "Chat", exact: true }).click();

  // Expand the Conversations sidebar section (collapsed by default) and open
  // the first saved one.
  await page.getByRole("button", { name: /Conversations/ }).click();
  const firstConv = page.getByRole("button", { name: /\d+\s+messages/ }).first();
  await expect(
    firstConv,
    "No saved conversations — run a chart-producing query in the UI, then re-run this test",
  ).toBeVisible({ timeout: 3000 });
  await firstConv.click();

  const chart = page.locator(".js-plotly-plot").first();
  await expect(
    chart,
    "Loaded conversation has no Plotly chart — pick a conversation that contains one",
  ).toBeVisible({ timeout: 10000 });
  await chart.scrollIntoViewIfNeeded();
  await page.waitForTimeout(500);
  await page.screenshot({ path: path.join(OUT_DIR, "04-chart-result.png") });
});

test("dashboard", async ({ page }) => {
  await page.goto("/");
  await waitForProjectLoaded(page);
  await page.getByRole("button", { name: /^Dashboard/ }).click();
  await page.waitForTimeout(400);
  await page.screenshot({ path: path.join(OUT_DIR, "05-dashboard.png") });
});

test("web-ui-reference", async ({ page }) => {
  // Full-window baseline for the annotated reference in docs/end-user/reference/web-ui.md.
  // Annotations are added to the exported PNG manually.
  await page.goto("/");
  await waitForProjectLoaded(page);
  await page.getByRole("button", { name: "Chat", exact: true }).click();
  await page.screenshot({ path: path.join(OUT_DIR, "06-web-ui-reference.png"), fullPage: false });
});

});
