import { test, expect } from "@playwright/test";

test.describe("Landing page", () => {
  test("renders the datasight header", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator("header")).toBeVisible();
    await expect(page.locator("header").locator("text=datasight")).toBeVisible();
  });

  test("shows landing page when no project is loaded", async ({ page }) => {
    await page.goto("/");
    // Should see the landing page content
    await expect(
      page.locator("text=datasight").first(),
    ).toBeVisible();
  });

  test("theme toggle changes data-theme attribute", async ({ page }) => {
    await page.goto("/");
    const html = page.locator("html");

    // Get initial theme
    const initialTheme = await html.getAttribute("data-theme");

    // Click theme toggle button
    const themeBtn = page.locator('button[title="Toggle theme"]');
    await themeBtn.click();

    const newTheme = await html.getAttribute("data-theme");
    expect(newTheme).not.toBe(initialTheme);
  });
});

test.describe("Keyboard shortcuts", () => {
  test("Cmd+K opens command palette", async ({ page }) => {
    await page.goto("/");
    await page.keyboard.press("ControlOrMeta+k");
    // Should see the search input placeholder
    await expect(
      page.locator('input[placeholder*="Search commands"]'),
    ).toBeVisible({ timeout: 2000 });
  });

  test("Escape closes command palette", async ({ page }) => {
    await page.goto("/");
    await page.keyboard.press("ControlOrMeta+k");
    await expect(
      page.locator('input[placeholder*="Search commands"]'),
    ).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(
      page.locator('input[placeholder*="Search commands"]'),
    ).not.toBeVisible();
  });
});

test.describe("Settings panel", () => {
  test("Cmd+, opens settings", async ({ page }) => {
    await page.goto("/");
    await page.keyboard.press("ControlOrMeta+,");
    await expect(page.locator("text=Query Behavior")).toBeVisible({
      timeout: 2000,
    });
  });
});
