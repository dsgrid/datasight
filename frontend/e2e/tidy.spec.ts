import { test, expect } from "@playwright/test";

/**
 * Drawer E2E.
 *
 * The CI server is launched without a project, so we mock the boot
 * endpoints so the app believes a project is loaded with one wide table.
 * Tidy endpoints are stubbed via page.route so this never depends on a
 * real DuckDB or LLM provider.
 */

const SCHEMA_PAYLOAD = {
  tables: [
    {
      name: "sales",
      row_count: 2,
      columns: [
        { name: "region", dtype: "VARCHAR" },
        { name: "sales_2020", dtype: "INTEGER" },
        { name: "sales_2021", dtype: "INTEGER" },
        { name: "sales_2022", dtype: "INTEGER" },
        { name: "sales_2023", dtype: "INTEGER" },
      ],
    },
  ],
};

const PROPOSAL = {
  pattern: "date_in_column_names",
  table: "sales",
  dimensions: [{ name: "year", kind: "year" }],
  column_mappings: [
    { column: "sales_2020", dimension_values: { year: "2020" } },
    { column: "sales_2021", dimension_values: { year: "2021" } },
    { column: "sales_2022", dimension_values: { year: "2022" } },
    { column: "sales_2023", dimension_values: { year: "2023" } },
  ],
  id_columns: ["region"],
  value_column: "sales",
  target_object_name: "sales_long",
  rationale: "Looks like a wide-by-year table.",
  reshape_sql: "CREATE OR REPLACE TABLE sales_long AS ...",
  confidence: "high",
  source: "deterministic",
  preview_sql: "SELECT region, year, sales FROM sales LIMIT 50",
  reshape_sql_view: "CREATE OR REPLACE VIEW sales_long AS ...",
  reshape_sql_table: "CREATE OR REPLACE TABLE sales_long AS ...",
};

async function mockBoot(page: import("@playwright/test").Page) {
  await page.route("**/api/project", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        loaded: true,
        path: "/tmp/mock_project",
        name: "mock_project",
        is_ephemeral: false,
        has_time_series: false,
        sql_dialect: "duckdb",
      }),
    });
  });
  await page.route("**/api/settings", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        confirm_sql: false,
        explain_sql: false,
        clarify_sql: false,
        show_provenance: false,
        show_cost: true,
        max_history_pairs: 10,
      }),
    });
  });
  await page.route("**/api/settings/llm", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        provider: "anthropic",
        model: "claude-haiku-4-5",
        base_url: "",
        has_api_key: true,
        connected: true,
        env_keys: {},
        env_models: {},
      }),
    });
  });
  await page.route("**/api/schema", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(SCHEMA_PAYLOAD),
    });
  });
  // Endpoints that App.onProjectReady fans out to. Each has its own response
  // shape — returning `[]` or `{}` would crash the sidebar's destructuring.
  const emptyShapes: Array<[string, Record<string, unknown>]> = [
    ["**/api/queries", { queries: [] }],
    ["**/api/recipes", { examples: [], recipes: [] }],
    ["**/api/bookmarks", { bookmarks: [] }],
    ["**/api/reports", { reports: [] }],
    ["**/api/conversations", { conversations: [] }],
    [
      "**/api/dashboard*",
      { items: [], columns: 0, filters: [] },
    ],
    [
      "**/api/measures/editor/catalog",
      { ok: true, measures: [] },
    ],
  ];
  for (const [path, body] of emptyShapes) {
    await page.route(path, async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(body),
      });
    });
  }
  // Per-conversation restore endpoint. App.onMount calls loadConversation
  // with the current session id; if it 404s the boot finally still runs,
  // but stubbing here keeps the flow deterministic.
  await page.route(/\/api\/conversations\/[^/]+$/, async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ events: [], dashboard: { items: [], columns: 0, filters: [] } }),
    });
  });
  // Optional sidebar/health endpoints — return empty objects so they don't 404.
  await page.route("**/api/project-health", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ checks: [] }),
    });
  });
}

test.describe("Tidy review drawer", () => {
  test("opens, shows deterministic proposals, and runs the agent on demand", async ({
    page,
  }) => {
    await mockBoot(page);

    // /api/tidy/detect — synchronous, returns deterministic hits.
    await page.route("**/api/tidy/detect*", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ proposals: [PROPOSAL], error: null }),
      });
    });

    // /api/tidy/propose — fired only when the user clicks "Run agent".
    let proposeCalls = 0;
    await page.route("**/api/tidy/propose", async (route) => {
      proposeCalls += 1;
      const llmProposal = {
        ...PROPOSAL,
        source: "llm",
        target_object_name: "sales_long_llm",
        rationale: "Agent-derived alternative reshape.",
      };
      const body = [
        `event: llm_started\ndata: {}\n\n`,
        `event: llm_proposals\ndata: ${JSON.stringify({
          proposals: [llmProposal],
          parse_warnings: [],
        })}\n\n`,
        `event: done\ndata: {}\n\n`,
      ].join("");
      await route.fulfill({
        status: 200,
        contentType: "text/event-stream",
        body,
      });
    });

    await page.goto("/");

    // Expand the sales table row in the schema inspector.
    const tableRow = page.locator("button.table-header-btn", {
      hasText: "sales",
    });
    await expect(tableRow).toBeVisible({ timeout: 5000 });
    await tableRow.click();

    const tidyBtn = page.locator("button", { hasText: /^Tidy$/ });
    await expect(tidyBtn).toBeVisible();
    await tidyBtn.click();

    const drawer = page.getByRole("dialog", { name: "Tidy review" });
    await expect(drawer).toBeVisible();

    // Deterministic proposal should be visible immediately, without the
    // LLM having been called.
    await expect(
      drawer.locator("text=sales → sales_long").first(),
    ).toBeVisible();
    await expect(drawer.locator("text=regex").first()).toBeVisible();
    expect(proposeCalls).toBe(0);

    // The agent panel offers an explicit Run button.
    const runBtn = drawer.getByRole("button", { name: "Run agent" });
    await expect(runBtn).toBeVisible();
    await runBtn.click();

    // After Run, an LLM proposal arrives and the propose endpoint was hit.
    await expect(
      drawer.locator("text=sales → sales_long_llm").first(),
    ).toBeVisible();
    expect(proposeCalls).toBe(1);
    // The button now offers a re-run.
    await expect(
      drawer.getByRole("button", { name: "Run again" }),
    ).toBeVisible();

    // Esc closes the drawer.
    await page.keyboard.press("Escape");
    await expect(drawer).not.toBeVisible();
  });
});
