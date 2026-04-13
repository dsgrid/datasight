import { describe, it, expect, vi, beforeEach } from "vitest";

// Note: Svelte stores using $state() runes compile to getter/setter pairs.
// We can test them directly since vitest.config.ts includes the svelte plugin.

describe("sessionStore", () => {
  // We need a fresh import per test to avoid cross-test leakage in some cases,
  // but for read-only checks of the API surface, direct import is fine.
  it("has a sessionId persisted in localStorage", async () => {
    const { sessionStore } = await import("$lib/stores/session.svelte");
    expect(sessionStore.sessionId).toBeTruthy();
    expect(localStorage.getItem("datasight-session")).toBe(
      sessionStore.sessionId,
    );
  });

  it("defaults to projectLoaded false", async () => {
    const { sessionStore } = await import("$lib/stores/session.svelte");
    // After fresh import, projectLoaded should be false
    expect(typeof sessionStore.projectLoaded).toBe("boolean");
  });

  it("reset generates a new sessionId", async () => {
    const { sessionStore } = await import("$lib/stores/session.svelte");
    const oldId = sessionStore.sessionId;
    sessionStore.reset();
    expect(sessionStore.sessionId).not.toBe(oldId);
    expect(sessionStore.projectLoaded).toBe(false);
    expect(sessionStore.currentProjectPath).toBeNull();
  });
});

describe("settingsStore", () => {
  it("defaults provenance display to false and applies API values", async () => {
    const { settingsStore } = await import("$lib/stores/settings.svelte");
    expect(settingsStore.showProvenance).toBe(false);

    settingsStore.applyFromApi({
      confirm_sql: false,
      explain_sql: false,
      clarify_sql: true,
      show_cost: true,
      show_provenance: true,
    });

    expect(settingsStore.showProvenance).toBe(true);
  });
});

describe("dashboardStore", () => {
  it("adds and removes items", async () => {
    const { dashboardStore } = await import("$lib/stores/dashboard.svelte");
    dashboardStore.clear();
    expect(dashboardStore.pinnedItems.length).toBe(0);
    expect(dashboardStore.columns).toBe(0);

    const item = dashboardStore.addItem({ type: "note", title: "Test" });
    expect(item.id).toBeGreaterThan(0);
    expect(dashboardStore.pinnedItems.length).toBe(1);

    dashboardStore.removeItem(item.id);
    expect(dashboardStore.pinnedItems.length).toBe(0);
  });

  it("reorders items", async () => {
    const { dashboardStore } = await import("$lib/stores/dashboard.svelte");
    dashboardStore.clear();
    const a = dashboardStore.addItem({ type: "note", title: "A" });
    const b = dashboardStore.addItem({ type: "note", title: "B" });
    const c = dashboardStore.addItem({ type: "note", title: "C" });

    dashboardStore.reorder(0, 2);
    expect(dashboardStore.pinnedItems[0].title).toBe("B");
    expect(dashboardStore.pinnedItems[1].title).toBe("C");
    expect(dashboardStore.pinnedItems[2].title).toBe("A");
  });

  it("updates items", async () => {
    const { dashboardStore } = await import("$lib/stores/dashboard.svelte");
    dashboardStore.clear();
    const item = dashboardStore.addItem({ type: "table", title: "Old" });
    dashboardStore.updateItem(item.id, { title: "New" });
    expect(dashboardStore.pinnedItems[0].title).toBe("New");
  });

  it("returns the union of filterable card columns", async () => {
    const { getAllCardColumns } = await import("$lib/stores/dashboard.svelte");

    const columns = getAllCardColumns([
      {
        id: 1,
        type: "chart",
        sql: "SELECT state, fuel, total FROM a",
        source_meta: {
          question: "",
          resultType: "chart",
          meta: { columns: ["state", "fuel", "total"] },
        },
      },
      {
        id: 2,
        type: "table",
        sql: "SELECT state, total FROM b",
        source_meta: {
          question: "",
          resultType: "table",
          meta: { columns: ["state", "total"] },
        },
      },
      { id: 3, type: "note", title: "note" },
    ]);

    expect(columns).toEqual(["state", "fuel", "total"]);
  });

  it("classifies filter status per card", async () => {
    const { getCardFilterStatus, filtersForCard } = await import(
      "$lib/stores/dashboard.svelte"
    );

    const cardA = {
      id: 1,
      type: "chart" as const,
      sql: "SELECT state, total FROM a",
      source_meta: {
        question: "",
        resultType: "chart",
        meta: { columns: ["state", "total"] },
      },
    };
    const cardB = {
      id: 2,
      type: "table" as const,
      sql: "SELECT fuel, total FROM b",
      source_meta: {
        question: "",
        resultType: "table",
        meta: { columns: ["fuel", "total"] },
      },
    };

    const stateFilter = {
      id: 1,
      column: "state",
      operator: "eq" as const,
      value: "CA",
      scope: { type: "all" as const },
    };
    const fuelScoped = {
      id: 2,
      column: "fuel",
      operator: "eq" as const,
      value: "NG",
      scope: { type: "cards" as const, cardIds: [2] },
    };
    const totalExcluded = {
      id: 3,
      column: "total",
      operator: "gt" as const,
      value: 10,
      scope: { type: "cards" as const, cardIds: [2] },
    };

    expect(getCardFilterStatus(cardA, stateFilter)).toBe("applied");
    expect(getCardFilterStatus(cardB, stateFilter)).toBe("not_applicable");
    expect(getCardFilterStatus(cardA, fuelScoped)).toBe("excluded_by_scope");
    expect(getCardFilterStatus(cardB, fuelScoped)).toBe("applied");
    expect(getCardFilterStatus(cardA, totalExcluded)).toBe("excluded_by_scope");

    expect(
      filtersForCard(cardA, [stateFilter, fuelScoped, totalExcluded]).map(
        (f) => f.id,
      ),
    ).toEqual([1]);
    expect(
      filtersForCard(cardB, [stateFilter, fuelScoped, totalExcluded]).map(
        (f) => f.id,
      ),
    ).toEqual([2, 3]);
  });

  it("skips disabled filters when narrowing per card", async () => {
    const { filtersForCard } = await import("$lib/stores/dashboard.svelte");

    const card = {
      id: 1,
      type: "chart" as const,
      sql: "SELECT state FROM a",
      source_meta: {
        question: "",
        resultType: "chart",
        meta: { columns: ["state"] },
      },
    };
    const disabled = {
      id: 1,
      column: "state",
      operator: "eq" as const,
      value: "CA",
      scope: { type: "all" as const },
      enabled: false,
    };
    expect(filtersForCard(card, [disabled])).toEqual([]);
  });
});

describe("paletteStore", () => {
  it("toggles open/close and resets on close", async () => {
    const { paletteStore } = await import("$lib/stores/palette.svelte");
    expect(paletteStore.open).toBe(false);

    paletteStore.toggle();
    expect(paletteStore.open).toBe(true);

    paletteStore.query = "test";
    paletteStore.results = [
      {
        type: "action",
        group: "Actions",
        title: "Test",
        score: 100,
        run: () => {},
      },
    ];
    paletteStore.selectedIdx = 0;

    paletteStore.toggle();
    expect(paletteStore.open).toBe(false);
    expect(paletteStore.query).toBe("");
    expect(paletteStore.results.length).toBe(0);
    expect(paletteStore.selectedIdx).toBe(0);
  });

  it("moveSelection wraps around", async () => {
    const { paletteStore } = await import("$lib/stores/palette.svelte");
    paletteStore.results = [
      {
        type: "a",
        group: "A",
        title: "1",
        score: 1,
        run: () => {},
      },
      {
        type: "a",
        group: "A",
        title: "2",
        score: 1,
        run: () => {},
      },
      {
        type: "a",
        group: "A",
        title: "3",
        score: 1,
        run: () => {},
      },
    ];
    paletteStore.selectedIdx = 0;

    paletteStore.moveSelection(-1);
    expect(paletteStore.selectedIdx).toBe(2); // wraps to end

    paletteStore.moveSelection(1);
    expect(paletteStore.selectedIdx).toBe(0); // wraps to start
  });

  it("executeSelected calls run and closes", async () => {
    const { paletteStore } = await import("$lib/stores/palette.svelte");
    const fn = vi.fn();
    paletteStore.open = true;
    paletteStore.results = [
      {
        type: "a",
        group: "A",
        title: "Test",
        score: 1,
        run: fn,
      },
    ];
    paletteStore.selectedIdx = 0;

    paletteStore.executeSelected();
    expect(fn).toHaveBeenCalledOnce();
    expect(paletteStore.open).toBe(false);
  });
});

describe("queriesStore", () => {
  it("adds queries and tracks cost", async () => {
    const { queriesStore } = await import("$lib/stores/queries.svelte");
    queriesStore.clear();

    queriesStore.addQuery({
      tool: "run_sql",
      sql: "SELECT 1",
      timestamp: new Date().toISOString(),
    });
    expect(queriesStore.sessionQueries.length).toBe(1);

    queriesStore.addCost(0.05);
    expect(queriesStore.sessionTotalCost).toBe(0.05);
    expect(queriesStore.sessionQueries[0].turn_cost).toBe(0.05);
  });

  it("prepends queries (most recent first)", async () => {
    const { queriesStore } = await import("$lib/stores/queries.svelte");
    queriesStore.clear();

    queriesStore.addQuery({
      tool: "run_sql",
      sql: "SELECT 1",
      timestamp: "t1",
    });
    queriesStore.addQuery({
      tool: "run_sql",
      sql: "SELECT 2",
      timestamp: "t2",
    });
    expect(queriesStore.sessionQueries[0].sql).toBe("SELECT 2");
    expect(queriesStore.sessionQueries[1].sql).toBe("SELECT 1");
  });
});

describe("conversation replay", () => {
  it("hydrates chat messages and query history from persisted events", async () => {
    const { replayConversationEvents } = await import("$lib/utils/conversation");

    const replay = replayConversationEvents([
      { event: "user_message", data: { text: "Generation by fuel?" } },
      { event: "assistant_message", data: { text: "I'll query it." } },
      {
        event: "tool_start",
        data: {
          tool: "visualize_data",
          input: {
            sql: "SELECT fuel_type_code_agg, SUM(net_generation_mwh) FROM generation_fuel GROUP BY 1",
            plotly_spec: { type: "bar" },
          },
        },
      },
      {
        event: "tool_result",
        data: { type: "chart", html: "<div>chart</div>", title: "Generation" },
      },
      {
        event: "tool_done",
        data: {
          tool: "visualize_data",
          sql: "SELECT fuel_type_code_agg, SUM(net_generation_mwh) FROM generation_fuel GROUP BY 1",
          execution_time_ms: 12,
          row_count: 4,
          column_count: 2,
          columns: ["fuel_type_code_agg", "net_generation_mwh"],
          validation: { status: "passed", errors: [] },
          timestamp: "2026-04-12T00:00:00Z",
        },
      },
      {
        event: "provenance",
        data: {
          model: "claude-test",
          dialect: "duckdb",
          tools: [
            {
              tool: "visualize_data",
              formatted_sql:
                "SELECT fuel_type_code_agg, SUM(net_generation_mwh) FROM generation_fuel GROUP BY 1",
              validation: { status: "passed", errors: [] },
              execution: {
                status: "success",
                execution_time_ms: 12,
                row_count: 4,
                column_count: 2,
                columns: ["fuel_type_code_agg", "net_generation_mwh"],
              },
            },
          ],
          llm: { api_calls: 2, input_tokens: 100, output_tokens: 50 },
        },
      },
      { event: "suggestions", data: { suggestions: ["Show coal by month"] } },
    ]);

    expect(replay.messages.map((message) => message.type)).toEqual([
      "user_message",
      "assistant_message",
      "tool_start",
      "tool_result",
      "tool_done",
      "provenance",
      "suggestions",
    ]);
    const provenance = replay.messages[5];
    expect(provenance.type).toBe("provenance");
    if (provenance.type === "provenance") {
      expect(provenance.provenance.model).toBe("claude-test");
      expect(provenance.provenance.tools[0].validation?.status).toBe("passed");
    }
    expect(replay.queries).toEqual([
      {
        tool: "visualize_data",
        sql: "SELECT fuel_type_code_agg, SUM(net_generation_mwh) FROM generation_fuel GROUP BY 1",
        timestamp: "2026-04-12T00:00:00Z",
        execution_time_ms: 12,
        row_count: 4,
        column_count: 2,
        error: undefined,
      },
    ]);
  });
});

describe("chatStore", () => {
  it("pushes and removes messages", async () => {
    const { chatStore } = await import("$lib/stores/chat.svelte");
    chatStore.clear();

    chatStore.pushMessage({ type: "user_message", content: "hello" });
    chatStore.pushMessage({
      type: "assistant_message",
      content: "hi",
    });
    expect(chatStore.messages.length).toBe(2);

    chatStore.removeMessage(0);
    expect(chatStore.messages.length).toBe(1);
    expect(chatStore.messages[0].type).toBe("assistant_message");
  });

  it("clear resets all state", async () => {
    const { chatStore } = await import("$lib/stores/chat.svelte");
    chatStore.pushMessage({ type: "user_message", content: "test" });
    chatStore.lastSql = "SELECT 1";

    chatStore.clear();
    expect(chatStore.messages.length).toBe(0);
    expect(chatStore.lastSql).toBe("");
  });
});
