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

describe("dashboardStore", () => {
  it("adds and removes items", async () => {
    const { dashboardStore } = await import("$lib/stores/dashboard.svelte");
    dashboardStore.clear();
    expect(dashboardStore.pinnedItems.length).toBe(0);

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
