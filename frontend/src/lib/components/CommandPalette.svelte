<script lang="ts">
  import { paletteStore } from "$lib/stores/palette.svelte";
  import type { PaletteResult } from "$lib/stores/palette.svelte";
  import { schemaStore } from "$lib/stores/schema.svelte";
  import { sidebarStore } from "$lib/stores/sidebar.svelte";
  import { sessionStore } from "$lib/stores/session.svelte";
  import { dashboardStore } from "$lib/stores/dashboard.svelte";
  import { chatStore } from "$lib/stores/chat.svelte";
  import { sendMessage } from "$lib/api/chat";
  import { loadPreview } from "$lib/api/schema";
  import {
    loadDatasetOverview,
    loadMeasureOverview,
    loadDimensionOverview,
    loadQualityOverview,
    loadTrendOverview,
    loadTimeseriesOverview,
  } from "$lib/api/starters";
  import { scorePaletteResult, highlightMatch } from "$lib/utils/search";
  import { tick } from "svelte";

  interface Props {
    onToggleSettings: () => void;
    onToggleSidebar: () => void;
    onNewChat: () => void;
  }

  let { onToggleSettings, onToggleSidebar, onNewChat }: Props = $props();

  let inputEl = $state<HTMLInputElement | null>(null);
  let resultsEl = $state<HTMLElement | null>(null);

  // Focus input when opened
  $effect(() => {
    if (paletteStore.open) {
      tick().then(() => inputEl?.focus());
    }
  });

  // Update results when query changes
  $effect(() => {
    const q = paletteStore.query;
    paletteStore.results = buildResults(q);
  });

  // Scroll selected into view
  $effect(() => {
    const idx = paletteStore.selectedIdx;
    if (resultsEl) {
      const selected = resultsEl.querySelector(`[data-idx="${idx}"]`);
      selected?.scrollIntoView({ block: "nearest" });
    }
  });

  /** Run a deterministic starter and push the overview panel into chat. */
  async function runStarter(
    kind: string,
    loader: (table?: string) => Promise<{ overview: Record<string, unknown> }>,
  ) {
    const table = schemaStore.selectedTable || undefined;
    try {
      const result = await loader(table);
      if (result.overview) {
        chatStore.pushMessage({
          type: "starter_overview",
          kind,
          overview: result.overview,
        });
      }
    } catch {
      // Starter failed silently
    }
  }

  function buildResults(query: string): PaletteResult[] {
    const results: PaletteResult[] = [];

    // Actions
    const actions: Array<{
      title: string;
      subtitle: string;
      score: number;
      run: () => void;
    }> = [
      {
        title: "Switch to Chat",
        subtitle: "View",
        score: 800,
        run: () => (dashboardStore.currentView = "chat"),
      },
      {
        title: "Switch to Dashboard",
        subtitle: "View",
        score: 800,
        run: () => (dashboardStore.currentView = "dashboard"),
      },
      {
        title: "Open Settings",
        subtitle: "Panel",
        score: 740,
        run: onToggleSettings,
      },
      {
        title: "Toggle Sidebar",
        subtitle: "Panel",
        score: 720,
        run: onToggleSidebar,
      },
      {
        title: "New Conversation",
        subtitle: "Action",
        score: 760,
        run: onNewChat,
      },
      {
        title: "Summarize Dataset",
        subtitle: "Starter",
        score: 780,
        run: () => runStarter("profile", loadDatasetOverview),
      },
      {
        title: "Inspect Measures",
        subtitle: "Starter",
        score: 760,
        run: () => runStarter("measures", loadMeasureOverview),
      },
      {
        title: "Inspect Dimensions",
        subtitle: "Starter",
        score: 740,
        run: () => runStarter("dimensions", loadDimensionOverview),
      },
      {
        title: "Show Trends",
        subtitle: "Starter",
        score: 720,
        run: () => runStarter("trend", loadTrendOverview),
      },
      {
        title: "Data Quality Check",
        subtitle: "Starter",
        score: 720,
        run: () => runStarter("quality", loadQualityOverview),
      },
      ...(sessionStore.hasTimeSeries ? [{
        title: "Time Series Check",
        subtitle: "Starter",
        score: 710,
        run: () => runStarter("timeseries", loadTimeseriesOverview),
      }] : []),
    ];

    for (const a of actions) {
      const score = query ? scorePaletteResult(query, [a.title, a.subtitle], a.score) : a.score;
      if (score > 0) {
        results.push({
          type: "action",
          group: "Actions",
          title: a.title,
          subtitle: a.subtitle,
          score,
          run: a.run,
        });
      }
    }

    // Tables
    for (const table of schemaStore.schemaData) {
      const subtitle = `${table.columns.length} columns${table.row_count ? ` · ${table.row_count.toLocaleString()} rows` : ""}`;
      const score = query
        ? scorePaletteResult(query, [table.name], 620)
        : 620;
      if (score > 0) {
        results.push({
          type: "table",
          group: "Tables",
          title: table.name,
          subtitle,
          score,
          run: () => {
            schemaStore.selectedTable = table.name;
            sidebarStore.sidebarOpen = true;
          },
        });
        // Preview sub-action
        if (score > 0) {
          results.push({
            type: "table-action",
            group: "Tables",
            title: `Preview ${table.name}`,
            subtitle: "Preview rows",
            score: score - 20,
            run: () => loadPreview(table.name),
          });
        }
      }
    }

    // Columns
    for (const table of schemaStore.schemaData) {
      for (const col of table.columns) {
        const subtitle = `${table.name} · ${col.dtype}`;
        const score = query
          ? scorePaletteResult(query, [col.name, table.name + " " + col.name], 560)
          : 560;
        if (score > 0) {
          results.push({
            type: "column",
            group: "Columns",
            title: col.name,
            subtitle,
            score,
            run: () => {
              schemaStore.selectedTable = table.name;
              sidebarStore.sidebarOpen = true;
            },
          });
        }
      }
    }

    // Bookmarks
    for (const bm of sidebarStore.bookmarksCache) {
      const title = bm.name || bm.sql.slice(0, 50);
      const score = query ? scorePaletteResult(query, [title, bm.sql], 520) : 520;
      if (score > 0) {
        results.push({
          type: "bookmark",
          group: "Bookmarks",
          title,
          subtitle: "Bookmark",
          score,
          run: () =>
            sendMessage(
              `Run this SQL query and display the results as a table:\n${bm.sql}`,
            ),
        });
      }
    }

    // Recipes
    for (const recipe of schemaStore.recipesCache) {
      const score = query
        ? scorePaletteResult(query, [recipe.title, recipe.category || "Recipe"], 540)
        : 540;
      if (score > 0) {
        results.push({
          type: "recipe",
          group: "Recipes",
          title: recipe.title,
          subtitle: recipe.category || "Recipe",
          score,
          run: () => sendMessage(recipe.prompt),
        });
      }
    }

    // Conversations
    for (const conv of sidebarStore.conversationsCache) {
      const score = query
        ? scorePaletteResult(query, [conv.title], 510)
        : 510;
      if (score > 0) {
        results.push({
          type: "conversation",
          group: "Conversations",
          title: conv.title,
          subtitle: `${conv.message_count} messages`,
          score,
          run: () => {
            // Will be handled by ConversationsList
          },
        });
      }
    }

    // Sort by score desc, then alphabetically
    results.sort((a, b) => b.score - a.score || a.title.localeCompare(b.title));
    return results.slice(0, 60);
  }

  function handleKeydown(e: KeyboardEvent) {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      paletteStore.moveSelection(1);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      paletteStore.moveSelection(-1);
    } else if (e.key === "Enter") {
      e.preventDefault();
      paletteStore.executeSelected();
    } else if (e.key === "Escape") {
      e.preventDefault();
      paletteStore.open = false;
    }
  }

  // Group results for rendering
  let groupedResults = $derived.by(() => {
    const groups: Array<{ name: string; items: Array<PaletteResult & { globalIdx: number }> }> =
      [];
    let globalIdx = 0;
    const groupMap = new Map<string, Array<PaletteResult & { globalIdx: number }>>();

    for (const r of paletteStore.results) {
      if (!groupMap.has(r.group)) {
        groupMap.set(r.group, []);
      }
      groupMap.get(r.group)!.push({ ...r, globalIdx });
      globalIdx++;
    }

    for (const [name, items] of groupMap) {
      groups.push({ name, items });
    }
    return groups;
  });
</script>

{#if paletteStore.open}
  <!-- svelte-ignore a11y_no_static_element_interactions -->
  <!-- svelte-ignore a11y_click_events_have_key_events -->
  <!-- svelte-ignore a11y_no_static_element_interactions -->
  <div
    class="fixed inset-0 z-[110]"
    onclick={() => (paletteStore.open = false)}
  ></div>

  <!-- svelte-ignore a11y_no_static_element_interactions -->
  <div
    class="fixed z-[120] bg-surface border border-border overflow-hidden animate-fade-in"
    style="top: 12vh; left: 50%; transform: translateX(-50%);
           width: min(720px, calc(100vw - 32px)); max-height: 70vh;
           border-radius: 16px; box-shadow: 0 24px 60px rgba(0,0,0,0.22);"
    onkeydown={handleKeydown}
  >
    <!-- Search input -->
    <div class="flex items-center border-b border-border" style="gap: 10px; padding: 14px 16px;">
      <span class="text-text-secondary" style="font-size: 1rem;">&#128269;</span>
      <input
        bind:this={inputEl}
        type="text"
        placeholder="Search commands, tables, bookmarks..."
        bind:value={paletteStore.query}
        class="flex-1 bg-transparent text-text-primary
          placeholder:text-text-secondary focus:outline-none"
        style="border: none; font-family: inherit; font-size: 0.95rem;"
      />
      <button
        class="text-text-secondary hover:text-text-primary cursor-pointer"
        style="background: none; border: none; font-size: 1.4rem; line-height: 1;"
        onclick={() => (paletteStore.open = false)}
      >
        &times;
      </button>
    </div>

    <!-- Results -->
    <div
      bind:this={resultsEl}
      class="overflow-y-auto"
      style="max-height: calc(70vh - 58px); padding: 8px;"
    >
      {#if paletteStore.results.length === 0}
        <div class="text-center text-text-secondary" style="padding: 24px 16px; font-size: 0.85rem;">
          {paletteStore.query ? "No results found" : "Type to search..."}
        </div>
      {:else}
        {#each groupedResults as group}
          <div
            class="text-text-secondary font-semibold uppercase"
            style="padding: 10px 14px 6px; font-size: 0.72rem; letter-spacing: 0.04em;"
          >
            {group.name}
          </div>
          {#each group.items as item (item.globalIdx)}
            <button
              data-idx={item.globalIdx}
              class="w-full flex items-center text-left cursor-pointer transition-colors
                {paletteStore.selectedIdx === item.globalIdx
                ? 'bg-teal/10 text-teal'
                : 'text-text-primary hover:bg-surface-alt'}"
              style="gap: 12px; padding: 12px 14px; border: none; border-radius: 10px;
                     background: {paletteStore.selectedIdx === item.globalIdx ? '' : 'transparent'};
                     font-family: inherit; font-size: 0.85rem;"
              onclick={() => {
                paletteStore.selectedIdx = item.globalIdx;
                paletteStore.executeSelected();
              }}
              onmouseenter={() =>
                (paletteStore.selectedIdx = item.globalIdx)}
            >
              <div class="flex-1 min-w-0">
                <div class="truncate">
                  {#if paletteStore.query}
                    {@html highlightMatch(item.title, paletteStore.query)}
                  {:else}
                    {item.title}
                  {/if}
                </div>
                {#if item.subtitle}
                  <div class="text-text-secondary truncate" style="font-size: 0.72rem;">
                    {item.subtitle}
                  </div>
                {/if}
              </div>
            </button>
          {/each}
        {/each}
      {/if}
    </div>
  </div>
{/if}
