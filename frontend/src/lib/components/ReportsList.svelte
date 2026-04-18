<script lang="ts">
  import { sidebarStore } from "$lib/stores/sidebar.svelte";
  import {
    deleteReport,
    clearReports,
    runReport,
    updateReport,
  } from "$lib/api/saved";
  import { chatStore } from "$lib/stores/chat.svelte";

  let editingReport = $state<string | null>(null);
  let editName = $state("");
  let editSql = $state("");
  let editVizJson = $state("");
  let runningReport = $state<string | null>(null);

  function startEdit(report: {
    id: string;
    name: string;
    sql: string;
    plotly_spec?: unknown;
  }) {
    editingReport = report.id;
    editName = report.name;
    editSql = report.sql;
    editVizJson = report.plotly_spec
      ? JSON.stringify(report.plotly_spec, null, 2)
      : "";
  }

  function cancelEdit() {
    editingReport = null;
  }

  async function saveEdit(id: string) {
    const updates: { name?: string; sql?: string; plotly_spec?: unknown } = {};
    const orig = sidebarStore.reportsCache.find((r) => r.id === id);
    if (!orig) return;
    if (editName !== orig.name) updates.name = editName;
    if (editSql !== orig.sql) updates.sql = editSql;
    if (editVizJson) {
      try {
        updates.plotly_spec = JSON.parse(editVizJson);
      } catch {
        return; // invalid JSON
      }
    }
    await updateReport(id, updates);
    editingReport = null;
  }

  async function handleRun(id: string) {
    runningReport = id;
    try {
      const result = await runReport(id);
      if (result.ok) {
        chatStore.pushMessage({
          type: "tool_result",
          html: result.html,
          title: result.title,
          resultType: result.type === "chart" ? "chart" : "table",
          plotlySpec: result.plotly_spec,
        });
      } else {
        chatStore.pushMessage({
          type: "error",
          error: result.error || "Report execution failed",
        });
      }
    } finally {
      runningReport = null;
    }
  }
</script>

<div class="space-y-1">
  {#each sidebarStore.reportsCache as report (report.id)}
    {#if editingReport === report.id}
      <!-- Edit form -->
      <div class="p-2 rounded-md border border-border space-y-2">
        <input
          type="text"
          bind:value={editName}
          class="w-full px-2 py-1 text-xs rounded border border-border bg-bg
            text-text-primary focus:outline-none focus:ring-1 focus:ring-teal/40"
          placeholder="Report name"
        />
        <textarea
          bind:value={editSql}
          class="w-full h-20 px-2 py-1 text-xs font-mono rounded border
            border-border bg-bg text-text-primary resize-y
            focus:outline-none focus:ring-1 focus:ring-teal/40"
          placeholder="SQL"
        ></textarea>
        {#if editVizJson || report.plotly_spec}
          <details>
            <summary class="text-[10px] text-text-secondary cursor-pointer">
              Visualization JSON
            </summary>
            <textarea
              bind:value={editVizJson}
              class="w-full h-16 mt-1 px-2 py-1 text-[10px] font-mono rounded
                border border-border bg-bg text-text-primary resize-y
                focus:outline-none focus:ring-1 focus:ring-teal/40"
            ></textarea>
          </details>
        {/if}
        <div class="flex gap-1">
          <button
            class="text-[10px] px-2 py-0.5 rounded bg-teal text-white
              hover:opacity-90 cursor-pointer"
            onclick={() => saveEdit(report.id)}
          >
            Save
          </button>
          <button
            class="text-[10px] px-2 py-0.5 rounded border border-border
              text-text-secondary hover:text-text-primary cursor-pointer"
            onclick={cancelEdit}
          >
            Cancel
          </button>
        </div>
      </div>
    {:else}
      <div class="group relative">
        <button
          class="w-full text-left py-2 pr-12 border-b border-border/30
            hover:bg-teal/[0.04] transition-colors duration-100 cursor-pointer"
          style="padding-left: 16px;"
          onclick={() => handleRun(report.id)}
        >
          <div class="text-text-primary truncate" style="font-size: 0.82rem; line-height: 1.4;">
            <span class="mr-1">{report.plotly_spec ? "📊" : "📋"}</span>
            {report.name || report.sql.slice(0, 50)}
          </div>
          {#if runningReport === report.id}
            <div class="text-[10px] text-teal mt-0.5">Running...</div>
          {/if}
        </button>
        <div
          class="absolute right-1 top-1/2 -translate-y-1/2 flex gap-0.5
            opacity-0 group-hover:opacity-100 transition-opacity"
        >
          <button
            class="text-text-secondary hover:text-teal text-[10px] cursor-pointer px-0.5"
            onclick={() => startEdit(report)}
            title="Edit"
          >
            ✎
          </button>
          <button
            class="text-text-secondary hover:text-red-500 text-xs cursor-pointer px-0.5"
            onclick={() => deleteReport(report.id)}
            title="Delete"
          >
            &times;
          </button>
        </div>
      </div>
    {/if}
  {/each}

  {#if sidebarStore.reportsCache.length === 0}
    <div class="text-[11px] text-text-secondary py-2 text-center">
      No reports saved
    </div>
  {:else}
    <button
      class="text-[10px] text-text-secondary hover:text-red-500
        transition-colors cursor-pointer mt-1"
      onclick={clearReports}
    >
      Clear all
    </button>
  {/if}
</div>
