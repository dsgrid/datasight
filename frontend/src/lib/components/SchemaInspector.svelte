<script lang="ts">
  import { schemaStore } from "$lib/stores/schema.svelte";
  import { sendMessage } from "$lib/api/chat";
  import { loadPreview, loadColumnStats } from "$lib/api/schema";
  import { getVisibleSchemaEntries } from "$lib/utils/search";
  import type { TableInfo, ColumnInfo } from "$lib/stores/schema.svelte";

  let expandedTable = $state<string | null>(null);
  let expandedColumn = $state<string | null>(null);
  let previewTable = $state<string | null>(null);
  let previewLoading = $state(false);
  let statsLoading = $state<string | null>(null);

  let filteredTables = $derived.by(() => {
    const q = schemaStore.searchQuery.trim();
    if (!q) return schemaStore.schemaData;

    const entries = getVisibleSchemaEntries(schemaStore.schemaData, q);
    // Return matching tables, preserving column info
    const tableNames = new Set(entries.map((e) => e.table.name));
    return schemaStore.schemaData.filter((t) => tableNames.has(t.name));
  });

  function selectTable(name: string) {
    if (schemaStore.selectedTable === name) {
      schemaStore.selectedTable = null;
      expandedTable = null;
    } else {
      schemaStore.selectedTable = name;
      expandedTable = name;
    }
  }

  function toggleTable(name: string) {
    expandedTable = expandedTable === name ? null : name;
  }

  async function showPreview(tableName: string) {
    if (previewTable === tableName) {
      previewTable = null;
      return;
    }
    previewTable = tableName;
    previewLoading = true;
    try {
      await loadPreview(tableName);
    } finally {
      previewLoading = false;
    }
  }

  async function showColumnStats(tableName: string, columnName: string) {
    const key = `${tableName}.${columnName}`;
    if (expandedColumn === key) {
      expandedColumn = null;
      return;
    }
    expandedColumn = key;

    if (!schemaStore.columnStatsCache.get(key)) {
      statsLoading = key;
      try {
        await loadColumnStats(tableName, columnName);
      } finally {
        statsLoading = null;
      }
    }
  }

  function askAboutTable(table: TableInfo) {
    sendMessage(`Describe the ${table.name} table`);
  }

  function askAboutColumn(tableName: string, column: ColumnInfo) {
    sendMessage(
      `What values are in the ${column.name} column of ${tableName}?`,
    );
  }
</script>

<div class="min-w-0 overflow-hidden">
  <!-- Search -->
  <div style="padding: 10px 12px 0;">
    <input
      type="text"
      placeholder="Search tables and columns..."
      bind:value={schemaStore.searchQuery}
      class="w-full border border-border bg-bg
        text-text-primary placeholder:text-text-secondary/60
        focus:outline-none focus:border-teal schema-search-input"
      style="padding: 8px 10px; border-radius: 8px; font-size: 0.8rem; font-family: inherit;"
    />
  </div>

  <!-- Table list -->
  <div>
    {#each filteredTables as table (table.name)}
      {@const isSelected = schemaStore.selectedTable === table.name}
      {@const isExpanded = expandedTable === table.name}

      <div class="table-item" style="border-bottom: 1px solid var(--border);
                   {isSelected ? 'background: rgba(21,168,168,0.08); border-left: 3px solid var(--teal);' : ''}">
        <!-- Table header -->
        <button
          class="flex items-center w-full cursor-pointer select-none table-header-btn"
          style="gap: 8px; padding: 10px 16px; {isSelected ? 'padding-left: 13px;' : ''}"
          onclick={() => selectTable(table.name)}
        >
          <!-- Table icon -->
          <svg class="shrink-0 text-teal" width="16" height="16" viewBox="0 0 16 16" fill="none">
            <rect x="1" y="2" width="14" height="12" rx="2" stroke="currentColor" stroke-width="1.2" />
            <line x1="1" y1="6" x2="15" y2="6" stroke="currentColor" stroke-width="1.2" />
            <line x1="6" y1="6" x2="6" y2="14" stroke="currentColor" stroke-width="1.2" />
          </svg>
          <span class="flex-1 text-left text-text-primary truncate" style="font-size: 0.88rem; font-weight: 600;">
            {table.name}
          </span>
          {#if table.row_count !== undefined}
            <span class="text-text-secondary" style="font-size: 0.7rem; background: var(--bg); padding: 2px 6px; border-radius: 4px; white-space: nowrap;">
              {table.row_count.toLocaleString()}
            </span>
          {/if}
          <!-- Chevron -->
          <svg
            class="text-text-secondary shrink-0 transition-transform duration-200
              {isExpanded ? 'rotate-90' : ''}"
            width="14" height="14" viewBox="0 0 16 16" fill="none"
          >
            <path d="M6 4l4 4-4 4" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" />
          </svg>
        </button>

        <!-- Columns (expanded) -->
        {#if isExpanded}
          <div style="padding: 0 16px 8px 40px;">
            <!-- Quick actions -->
            <div class="inline-flex items-center" style="gap: 6px; margin-bottom: 6px;">
              <button
                class="cursor-pointer transition-all duration-150
                  hover:text-teal hover:border-teal"
                style="border: 1px solid var(--border); background: none; border-radius: 4px;
                       padding: 3px 10px; font-family: inherit; font-size: 0.72rem;
                       color: var(--text-secondary);"
                onclick={() => showPreview(table.name)}
              >
                {previewTable === table.name ? "Hide" : "Preview rows"}
              </button>
              <button
                class="cursor-pointer transition-all duration-150
                  hover:text-teal hover:border-teal"
                style="border: 1px solid var(--border);
                       background: color-mix(in srgb, var(--surface) 82%, var(--bg));
                       border-radius: 999px; padding: 2px 8px; font-family: inherit;
                       font-size: 0.67rem; color: var(--text-secondary);"
                onclick={() => askAboutTable(table)}
              >
                Ask
              </button>
            </div>

            <!-- Preview -->
            {#if previewTable === table.name}
              <div style="margin-top: 8px; max-height: 300px; overflow: auto; font-size: 0.7rem;">
                {#if previewLoading}
                  <div class="text-text-secondary" style="font-size: 0.7rem; padding: 4px 0; font-style: italic;">
                    Loading preview...
                  </div>
                {:else}
                  {@const preview = schemaStore.tablePreviewCache.get(table.name)}
                  {#if preview?.html}
                    <div
                      class="overflow-auto rounded border border-border
                        [&_table]:w-full [&_table]:border-collapse
                        [&_th]:px-2 [&_th]:py-1 [&_th]:text-left [&_th]:bg-surface-alt [&_th]:font-medium
                        [&_td]:px-2 [&_td]:py-1 [&_td]:border-t [&_td]:border-border"
                      style="font-size: 0.7rem;"
                    >
                      {@html preview.html}
                    </div>
                  {:else if preview?.error}
                    <div style="font-size: 0.7rem; color: #e55;">{preview.error}</div>
                  {/if}
                {/if}
              </div>
            {/if}

            <!-- Column list -->
            {#each table.columns as col (col.name)}
              {@const statsKey = `${table.name}.${col.name}`}
              {@const stats = schemaStore.columnStatsCache.get(statsKey)}
              <div>
                <button
                  class="flex flex-wrap items-baseline w-full cursor-pointer group
                    hover:bg-[rgba(21,168,168,0.04)] transition-colors"
                  style="gap: 6px; padding: 3px 0; font-size: 0.78rem; border-radius: 3px;"
                  onclick={() => showColumnStats(table.name, col.name)}
                >
                  <span class="font-mono text-text-primary" style="font-size: 0.76rem;">
                    {col.name}
                  </span>
                  <span
                    class="text-text-secondary"
                    style="font-size: 0.68rem; background: var(--bg); padding: 1px 4px; border-radius: 3px;"
                  >
                    {col.dtype}
                  </span>
                  <!-- svelte-ignore a11y_click_events_have_key_events -->
                  <!-- svelte-ignore a11y_no_static_element_interactions -->
                  <span
                    class="text-teal opacity-0 group-hover:opacity-100
                      transition-opacity cursor-pointer"
                    style="font-size: 0.67rem; margin-left: auto;"
                    onclick={(e: MouseEvent) => {
                      e.stopPropagation();
                      askAboutColumn(table.name, col);
                    }}
                  >
                    ask
                  </span>
                </button>

                <!-- Column stats -->
                {#if expandedColumn === statsKey}
                  <div class="flex flex-wrap text-text-secondary" style="gap: 4px 12px; font-size: 0.68rem; padding: 2px 0 4px;">
                    {#if statsLoading === statsKey}
                      <span style="font-style: italic;">Loading stats...</span>
                    {:else if stats}
                      <span>Distinct: <b class="text-text-primary" style="font-weight: 500;">{stats.distinct}</b></span>
                      <span>Nulls: <b class="text-text-primary" style="font-weight: 500;">{stats.nulls}</b></span>
                      {#if stats.min !== undefined}
                        <span>Min: <b class="text-text-primary" style="font-weight: 500;">{stats.min}</b></span>
                      {/if}
                      {#if stats.max !== undefined}
                        <span>Max: <b class="text-text-primary" style="font-weight: 500;">{stats.max}</b></span>
                      {/if}
                      {#if stats.avg !== undefined}
                        <span>Avg: <b class="text-text-primary" style="font-weight: 500;">{stats.avg}</b></span>
                      {/if}
                    {:else}
                      <span>No stats available</span>
                    {/if}
                  </div>
                {/if}
              </div>
            {/each}
          </div>
        {/if}
      </div>
    {/each}

    {#if filteredTables.length === 0}
      <div class="text-text-secondary italic" style="font-size: 0.82rem; padding: 16px; text-align: center;">
        {schemaStore.searchQuery ? "No matching tables" : "No tables loaded"}
      </div>
    {/if}
  </div>
</div>
