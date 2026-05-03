<script lang="ts">
  import {
    dashboardStore,
    getAllCardColumns,
    getCardColumns,
    getValidDashboardFilters,
    isFilterableCard,
  } from "$lib/stores/dashboard.svelte";
  import {
    clearDashboard,
    loadDashboardFilterValues,
    rerunDashboardCards,
    saveDashboard,
  } from "$lib/api/dashboard";
  import type {
    DashboardFilter,
    DashboardFilterScope,
    DashboardItem,
  } from "$lib/stores/dashboard.svelte";
  import { toastStore } from "$lib/stores/toast.svelte";

  let exporting = $state(false);
  let filterColumn = $state("");
  let filterOperator = $state("eq");
  let filterValue = $state("");
  let filterValueOptions = $state<string[]>([]);
  let loadingFilterValues = $state(false);
  let filterValuesColumn = $state("");
  let filterableColumns = $derived.by(() =>
    getAllCardColumns(dashboardStore.pinnedItems),
  );
  let expandedFilterId = $state<number | null>(null);
  let newFilterScope = $state<DashboardFilterScope>({ type: "all" });
  let newScopeMenuOpen = $state(false);

  function filterableCardsForColumn(column: string): DashboardItem[] {
    return dashboardStore.pinnedItems.filter(
      (item) => isFilterableCard(item) && getCardColumns(item).includes(column),
    );
  }

  function scopeLabel(filter: DashboardFilter): string {
    if (filter.scope.type === "all") {
      const applicable = filterableCardsForColumn(filter.column).length;
      return applicable === 1 ? "1 card" : `${applicable} cards`;
    }
    const n = filter.scope.cardIds.length;
    return n === 1 ? "1 card" : `${n} cards`;
  }

  async function setFilterScopeAll(filter: DashboardFilter) {
    dashboardStore.updateFilter(filter.id, { scope: { type: "all" } });
    await rerunDashboardCards();
  }

  async function setFilterScopeSelected(filter: DashboardFilter) {
    const current =
      filter.scope.type === "cards"
        ? filter.scope.cardIds
        : filterableCardsForColumn(filter.column).map((c) => c.id);
    dashboardStore.updateFilter(filter.id, {
      scope: { type: "cards", cardIds: current },
    });
    await rerunDashboardCards();
  }

  async function toggleFilterEnabled(filter: DashboardFilter) {
    dashboardStore.updateFilter(filter.id, { enabled: filter.enabled === false });
    await rerunDashboardCards();
  }

  async function toggleFilterCard(filter: DashboardFilter, cardId: number) {
    if (filter.scope.type !== "cards") return;
    const set = new Set(filter.scope.cardIds);
    if (set.has(cardId)) set.delete(cardId);
    else set.add(cardId);
    dashboardStore.updateFilter(filter.id, {
      scope: { type: "cards", cardIds: [...set] },
    });
    await rerunDashboardCards();
  }

  function newScopeLabel(): string {
    if (newFilterScope.type === "all") return "All cards";
    const n = newFilterScope.cardIds.length;
    return n === 1 ? "1 card" : `${n} cards`;
  }

  function setNewScopeAll() {
    newFilterScope = { type: "all" };
  }

  function setNewScopeSelected() {
    const ids = filterColumn
      ? filterableCardsForColumn(filterColumn).map((c) => c.id)
      : [];
    newFilterScope = { type: "cards", cardIds: ids };
  }

  function toggleNewScopeCard(cardId: number) {
    if (newFilterScope.type !== "cards") return;
    const set = new Set(newFilterScope.cardIds);
    if (set.has(cardId)) set.delete(cardId);
    else set.add(cardId);
    newFilterScope = { type: "cards", cardIds: [...set] };
  }
  let filterValueListId = $derived(
    `dashboard-filter-values-${filterColumn.replace(/[^A-Za-z0-9_-]/g, "-")}`,
  );

  const COLUMN_OPTIONS = [
    { value: 0, label: "Auto" },
    { value: 1, label: "1" },
    { value: 2, label: "2" },
    { value: 3, label: "3" },
  ];

  $effect(() => {
    if (filterableColumns.length === 0) {
      filterColumn = "";
    } else if (!filterableColumns.includes(filterColumn)) {
      filterColumn = filterableColumns[0];
    }
  });

  // Reset the new-filter scope whenever the target column changes.
  $effect(() => {
    filterColumn;
    newFilterScope = { type: "all" };
  });

  $effect(() => {
    const validFilters = getValidDashboardFilters(dashboardStore.filters, filterableColumns);
    if (validFilters.length !== dashboardStore.filters.length) {
      dashboardStore.filters = validFilters;
      void saveDashboard();
    }
  });

  $effect(() => {
    const column = filterColumn;
    if (!column) {
      filterValueOptions = [];
      filterValuesColumn = "";
      return;
    }

    let cancelled = false;
    loadingFilterValues = true;
    filterValuesColumn = column;
    loadDashboardFilterValues(column)
      .then((result) => {
        if (cancelled || filterValuesColumn !== column) return;
        filterValueOptions = result.ok
          ? result.values.map((value) => String(value))
          : [];
      })
      .catch(() => {
        if (!cancelled) filterValueOptions = [];
      })
      .finally(() => {
        if (!cancelled && filterValuesColumn === column) {
          loadingFilterValues = false;
        }
      });

    return () => {
      cancelled = true;
    };
  });

  function setColumns(cols: number) {
    dashboardStore.columns = cols;
    saveDashboard();
  }

  function handleTitleBlur() {
    void saveDashboard();
  }

  function handleTitleKeydown(e: KeyboardEvent) {
    if (e.key === "Enter") {
      e.preventDefault();
      (e.currentTarget as HTMLInputElement).blur();
    }
  }

  function addNote() {
    dashboardStore.addItem({
      type: "note",
      title: "",
      markdown: "",
    });
    saveDashboard();
  }

  function addSection() {
    dashboardStore.addItem({
      type: "section",
      title: "",
      markdown: "",
    });
    saveDashboard();
  }

  function syncScales() {
    // Collect all chart iframes and unify y-axis ranges
    const iframes = document.querySelectorAll<HTMLIFrameElement>(
      ".dashboard-chart-iframe",
    );
    if (iframes.length < 2) {
      toastStore.show("Need at least 2 charts to sync scales", "info");
      return;
    }

    let globalMin = Infinity;
    let globalMax = -Infinity;

    for (const iframe of iframes) {
      try {
        const win = iframe.contentWindow as Window & {
          Plotly?: { d3: unknown };
          document: Document;
        };
        const plotEl = win.document.querySelector(".js-plotly-plot") as HTMLElement & {
          data?: Array<{ y?: number[] }>;
        };
        if (plotEl?.data) {
          for (const trace of plotEl.data) {
            if (trace.y) {
              for (const v of trace.y) {
                if (typeof v === "number") {
                  globalMin = Math.min(globalMin, v);
                  globalMax = Math.max(globalMax, v);
                }
              }
            }
          }
        }
      } catch {
        // cross-origin or no Plotly
      }
    }

    if (globalMin === Infinity) return;

    const padding = (globalMax - globalMin) * 0.05;
    const range = [globalMin - padding, globalMax + padding];

    for (const iframe of iframes) {
      try {
        const win = iframe.contentWindow as Window & {
          Plotly?: { relayout: (el: HTMLElement, update: unknown) => void };
          document: Document;
        };
        const plotEl = win.document.querySelector(
          ".js-plotly-plot",
        ) as HTMLElement;
        if (plotEl && win.Plotly) {
          win.Plotly.relayout(plotEl, {
            "yaxis.range": range,
            "yaxis.autorange": false,
          });
        }
      } catch {
        // ignore
      }
    }

    toastStore.show("Y-axis scales synced", "success");
  }

  async function handleExportDashboard() {
    if (dashboardStore.pinnedItems.length === 0) {
      toastStore.show("No items to export", "info");
      return;
    }
    exporting = true;
    try {
      const res = await fetch("/api/dashboard/export", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          items: dashboardStore.pinnedItems,
          columns: dashboardStore.columns || 2,
          title: dashboardStore.title.trim(),
          filters: dashboardStore.filters.filter((f) => f.enabled !== false),
        }),
      });
      if (!res.ok) {
        toastStore.show("Export failed", "error");
        return;
      }
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "datasight-dashboard.html";
      a.click();
      URL.revokeObjectURL(url);
      toastStore.show("Dashboard exported", "success");
    } catch {
      toastStore.show("Export failed", "error");
    } finally {
      exporting = false;
    }
  }

  async function handleSave() {
    await saveDashboard();
    toastStore.show("Dashboard saved", "success");
  }

  async function handleClear() {
    await clearDashboard();
    toastStore.show("Dashboard cleared", "success");
  }

  async function addFilter() {
    if (!filterColumn.trim() || !filterValue.trim()) {
      toastStore.show("Filter needs a column and value", "info");
      return;
    }
    if (!filterableColumns.includes(filterColumn)) {
      toastStore.show("Choose a column from a dashboard card", "info");
      return;
    }
    dashboardStore.addFilter({
      column: filterColumn.trim(),
      operator: filterOperator as DashboardFilter["operator"],
      value: filterValue.trim(),
      scope: newFilterScope,
    });
    await rerunDashboardCards();
    filterValue = "";
    newFilterScope = { type: "all" };
    newScopeMenuOpen = false;
    toastStore.show("Dashboard filter applied", "success");
  }

  async function removeFilter(id: number) {
    dashboardStore.removeFilter(id);
    await rerunDashboardCards();
  }

  async function clearFilters() {
    dashboardStore.clearFilters();
    await rerunDashboardCards();
    toastStore.show("Dashboard filters cleared", "success");
  }
</script>

<div class="dashboard-toolbar">
  <!-- Title -->
  <input
    class="toolbar-field dashboard-title-input"
    type="text"
    placeholder="Dashboard title (optional)"
    title="Title shown on the exported HTML page. Leave blank to omit."
    bind:value={dashboardStore.title}
    onblur={handleTitleBlur}
    onkeydown={handleTitleKeydown}
  />

  <!-- Layout -->
  <span class="text-[10px] uppercase tracking-wider text-text-secondary font-semibold">
    Layout
  </span>
  <div class="toolbar-group">
    {#each COLUMN_OPTIONS as opt}
      <button
        class="toolbar-button compact {dashboardStore.columns === opt.value ? 'primary' : ''}"
        onclick={() => setColumns(opt.value)}
      >
        {opt.label}
      </button>
    {/each}
  </div>

  {#if dashboardStore.filters.length > 0}
    <div class="toolbar-group active-filters">
      {#each dashboardStore.filters as filter (filter.id)}
        <div class="filter-chip-wrap">
          <div
            class="toolbar-chip filter-chip {filter.enabled === false
              ? 'filter-chip-disabled'
              : ''}"
          >
            <button
              class="filter-chip-label"
              title={filter.enabled === false
                ? "Filter disabled — click to edit"
                : "Edit filter scope"}
              onclick={() =>
                (expandedFilterId =
                  expandedFilterId === filter.id ? null : filter.id)}
            >
              <span>{filter.column} {filter.operator} {String(filter.value)}</span>
              <span class="filter-chip-scope">· {scopeLabel(filter)}</span>
            </button>
            <button
              class="filter-chip-remove"
              title="Remove filter"
              aria-label="Remove filter"
              onclick={() => removeFilter(filter.id)}
            >
              ×
            </button>
          </div>
          {#if expandedFilterId === filter.id}
            {@const applicableCards = filterableCardsForColumn(filter.column)}
            <div class="filter-scope-menu">
              <label class="filter-scope-radio filter-enabled-toggle">
                <input
                  type="checkbox"
                  checked={filter.enabled !== false}
                  onchange={() => toggleFilterEnabled(filter)}
                />
                <span>Enabled</span>
              </label>
              <div class="filter-scope-label">Applies to</div>
              <label class="filter-scope-radio">
                <input
                  type="radio"
                  name="scope-{filter.id}"
                  checked={filter.scope.type === "all"}
                  onchange={() => setFilterScopeAll(filter)}
                />
                <span>All applicable cards ({applicableCards.length})</span>
              </label>
              <label class="filter-scope-radio">
                <input
                  type="radio"
                  name="scope-{filter.id}"
                  checked={filter.scope.type === "cards"}
                  onchange={() => setFilterScopeSelected(filter)}
                />
                <span>Selected cards</span>
              </label>
              {#if filter.scope.type === "cards"}
                <div class="filter-scope-cards">
                  {#each applicableCards as card (card.id)}
                    <label class="filter-scope-card">
                      <input
                        type="checkbox"
                        checked={filter.scope.type === "cards" &&
                          filter.scope.cardIds.includes(card.id)}
                        onchange={() => toggleFilterCard(filter, card.id)}
                      />
                      <span>{card.title || `Card #${card.id}`}</span>
                    </label>
                  {:else}
                    <div class="filter-scope-empty">No cards have this column.</div>
                  {/each}
                </div>
              {/if}
            </div>
          {/if}
        </div>
      {/each}
    </div>
  {/if}

  <div class="toolbar-spacer"></div>

  <div class="toolbar-group filter-controls">
    <select
      class="toolbar-field"
      bind:value={filterColumn}
      title="Result column to filter"
      disabled={filterableColumns.length === 0}
    >
      {#if filterableColumns.length === 0}
        <option value="">No columns available</option>
      {:else}
        {#each filterableColumns as column}
          <option value={column}>{column}</option>
        {/each}
      {/if}
    </select>
    <select
      class="toolbar-field operator"
      bind:value={filterOperator}
      title="Filter operator"
    >
      <option value="eq">=</option>
      <option value="neq">!=</option>
      <option value="gt">&gt;</option>
      <option value="gte">&gt;=</option>
      <option value="lt">&lt;</option>
      <option value="lte">&lt;=</option>
      <option value="contains">contains</option>
    </select>
    <input
      class="toolbar-field"
      bind:value={filterValue}
      placeholder="value"
      title="Filter value"
      list={filterValueOptions.length > 0 ? filterValueListId : undefined}
      onkeydown={(e) => {
        if (e.key === "Enter") addFilter();
      }}
    />
    {#if filterValueOptions.length > 0}
      <datalist id={filterValueListId}>
        {#each filterValueOptions as value}
          <option value={value}></option>
        {/each}
      </datalist>
    {/if}
    {#if loadingFilterValues}
      <span class="toolbar-hint">Loading values...</span>
    {:else if filterColumn && filterValueOptions.length > 0}
      <span class="toolbar-hint">{filterValueOptions.length} values</span>
    {/if}
    <div class="new-scope-wrap">
      <button
        class="toolbar-button"
        onclick={() => (newScopeMenuOpen = !newScopeMenuOpen)}
        disabled={filterableColumns.length === 0}
        title="Choose which cards this filter applies to"
      >
        Scope: {newScopeLabel()} ▾
      </button>
      {#if newScopeMenuOpen}
        {@const applicableCards = filterColumn
          ? filterableCardsForColumn(filterColumn)
          : []}
        <div class="filter-scope-menu">
          <div class="filter-scope-label">Applies to</div>
          <label class="filter-scope-radio">
            <input
              type="radio"
              name="new-scope"
              checked={newFilterScope.type === "all"}
              onchange={setNewScopeAll}
            />
            <span>All applicable cards ({applicableCards.length})</span>
          </label>
          <label class="filter-scope-radio">
            <input
              type="radio"
              name="new-scope"
              checked={newFilterScope.type === "cards"}
              onchange={setNewScopeSelected}
            />
            <span>Selected cards</span>
          </label>
          {#if newFilterScope.type === "cards"}
            <div class="filter-scope-cards">
              {#each applicableCards as card (card.id)}
                <label class="filter-scope-card">
                  <input
                    type="checkbox"
                    checked={newFilterScope.type === "cards" &&
                      newFilterScope.cardIds.includes(card.id)}
                    onchange={() => toggleNewScopeCard(card.id)}
                  />
                  <span>{card.title || `Card #${card.id}`}</span>
                </label>
              {:else}
                <div class="filter-scope-empty">
                  {filterColumn
                    ? "No cards have this column."
                    : "Pick a column first."}
                </div>
              {/each}
            </div>
          {/if}
        </div>
      {/if}
    </div>
    <button
      class="toolbar-button"
      onclick={addFilter}
      disabled={filterableColumns.length === 0}
    >
      Add Filter
    </button>
    {#if dashboardStore.filters.length > 0}
      <button
        class="toolbar-button danger"
        onclick={clearFilters}
      >
        Clear Filters
      </button>
    {/if}
  </div>

  <!-- Actions -->
  <button
    class="toolbar-button"
    onclick={addNote}
  >
    + Note
  </button>
  <button
    class="toolbar-button"
    onclick={addSection}
  >
    + Section
  </button>
  <button
    class="toolbar-button"
    onclick={syncScales}
  >
    Sync Scales
  </button>

  <div class="toolbar-separator"></div>

  <button
    class="toolbar-button primary"
    onclick={handleSave}
  >
    Save
  </button>
  <button
    class="toolbar-button"
    onclick={handleExportDashboard}
    disabled={exporting}
  >
    {exporting ? "Exporting..." : "Export"}
  </button>
  <button
    class="toolbar-button danger"
    onclick={handleClear}
  >
    Clear Dashboard
  </button>
</div>

<style>
  .dashboard-toolbar {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 8px;
    margin: 14px;
    padding: 12px 14px;
    border: 1px solid var(--border);
    border-radius: 8px;
    background: color-mix(in srgb, var(--surface) 92%, var(--bg) 8%);
    box-shadow: 0 10px 26px rgba(2, 61, 96, 0.05);
  }

  .toolbar-group {
    display: inline-flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 6px;
  }

  .toolbar-spacer {
    flex: 1 1 24px;
  }

  .filter-controls {
    padding: 4px;
    border: 1px solid color-mix(in srgb, var(--teal) 18%, var(--border));
    border-radius: 8px;
    background: color-mix(in srgb, var(--surface-alt) 70%, transparent);
  }

  .active-filters {
    min-width: 0;
  }

  .toolbar-button,
  .toolbar-chip,
  .toolbar-field {
    height: 30px;
    border: 1px solid var(--border);
    border-radius: 6px;
    font-family: inherit;
    font-size: 0.75rem;
    line-height: 1;
    transition: border-color 0.15s, background 0.15s, color 0.15s, opacity 0.15s;
  }

  .toolbar-button,
  .toolbar-chip {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 4px;
    padding: 0 10px;
    color: var(--text-secondary);
    background: var(--surface-alt);
    cursor: pointer;
  }

  .toolbar-button:hover,
  .toolbar-chip:hover {
    color: var(--text-primary);
    border-color: color-mix(in srgb, var(--teal) 35%, var(--border));
    background: color-mix(in srgb, var(--surface-alt) 72%, var(--teal) 8%);
  }

  .toolbar-button.primary {
    color: white;
    border-color: var(--teal);
    background: var(--teal);
  }

  .toolbar-button.primary:hover {
    color: white;
    opacity: 0.9;
  }

  .toolbar-button.danger {
    background: transparent;
  }

  .toolbar-button.danger:hover,
  .toolbar-chip:hover {
    color: #dc2626;
    border-color: color-mix(in srgb, #dc2626 35%, var(--border));
  }

  .toolbar-button.compact {
    min-width: 30px;
    padding: 0 8px;
  }

  .toolbar-button:disabled {
    cursor: not-allowed;
    opacity: 0.55;
  }

  .toolbar-chip {
    max-width: 320px;
    color: var(--teal);
    background: color-mix(in srgb, var(--teal) 10%, var(--surface));
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .filter-chip-wrap,
  .new-scope-wrap {
    position: relative;
  }

  .filter-chip {
    padding: 0;
    gap: 0;
  }

  .filter-chip-disabled {
    opacity: 0.55;
    text-decoration: line-through;
    text-decoration-color: color-mix(in srgb, var(--teal) 50%, transparent);
  }

  .filter-enabled-toggle {
    margin-bottom: 8px;
    padding-bottom: 8px;
    border-bottom: 1px solid var(--border);
  }

  .filter-chip-label {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 0 8px 0 10px;
    height: 100%;
    background: transparent;
    border: none;
    color: inherit;
    font: inherit;
    cursor: pointer;
  }

  .filter-chip-scope {
    color: color-mix(in srgb, var(--teal) 60%, var(--text-secondary));
    font-size: 0.7rem;
  }

  .filter-chip-remove {
    height: 100%;
    padding: 0 8px;
    background: transparent;
    border: none;
    border-left: 1px solid color-mix(in srgb, var(--teal) 20%, transparent);
    color: inherit;
    font: inherit;
    font-size: 0.9rem;
    cursor: pointer;
  }

  .filter-chip-remove:hover {
    color: #dc2626;
    background: color-mix(in srgb, #dc2626 8%, transparent);
  }

  .filter-scope-menu {
    position: absolute;
    z-index: 20;
    top: calc(100% + 4px);
    left: 0;
    min-width: 220px;
    padding: 10px 12px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    box-shadow: 0 10px 28px rgba(2, 61, 96, 0.14);
    font-size: 0.75rem;
  }

  .filter-scope-label {
    font-weight: 600;
    color: var(--text-secondary);
    margin-bottom: 6px;
    text-transform: uppercase;
    font-size: 0.65rem;
    letter-spacing: 0.04em;
  }

  .filter-scope-radio,
  .filter-scope-card {
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 3px 0;
    color: var(--text-primary);
    cursor: pointer;
  }

  .filter-scope-cards {
    margin-top: 6px;
    padding-top: 6px;
    border-top: 1px solid var(--border);
    max-height: 200px;
    overflow-y: auto;
  }

  .filter-scope-empty {
    color: var(--text-secondary);
    font-style: italic;
    padding: 4px 0;
  }

  .toolbar-field {
    width: 128px;
    padding: 0 9px;
    color: var(--text-primary);
    background: var(--surface);
    outline: none;
  }

  .toolbar-field.operator {
    width: 84px;
  }

  .dashboard-title-input {
    width: 240px;
    font-weight: 600;
  }

  .toolbar-hint {
    color: var(--text-secondary);
    font-size: 0.7rem;
    white-space: nowrap;
  }

  .toolbar-field:focus {
    border-color: var(--teal);
    box-shadow: 0 0 0 2px color-mix(in srgb, var(--teal) 16%, transparent);
  }

  .toolbar-separator {
    width: 1px;
    height: 24px;
    background: var(--border);
  }

  @media (max-width: 780px) {
    .toolbar-spacer {
      display: none;
    }

    .filter-controls {
      width: 100%;
    }

    .toolbar-field {
      flex: 1 1 120px;
    }
  }
</style>
