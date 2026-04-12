<script lang="ts">
  import {
    dashboardStore,
    getAllCardColumns,
    getCardFilterStatus,
    isFilterableCard,
  } from "$lib/stores/dashboard.svelte";
  import { rerunDashboardCards, saveDashboard } from "$lib/api/dashboard";
  import type { DashboardItem } from "$lib/stores/dashboard.svelte";
  import { sanitizeHtml } from "$lib/utils/markdown";
  import { toastStore } from "$lib/stores/toast.svelte";

  interface Props {
    item: DashboardItem;
    index: number;
    onDragStart: (index: number) => void;
    onDragOver: (e: DragEvent, index: number) => void;
    onDrop: (e: DragEvent, index: number) => void;
  }

  let { item, index, onDragStart, onDragOver, onDrop }: Props = $props();

  let dragOver = $state(false);
  let iframeEl = $state<HTMLIFrameElement | null>(null);

  let isFullscreen = $derived(dashboardStore.fullscreenCardId === item.id);

  let skippedFilters = $derived.by(() => {
    if (!isFilterableCard(item)) return [];
    return dashboardStore.filters
      .filter((f) => f.enabled !== false)
      .map((f) => ({ filter: f, status: getCardFilterStatus(item, f) }))
      .filter(({ status }) => status !== "applied");
  });

  let appliedFilterCount = $derived.by(() => {
    if (!isFilterableCard(item)) return 0;
    return dashboardStore.filters.filter(
      (f) => f.enabled !== false && getCardFilterStatus(item, f) === "applied",
    ).length;
  });

  let cardError = $derived(
    typeof item.source_meta?.meta?.error === "string"
      ? (item.source_meta.meta.error as string)
      : "",
  );

  let rowCount = $derived.by(() => {
    const raw = item.source_meta?.meta?.row_count;
    return typeof raw === "number" ? raw : null;
  });

  let emptyAfterFilter = $derived(
    appliedFilterCount > 0 && rowCount === 0 && !cardError,
  );

  let skippedFilterTooltip = $derived(
    skippedFilters
      .map(({ filter, status }) => {
        const reason =
          status === "not_applicable"
            ? "column not in this card"
            : "excluded by scope";
        return `${filter.column} (${reason})`;
      })
      .join("\n"),
  );

  $effect(() => {
    const iframe = iframeEl;
    if (!iframe) return;

    async function handleChartMessage(event: MessageEvent) {
      const sourceWindow = iframe?.contentWindow;
      if (!sourceWindow || event.source !== sourceWindow) return;
      if (event.data?.type !== "datasight-plotly-click") return;
      await applyClickFilter(event.data.point);
    }

    window.addEventListener("message", handleChartMessage);
    return () => window.removeEventListener("message", handleChartMessage);
  });

  function handleTitleChange(e: Event) {
    const target = e.target as HTMLInputElement;
    dashboardStore.updateItem(item.id, { title: target.value });
    saveDashboard();
  }

  function toggleFullscreen() {
    dashboardStore.fullscreenCardId = isFullscreen ? null : item.id;
  }

  function removeCard() {
    dashboardStore.removeItem(item.id);
    saveDashboard();
  }

  function getClickFilterColumn(point: {
    curveNumber?: number;
    x?: unknown;
    label?: unknown;
  }): string | null {
    const spec = item.plotly_spec as
      | { data?: Array<Record<string, unknown>> }
      | null
      | undefined;
    const traces = spec?.data || [];
    const trace = traces[point.curveNumber ?? 0] || traces[0];
    if (!trace) return null;

    if (typeof trace.x === "string" && point.x !== undefined) return trace.x;
    if (typeof trace.labels === "string" && point.label !== undefined) return trace.labels;
    return null;
  }

  function normalizeColumnName(column: string): string {
    return column
      .toLowerCase()
      .replace(/(_agg|_code|_name|_label|_id)$/g, "")
      .replace(/[^a-z0-9]/g, "");
  }

  function resolveSharedFilterColumn(column: string): string | null {
    const filterableColumns = getAllCardColumns(dashboardStore.pinnedItems);
    if (filterableColumns.includes(column)) return column;

    const normalized = normalizeColumnName(column);
    const matches = filterableColumns.filter((candidate) => {
      const normalizedCandidate = normalizeColumnName(candidate);
      return (
        normalizedCandidate === normalized ||
        normalizedCandidate.startsWith(normalized) ||
        normalized.startsWith(normalizedCandidate)
      );
    });
    return matches.length === 1 ? matches[0] : null;
  }

  async function applyClickFilter(point: {
    curveNumber?: number;
    pointNumber?: number;
    x?: unknown;
    y?: unknown;
    label?: unknown;
    value?: unknown;
  }) {
    if (!item.sql) {
      toastStore.show("This card cannot be rerun with filters", "info");
      return;
    }
    const rawColumn = getClickFilterColumn(point);
    const column = rawColumn ? resolveSharedFilterColumn(rawColumn) : null;
    const value = point.x ?? point.label;
    if (!column || value === undefined || value === null) {
      toastStore.show("Could not map this click to a result column", "info");
      return;
    }

    dashboardStore.addFilter({
      column,
      operator: "eq",
      value,
      scope: { type: "all" },
    });
    await rerunDashboardCards();
    toastStore.show(`Filtered dashboard by ${column} = ${String(value)}`, "success");
  }

  function syncThemeToIframe() {
    if (!iframeEl?.contentWindow) return;
    const theme =
      document.documentElement.getAttribute("data-theme") || "light";
    try {
      iframeEl.contentWindow.postMessage(
        { type: "theme-change", theme },
        "*",
      );
    } catch {
      // ignore
    }
  }
</script>

<!-- svelte-ignore a11y_no_static_element_interactions -->
<div
  class="rounded-xl border bg-surface overflow-hidden flex flex-col
    {isFullscreen
    ? 'fixed inset-4 z-[1000] shadow-2xl'
    : ''}
    {dragOver ? 'ring-2 ring-teal' : ''}
    {item.type === 'section' ? 'border-dashed border-border' : 'border-border'}"
  draggable="true"
  ondragstart={() => onDragStart(index)}
  ondragover={(e) => {
    dragOver = true;
    onDragOver(e, index);
  }}
  ondragleave={() => (dragOver = false)}
  ondrop={(e) => {
    dragOver = false;
    onDrop(e, index);
  }}
  ondragend={() => (dragOver = false)}
>
  <!-- Header -->
  <div class="flex items-center gap-1 px-3 py-1.5 border-b border-border">
    <span
      class="cursor-grab text-text-secondary/40 hover:text-text-secondary select-none text-xs"
      title="Drag to reorder"
    >
      ⠿
    </span>
    <input
      type="text"
      value={item.title || ""}
      onchange={handleTitleChange}
      placeholder="Add title..."
      class="flex-1 bg-transparent text-xs font-medium text-text-primary
        placeholder:text-text-secondary/40 focus:outline-none"
    />
    {#if skippedFilters.length > 0}
      <span
        class="text-[10px] px-1.5 py-0.5 rounded border border-border
          text-text-secondary bg-surface-alt whitespace-nowrap"
        title={skippedFilterTooltip}
      >
        {skippedFilters.length} filter{skippedFilters.length === 1 ? "" : "s"} not applied
      </span>
    {/if}
    <button
      class="text-text-secondary/40 hover:text-text-primary text-xs
        cursor-pointer transition-colors
        {isFullscreen ? 'rotate-45' : ''}"
      onclick={toggleFullscreen}
      title={isFullscreen ? "Exit fullscreen" : "Fullscreen"}
    >
      ⛶
    </button>
    <button
      class="text-text-secondary/40 hover:text-red-500 text-xs
        cursor-pointer transition-colors"
      onclick={removeCard}
      title="Remove"
    >
      &times;
    </button>
  </div>

  <!-- Content -->
  <div
    class="flex-1 overflow-auto relative
      {item.type === 'chart' ? 'min-h-[300px]' : 'max-h-[400px]'}
      {isFullscreen ? '!max-h-none' : ''}"
  >
    {#if cardError}
      <div
        class="absolute inset-0 z-10 flex flex-col items-center justify-center
          gap-1 p-4 text-center bg-surface/95 backdrop-blur-sm"
      >
        <div class="text-xs font-medium text-red-600">Rerun failed</div>
        <div class="text-[11px] text-text-secondary max-w-[90%] break-words">
          {cardError}
        </div>
      </div>
    {:else if emptyAfterFilter}
      <div
        class="absolute inset-0 z-10 flex flex-col items-center justify-center
          gap-1 p-4 text-center bg-surface/95 backdrop-blur-sm"
      >
        <div class="text-xs font-medium text-text-primary">No rows after filter</div>
        <div class="text-[11px] text-text-secondary">
          {appliedFilterCount} active filter{appliedFilterCount === 1 ? "" : "s"} excluded every row.
        </div>
      </div>
    {/if}
    {#if item.type === "chart" && item.html}
      <iframe
        bind:this={iframeEl}
        srcdoc={item.html}
        sandbox="allow-scripts allow-same-origin"
        class="w-full h-full min-h-[300px] border-0 dashboard-chart-iframe"
        title={item.title || "Chart"}
        onload={syncThemeToIframe}
      ></iframe>
    {:else if item.type === "table" && item.html}
      <div
        class="p-3 text-xs [&_table]:w-full [&_th]:px-2 [&_th]:py-1
          [&_th]:text-left [&_th]:bg-surface-alt [&_th]:font-medium
          [&_th]:border-b [&_th]:border-border
          [&_td]:px-2 [&_td]:py-1 [&_td]:border-b [&_td]:border-border"
      >
        {@html sanitizeHtml(item.html)}
      </div>
    {/if}
  </div>

  <!-- Source meta (collapsible) -->
  {#if item.source_meta && (item.type === "chart" || item.type === "table")}
    <details class="border-t border-border">
      <summary
        class="px-3 py-1 text-[10px] text-text-secondary cursor-pointer
          hover:text-text-primary"
      >
        Source details
      </summary>
      <div class="px-3 pb-2 text-[10px] text-text-secondary space-y-0.5">
        {#if item.source_meta.question}
          <div>Q: {item.source_meta.question}</div>
        {/if}
        <div>Type: {item.source_meta.resultType}</div>
        {#if item.source_meta.meta}
          {#each Object.entries(item.source_meta.meta) as [k, v]}
            <div>{k}: {v}</div>
          {/each}
        {/if}
      </div>
    </details>
  {/if}
</div>
