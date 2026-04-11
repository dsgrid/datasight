<script lang="ts">
  import { dashboardStore } from "$lib/stores/dashboard.svelte";
  import { saveDashboard } from "$lib/api/dashboard";
  import type { DashboardItem } from "$lib/stores/dashboard.svelte";
  import { sanitizeHtml } from "$lib/utils/markdown";

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

  function handleTitleChange(e: Event) {
    const target = e.target as HTMLInputElement;
    dashboardStore.updateItem(item.id, { title: target.value });
  }

  function toggleFullscreen() {
    dashboardStore.fullscreenCardId = isFullscreen ? null : item.id;
  }

  function removeCard() {
    dashboardStore.removeItem(item.id);
    saveDashboard();
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
    class="flex-1 overflow-auto
      {item.type === 'chart' ? 'min-h-[300px]' : 'max-h-[400px]'}
      {isFullscreen ? '!max-h-none' : ''}"
  >
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
