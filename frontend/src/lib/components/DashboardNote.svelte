<script lang="ts">
  import { dashboardStore } from "$lib/stores/dashboard.svelte";
  import { saveDashboard } from "$lib/api/dashboard";
  import { renderMarkdown } from "$lib/utils/markdown";
  import type { DashboardItem } from "$lib/stores/dashboard.svelte";

  interface Props {
    item: DashboardItem;
    index: number;
    onDragStart: (index: number) => void;
    onDragOver: (e: DragEvent, index: number) => void;
    onDrop: (e: DragEvent, index: number) => void;
  }

  let { item, index, onDragStart, onDragOver, onDrop }: Props = $props();

  let dragOver = $state(false);
  // svelte-ignore state_referenced_locally
  let markdown = $state(item.markdown || "");

  let renderedHtml = $derived(renderMarkdown(markdown));

  function handleInput() {
    dashboardStore.updateItem(item.id, { markdown });
  }

  function handleBlur() {
    saveDashboard();
  }

  function handleTitleChange(e: Event) {
    const target = e.target as HTMLInputElement;
    dashboardStore.updateItem(item.id, { title: target.value });
  }

  function removeCard() {
    dashboardStore.removeItem(item.id);
    saveDashboard();
  }
</script>

<!-- svelte-ignore a11y_no_static_element_interactions -->
<div
  class="rounded-xl border border-border bg-surface overflow-hidden
    {dragOver ? 'ring-2 ring-teal' : ''}"
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
    >
      ⠿
    </span>
    <input
      type="text"
      value={item.title || ""}
      onchange={handleTitleChange}
      placeholder="Note title..."
      class="flex-1 bg-transparent text-xs font-medium text-text-primary
        placeholder:text-text-secondary/40 focus:outline-none"
    />
    <button
      class="text-text-secondary/40 hover:text-red-500 text-xs
        cursor-pointer transition-colors"
      onclick={removeCard}
    >
      &times;
    </button>
  </div>

  <!-- Editor + Preview -->
  <div class="grid grid-cols-2 min-h-[120px]">
    <textarea
      bind:value={markdown}
      oninput={handleInput}
      onblur={handleBlur}
      spellcheck="false"
      placeholder="Write markdown..."
      class="p-3 text-xs font-mono bg-bg text-text-primary border-r border-border
        resize-y focus:outline-none focus:ring-1 focus:ring-teal/40 min-h-[120px]"
    ></textarea>
    <div
      class="p-3 text-xs text-text-primary overflow-auto prose-sm
        [&_h1]:text-base [&_h1]:font-bold [&_h2]:text-sm [&_h2]:font-semibold
        [&_h3]:text-xs [&_h3]:font-semibold [&_code]:text-[10px]
        [&_code]:bg-surface-alt [&_code]:px-1 [&_code]:rounded
        [&_pre]:bg-surface-alt [&_pre]:p-2 [&_pre]:rounded
        [&_ul]:list-disc [&_ul]:pl-4 [&_ol]:list-decimal [&_ol]:pl-4"
    >
      {#if markdown}
        {@html renderedHtml}
      {:else}
        <span class="text-text-secondary/40 italic">Preview</span>
      {/if}
    </div>
  </div>
</div>
