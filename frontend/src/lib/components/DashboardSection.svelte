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
  let editing = $state(false);

  let renderedHtml = $derived(renderMarkdown(markdown));

  function handleSave() {
    dashboardStore.updateItem(item.id, { markdown });
    editing = false;
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
  class="rounded-xl border border-dashed border-border bg-gradient-to-b
    from-teal/5 to-transparent overflow-hidden col-span-full
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
  <div class="flex items-center gap-1 px-3 py-1.5 border-b border-border/50">
    <span
      class="cursor-grab text-text-secondary/40 hover:text-text-secondary select-none text-xs"
    >
      ⠿
    </span>
    <input
      type="text"
      value={item.title || ""}
      onchange={handleTitleChange}
      placeholder="Section title..."
      class="flex-1 bg-transparent text-sm font-semibold text-text-primary
        placeholder:text-text-secondary/40 focus:outline-none"
    />
    <button
      class="text-[10px] text-text-secondary hover:text-teal cursor-pointer
        transition-colors"
      onclick={() => (editing = !editing)}
    >
      {editing ? "Done" : "Edit"}
    </button>
    <button
      class="text-text-secondary/40 hover:text-red-500 text-xs
        cursor-pointer transition-colors"
      onclick={removeCard}
    >
      &times;
    </button>
  </div>

  <!-- Content -->
  {#if editing}
    <div class="p-3">
      <textarea
        bind:value={markdown}
        spellcheck="false"
        placeholder="Section description (markdown)..."
        class="w-full min-h-[80px] px-3 py-2 text-xs font-mono bg-bg
          text-text-primary rounded border border-border resize-y
          focus:outline-none focus:ring-1 focus:ring-teal/40"
      ></textarea>
      <button
        class="mt-2 px-3 py-1 text-xs rounded bg-teal text-white
          hover:opacity-90 cursor-pointer"
        onclick={handleSave}
      >
        Save
      </button>
    </div>
  {:else if markdown}
    <div
      class="px-3 py-2 text-xs text-text-primary
        [&_h1]:text-base [&_h1]:font-bold [&_h2]:text-sm [&_h2]:font-semibold
        [&_code]:text-[10px] [&_code]:bg-surface-alt [&_code]:px-1
        [&_ul]:list-disc [&_ul]:pl-4"
    >
      {@html renderedHtml}
    </div>
  {/if}

  <!-- Separator line -->
  <div class="border-t border-dashed border-border/50"></div>
</div>
