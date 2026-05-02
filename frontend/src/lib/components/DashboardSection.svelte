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

  function handleInput() {
    dashboardStore.updateItem(item.id, { markdown });
  }

  function handleSave() {
    dashboardStore.updateItem(item.id, { markdown });
    editing = false;
    saveDashboard();
  }

  function toggleEditing() {
    if (editing) {
      handleSave();
    } else {
      editing = true;
    }
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
  <div class="flex items-center gap-2 px-3 py-1.5 border-b border-border/50">
    <span
      class="cursor-grab text-text-secondary/40 hover:text-text-secondary select-none text-xs"
    >
      ⠿
    </span>
    <span class="flex-1 text-[10px] uppercase tracking-wide text-text-secondary/60">
      Section
    </span>
    <button
      class="text-[10px] text-text-secondary hover:text-teal cursor-pointer
        transition-colors"
      onclick={toggleEditing}
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
        oninput={handleInput}
        spellcheck="false"
        placeholder="Section content — markdown supported (**bold**, *italic*, ## heading, lists, code, links)..."
        class="w-full min-h-[120px] px-3 py-2 text-sm font-mono bg-bg
          text-text-primary rounded border border-border resize-y
          focus:outline-none focus:ring-1 focus:ring-teal/40"
      ></textarea>
    </div>
  {:else if markdown}
    <div
      class="px-4 py-3 text-sm text-text-primary
        [&_h1]:text-lg [&_h1]:font-bold [&_h1]:mb-2
        [&_h2]:text-base [&_h2]:font-semibold [&_h2]:mb-2
        [&_h3]:text-sm [&_h3]:font-semibold [&_h3]:mb-1
        [&_p]:mb-2 [&_strong]:font-semibold
        [&_code]:text-xs [&_code]:bg-surface-alt [&_code]:px-1 [&_code]:rounded
        [&_ul]:list-disc [&_ul]:pl-5 [&_ol]:list-decimal [&_ol]:pl-5
        [&_a]:text-teal [&_a]:underline"
    >
      {@html renderedHtml}
    </div>
  {:else}
    <div class="px-4 py-3 text-sm text-text-secondary/50 italic">
      Empty section — click Edit to add content.
    </div>
  {/if}
</div>
