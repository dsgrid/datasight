<script lang="ts">
  import { dashboardStore } from "$lib/stores/dashboard.svelte";
  import { saveDashboard } from "$lib/api/dashboard";
  import DashboardCard from "./DashboardCard.svelte";
  import DashboardNote from "./DashboardNote.svelte";
  import DashboardSection from "./DashboardSection.svelte";

  let dragSourceIndex = $state<number | null>(null);

  let gridStyle = $derived.by(() => {
    const cols = dashboardStore.columns;
    if (cols === 0) return "grid-template-columns: repeat(auto-fill, minmax(450px, 1fr))";
    return `grid-template-columns: repeat(${cols}, 1fr)`;
  });

  function handleDragStart(index: number) {
    dragSourceIndex = index;
  }

  function handleDragOver(e: DragEvent, _index: number) {
    e.preventDefault();
  }

  function handleDrop(e: DragEvent, targetIndex: number) {
    e.preventDefault();
    if (dragSourceIndex === null || dragSourceIndex === targetIndex) return;
    dashboardStore.reorder(dragSourceIndex, targetIndex);
    dragSourceIndex = null;
    saveDashboard();
  }
</script>

{#if dashboardStore.pinnedItems.length === 0}
  <div class="flex-1 flex items-center justify-center">
    <div class="text-center text-text-secondary">
      <p class="text-sm">No items pinned yet</p>
      <p class="text-xs mt-1">
        Pin charts and tables from the chat, or add notes and sections above.
      </p>
    </div>
  </div>
{:else}
  <div class="flex-1 overflow-y-auto" style="padding: 0 22px 22px;">
    <div class="grid gap-4" style={gridStyle}>
      {#each dashboardStore.pinnedItems as item, idx (item.id)}
        {#if item.type === "note"}
          <DashboardNote
            {item}
            index={idx}
            onDragStart={handleDragStart}
            onDragOver={handleDragOver}
            onDrop={handleDrop}
          />
        {:else if item.type === "section"}
          <DashboardSection
            {item}
            index={idx}
            onDragStart={handleDragStart}
            onDragOver={handleDragOver}
            onDrop={handleDrop}
          />
        {:else}
          <DashboardCard
            {item}
            index={idx}
            onDragStart={handleDragStart}
            onDragOver={handleDragOver}
            onDrop={handleDrop}
          />
        {/if}
      {/each}
    </div>
  </div>
{/if}
