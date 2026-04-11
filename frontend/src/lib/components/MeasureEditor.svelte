<script lang="ts">
  import { sidebarStore } from "$lib/stores/sidebar.svelte";
  import { schemaStore } from "$lib/stores/schema.svelte";

  interface Props {
    onOpenModal: () => void;
  }

  let { onOpenModal }: Props = $props();

  let filteredMeasures = $derived.by(() => {
    const table = schemaStore.selectedTable;
    if (!table) return sidebarStore.measureEditorCatalog;
    return sidebarStore.measureEditorCatalog.filter(
      (m) => m.table === table,
    );
  });

  let scope = $derived(
    schemaStore.selectedTable
      ? schemaStore.selectedTable
      : "dataset",
  );
</script>

<div class="space-y-2">
  <!-- Summary -->
  <div class="flex items-center justify-between">
    <div class="text-xs text-text-secondary">
      {filteredMeasures.length} measures
      <span class="text-text-secondary/60">({scope})</span>
    </div>
    <button
      class="text-[10px] px-2 py-0.5 rounded bg-teal/10 text-teal
        hover:bg-teal/20 transition-colors cursor-pointer"
      onclick={onOpenModal}
    >
      Open Editor
    </button>
  </div>

  <!-- Measure list -->
  <div class="space-y-0.5">
    {#each filteredMeasures as measure, idx (idx)}
      <div
        class="py-1.5 border-b border-border/30 hover:bg-teal/[0.04] transition-colors duration-100"
      >
        <div class="flex items-center gap-1">
          <span class="font-medium text-text-primary" style="font-size: 0.82rem;">{measure.name}</span>
          {#if measure.is_override}
            <span
              class="text-[9px] px-1 py-px rounded bg-teal/10 text-teal"
            >
              custom
            </span>
          {/if}
          {#if measure.is_calculated}
            <span
              class="text-[9px] px-1 py-px rounded bg-orange/10 text-orange"
            >
              calc
            </span>
          {/if}
        </div>
        <div class="text-[10px] text-text-secondary flex gap-2">
          <span>{measure.table}</span>
          <span>{measure.aggregation}</span>
          {#if measure.display_name}
            <span class="italic">{measure.display_name}</span>
          {/if}
        </div>
      </div>
    {/each}

    {#if filteredMeasures.length === 0}
      <div class="text-[11px] text-text-secondary py-2 text-center">
        No measures found
      </div>
    {/if}
  </div>
</div>
