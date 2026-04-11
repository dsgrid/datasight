<script lang="ts">
  import { queriesStore } from "$lib/stores/queries.svelte";
  import { schemaStore } from "$lib/stores/schema.svelte";
  import { sendMessage } from "$lib/api/chat";
  import { formatCost, formatDuration } from "$lib/utils/format";

  let filteredQueries = $derived.by(() => {
    const table = schemaStore.selectedTable;
    if (!table) return queriesStore.sessionQueries;
    const matched = queriesStore.sessionQueries.filter(
      (q) => q.sql && q.sql.toLowerCase().includes(table.toLowerCase()),
    );
    return matched.length > 0 ? matched : queriesStore.sessionQueries;
  });

  function rerunQuery(sql: string) {
    sendMessage(
      `Run this SQL query and display the results as a table:\n${sql}`,
    );
  }
</script>

<div class="space-y-1">
  {#if schemaStore.selectedTable && filteredQueries !== queriesStore.sessionQueries}
    <div class="text-[10px] text-text-secondary mb-1">
      {filteredQueries.length} queries for {schemaStore.selectedTable}
    </div>
  {/if}

  {#each filteredQueries as query, idx (idx)}
    <button
      class="w-full text-left py-2 border-b border-border/30
        hover:bg-teal/[0.04] transition-colors duration-100 cursor-pointer group"
      style="padding-left: 16px; padding-right: 16px;"
      onclick={() => rerunQuery(query.sql)}
    >
      <div class="text-text-primary line-clamp-2" style="font-size: 0.82rem; line-height: 1.4;">
        {query.sql.split("\n")[0]}
      </div>
      <div class="flex gap-2 mt-0.5 text-text-secondary" style="font-size: 0.7rem;">
        {#if query.execution_time_ms}
          <span>{formatDuration(query.execution_time_ms)}</span>
        {/if}
        {#if query.row_count !== undefined}
          <span>{query.row_count} rows</span>
        {/if}
        {#if query.turn_cost}
          <span>${formatCost(query.turn_cost)}</span>
        {/if}
      </div>
    </button>
  {/each}

  {#if filteredQueries.length === 0}
    <div class="text-[11px] text-text-secondary py-2 text-center">
      No queries yet
    </div>
  {/if}
</div>
