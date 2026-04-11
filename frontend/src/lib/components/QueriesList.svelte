<script lang="ts">
  import { schemaStore } from "$lib/stores/schema.svelte";
  import { sendMessage } from "$lib/api/chat";

  let filteredQueries = $derived.by(() => {
    const table = schemaStore.selectedTable;
    if (!table) return schemaStore.allQueries;
    const matched = schemaStore.allQueries.filter(
      (q) => q.sql && q.sql.toLowerCase().includes(table.toLowerCase()),
    );
    return matched.length > 0 ? matched : schemaStore.allQueries;
  });
</script>

{#if schemaStore.selectedTable && filteredQueries !== schemaStore.allQueries}
  <div class="text-[10px] text-text-secondary" style="padding: 4px 16px 0;">
    {filteredQueries.length} queries for {schemaStore.selectedTable}
  </div>
{/if}

{#each filteredQueries as query, idx (idx)}
  <button
    class="w-full text-left hover:bg-teal/[0.04] transition-colors duration-100 cursor-pointer"
    style="padding: 6px 16px; border-bottom: 1px solid color-mix(in srgb, var(--border) 30%, transparent);"
    onclick={() => sendMessage(query.question)}
  >
    <div class="text-text-primary" style="font-size: 0.82rem; line-height: 1.4;">
      {query.question}
    </div>
  </button>
{/each}

{#if filteredQueries.length === 0}
  <div class="text-[11px] text-text-secondary py-2 text-center">
    No example queries available
  </div>
{/if}
