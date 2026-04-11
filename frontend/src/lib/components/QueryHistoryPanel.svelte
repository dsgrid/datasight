<script lang="ts">
  import { queriesStore } from "$lib/stores/queries.svelte";
  import { sendMessage } from "$lib/api/chat";
  import { addBookmark } from "$lib/api/saved";
  import { toastStore } from "$lib/stores/toast.svelte";
  import { formatCost, formatDuration } from "$lib/utils/format";

  interface Props {
    open: boolean;
    onClose: () => void;
  }

  let { open, onClose }: Props = $props();

  let expandedIdx = $state<number | null>(null);

  function copySQL(sql: string) {
    navigator.clipboard.writeText(sql);
    toastStore.show("SQL copied", "success");
  }

  function rerunSQL(sql: string) {
    sendMessage(
      `Run this SQL query and display the results as a table:\n${sql}`,
    );
    onClose();
  }

  function bookmarkSQL(sql: string, tool: string) {
    addBookmark(sql, tool, "");
    toastStore.show("Bookmarked", "success");
  }

  function handleKeydown(e: KeyboardEvent) {
    if (e.key === "Escape") onClose();
  }
</script>

<svelte:window onkeydown={open ? handleKeydown : undefined} />

<aside
  class="w-80 flex-shrink-0 border-l border-border bg-surface flex flex-col
    overflow-hidden transition-[margin] duration-250 ease-in-out"
  style="margin-right: {open ? 0 : -320}px"
>
  <!-- Header -->
  <div class="flex items-center justify-between px-3 py-2 border-b border-border">
    <span class="text-xs font-semibold text-text-primary">SQL History</span>
    <div class="flex items-center gap-2">
      <span class="text-[10px] text-text-secondary">
        ${formatCost(queriesStore.sessionTotalCost)} total
      </span>
      <button
        class="text-text-secondary hover:text-text-primary text-xs cursor-pointer"
        onclick={onClose}
      >
        &times;
      </button>
    </div>
  </div>

  <!-- Query list -->
  <div class="flex-1 overflow-y-auto">
    {#if queriesStore.sessionQueries.length === 0}
      <div class="px-3 py-6 text-center text-xs text-text-secondary">
        No queries yet.
      </div>
    {:else}
      {#each queriesStore.sessionQueries as query, idx (idx)}
        <div
          class="border-b border-border px-3 py-2 hover:bg-surface-alt
            transition-colors {query.error ? 'border-l-2 border-l-orange' : ''}"
        >
          <!-- Meta -->
          <div class="flex items-center gap-1.5 mb-1">
            <span
              class="text-[9px] px-1 py-px rounded font-medium
                {query.tool === 'visualize_data'
                ? 'bg-teal/10 text-teal'
                : 'bg-surface-alt text-text-secondary'}"
            >
              {query.tool === "visualize_data" ? "Chart" : "SQL"}
            </span>
            {#if query.execution_time_ms}
              <span class="text-[10px] text-text-secondary">
                {formatDuration(query.execution_time_ms)}
              </span>
            {/if}
            {#if query.row_count !== undefined}
              <span class="text-[10px] text-text-secondary">
                {query.row_count} rows
              </span>
            {/if}
            {#if query.turn_cost}
              <span class="text-[10px] text-text-secondary">
                ${formatCost(query.turn_cost)}
              </span>
            {/if}
            {#if query.error}
              <span class="text-[10px] text-orange">error</span>
            {/if}
          </div>

          <!-- SQL (click to expand) -->
          <button
            class="w-full text-left cursor-pointer"
            onclick={() =>
              (expandedIdx = expandedIdx === idx ? null : idx)}
          >
            <pre
              class="text-[10px] font-mono text-text-primary whitespace-pre-wrap
                {expandedIdx === idx ? '' : 'line-clamp-2'}"
            >{query.sql}</pre>
          </button>

          <!-- Actions -->
          <div class="flex gap-1 mt-1">
            <button
              class="text-[9px] px-1.5 py-0.5 rounded bg-surface-alt
                text-text-secondary hover:text-text-primary cursor-pointer
                transition-colors"
              onclick={() => copySQL(query.sql)}
            >
              Copy
            </button>
            <button
              class="text-[9px] px-1.5 py-0.5 rounded bg-surface-alt
                text-text-secondary hover:text-teal cursor-pointer
                transition-colors"
              onclick={() => rerunSQL(query.sql)}
            >
              Rerun
            </button>
            <button
              class="text-[9px] px-1.5 py-0.5 rounded bg-surface-alt
                text-text-secondary hover:text-teal cursor-pointer
                transition-colors"
              onclick={() => bookmarkSQL(query.sql, query.tool)}
            >
              &#9734;
            </button>
          </div>
        </div>
      {/each}
    {/if}
  </div>
</aside>
