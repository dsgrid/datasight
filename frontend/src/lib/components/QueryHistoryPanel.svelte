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
  class="history-panel"
  style="margin-right: {open ? 0 : -320}px"
>
  <!-- Header -->
  <div class="history-header">
    <span class="history-title">SQL History</span>
    <div class="history-header-right">
      {#if queriesStore.sessionTotalCost > 0}
        <span class="history-cost">{'$'}{formatCost(queriesStore.sessionTotalCost)} total</span>
      {/if}
      <button class="history-close" onclick={onClose} title="Close">&times;</button>
    </div>
  </div>

  <!-- Query list -->
  <div class="history-list">
    {#if queriesStore.sessionQueries.length === 0}
      <span class="history-empty">No queries yet.</span>
    {:else}
      {#each queriesStore.sessionQueries as query, idx (idx)}
        <div class="query-card {query.error ? 'error' : ''}">
          <!-- Header -->
          <div class="query-card-header">
            <span class="query-card-pill {query.error ? 'error' : ''}">
              {query.tool === "visualize_data" ? "Chart" : "SQL"}
            </span>
            {#if query.execution_time_ms}
              <span class="query-card-meta">{formatDuration(query.execution_time_ms)}</span>
            {/if}
            {#if query.row_count !== undefined}
              <span class="query-card-meta">{query.row_count} rows</span>
            {/if}
            {#if query.turn_cost}
              <span class="query-card-meta">{'$'}{formatCost(query.turn_cost)}</span>
            {/if}
            {#if query.error}
              <span class="query-card-meta error">error</span>
            {/if}
          </div>

          <!-- SQL -->
          <button
            class="query-card-sql {expandedIdx === idx ? 'expanded' : ''}"
            onclick={() => (expandedIdx = expandedIdx === idx ? null : idx)}
          >
            <pre>{query.formatted_sql || query.sql}</pre>
          </button>

          <!-- Actions -->
          <div class="query-card-actions">
            <button class="query-card-btn" onclick={() => copySQL(query.sql)}>Copy</button>
            <button class="query-card-btn" onclick={() => rerunSQL(query.sql)}>Rerun</button>
            <button class="query-card-btn" onclick={() => bookmarkSQL(query.sql, query.tool)}>&#9734;</button>
          </div>
        </div>
      {/each}
    {/if}
  </div>
</aside>

<style>
  .history-panel {
    width: 320px;
    flex-shrink: 0;
    border-left: 1px solid var(--border);
    background: var(--surface);
    display: flex;
    flex-direction: column;
    overflow: hidden;
    transition: margin 250ms ease-in-out;
  }

  .history-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 14px 12px 10px;
    border-bottom: 1px solid color-mix(in srgb, var(--border) 88%, transparent);
  }

  .history-title {
    font-size: 0.72rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--text-secondary);
  }

  .history-header-right {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .history-cost {
    font-size: 0.68rem;
    color: var(--text-secondary);
  }

  .history-close {
    background: none;
    border: none;
    color: var(--text-secondary);
    font-size: 1.1rem;
    line-height: 1;
    cursor: pointer;
    padding: 2px;
    transition: color 0.15s;
  }
  .history-close:hover {
    color: var(--text);
  }

  .history-list {
    flex: 1;
    overflow-y: auto;
    padding: 8px;
  }

  .history-list::-webkit-scrollbar { width: 5px; }
  .history-list::-webkit-scrollbar-track { background: transparent; }
  .history-list::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }

  .history-empty {
    display: block;
    padding: 8px;
    font-size: 0.82rem;
    color: var(--text-secondary);
    font-style: italic;
  }

  .query-card {
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 8px 10px;
    margin-bottom: 6px;
    transition: border-color 0.15s;
  }

  .query-card:hover {
    border-color: var(--teal);
  }

  .query-card.error {
    border-left: 3px solid var(--orange);
  }

  .query-card-header {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 6px;
    font-size: 0.72rem;
  }

  .query-card-pill {
    background: var(--teal);
    color: #fff;
    padding: 1px 6px;
    margin-right: 2px;
    border-radius: 3px;
    font-size: 0.65rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    flex-shrink: 0;
  }

  .query-card-pill.error {
    background: var(--orange);
  }

  .query-card-meta {
    color: var(--text-secondary);
    font-size: 0.7rem;
  }

  .query-card-meta.error {
    color: var(--orange);
  }

  .query-card-sql {
    display: block;
    width: 100%;
    text-align: left;
    background: transparent;
    border: none;
    padding: 0;
    cursor: pointer;
    font: inherit;
  }

  .query-card-sql pre {
    margin: 0;
    white-space: pre-wrap;
    word-wrap: break-word;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.78rem;
    line-height: 1.6;
    color: var(--text);
    max-height: 4.5em;
    overflow: hidden;
  }

  .query-card-sql.expanded pre {
    max-height: none;
  }

  .query-card-actions {
    display: flex;
    gap: 4px;
    margin-top: 6px;
  }

  .query-card-btn {
    background: transparent;
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 1px 7px;
    font-family: inherit;
    font-size: 0.65rem;
    color: var(--text-secondary);
    cursor: pointer;
    transition: all 0.15s;
  }

  .query-card-btn:hover {
    color: var(--text);
    border-color: var(--teal);
  }
</style>
