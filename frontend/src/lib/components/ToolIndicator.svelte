<script lang="ts">
  import { escapeHtml } from "$lib/utils/search";

  interface Props {
    tool: string;
    sql?: string;
    done?: boolean;
    onDelete?: () => void;
  }

  let { tool, sql, done = false, onDelete }: Props = $props();

  let toolLabel = $derived(
    tool === "run_sql" ? "Running SQL" : "Creating visualization",
  );

  let sqlPreview = $derived(
    sql ? (sql.length > 80 ? sql.substring(0, 80) + "..." : sql) : null,
  );
</script>

<div class="flex items-center text-text-secondary group max-w-[900px] mx-auto w-full animate-fade-in"
  style="gap: 8px; padding: 8px 14px; margin-bottom: 8px; font-size: 0.82rem;">
  <span
    class="rounded-full shrink-0 bg-teal"
    style="width: 6px; height: 6px; {done ? '' : 'animation: pulse 1s infinite;'}"
  ></span>
  <span>{toolLabel}...</span>
  {#if sqlPreview}
    <span class="font-mono text-teal truncate"
      style="font-size: 0.78em; background: rgba(21,168,168,0.08); padding: 2px 6px; border-radius: 4px; max-width: 400px;">
      {sqlPreview}
    </span>
  {/if}
  {#if onDelete}
    <button
      class="ml-auto p-1 rounded hover:bg-surface text-text-secondary
        opacity-0 group-hover:opacity-100 transition-opacity cursor-pointer"
      title="Delete"
      onclick={onDelete}
    >
      <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"><path d="M2 4h12M5.33 4V2.67a1.33 1.33 0 0 1 1.34-1.34h2.66a1.33 1.33 0 0 1 1.34 1.34V4M12.67 4v9.33a1.33 1.33 0 0 1-1.34 1.34H4.67a1.33 1.33 0 0 1-1.34-1.34V4" /></svg>
    </button>
  {/if}
</div>
