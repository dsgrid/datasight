<script lang="ts">
  import { sessionStore } from "$lib/stores/session.svelte";
  import { toastStore } from "$lib/stores/toast.svelte";

  interface Props {
    open: boolean;
    excludeIndices: Set<number>;
    onCancel: () => void;
    onExported: () => void;
  }

  let { open, excludeIndices, onCancel, onExported }: Props = $props();

  let exporting = $state(false);

  async function handleExport() {
    exporting = true;
    try {
      const res = await fetch(
        `/api/export/${encodeURIComponent(sessionStore.sessionId)}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            exclude_indices: Array.from(excludeIndices),
          }),
        },
      );

      if (!res.ok) {
        toastStore.show("Export failed", "error");
        return;
      }

      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "datasight-export.html";
      a.click();
      URL.revokeObjectURL(url);

      toastStore.show("Exported successfully", "success");
      onExported();
    } catch {
      toastStore.show("Export failed", "error");
    } finally {
      exporting = false;
    }
  }
</script>

{#if open}
  <div
    class="flex items-center justify-between px-4 py-2 border-t border-border
      bg-surface-alt"
  >
    <span class="text-xs text-text-secondary">
      Select messages to include in export
      {#if excludeIndices.size > 0}
        <span class="text-orange ml-1">
          ({excludeIndices.size} excluded)
        </span>
      {/if}
    </span>
    <div class="flex gap-2">
      <button
        class="px-3 py-1 text-xs rounded border border-border
          text-text-secondary hover:text-text-primary transition-colors
          cursor-pointer"
        onclick={onCancel}
      >
        Cancel
      </button>
      <button
        class="px-3 py-1 text-xs rounded bg-teal text-white
          hover:opacity-90 transition-opacity cursor-pointer
          disabled:opacity-50"
        disabled={exporting}
        onclick={handleExport}
      >
        {exporting ? "Exporting..." : "Export HTML"}
      </button>
    </div>
  </div>
{/if}
