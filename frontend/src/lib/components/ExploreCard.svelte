<script lang="ts">
  import { explore } from "$lib/api/projects";

  interface Props {
    onExplored: () => void;
    onError: (msg: string) => void;
  }

  let { onExplored, onError }: Props = $props();

  let path = $state("");
  let loading = $state(false);
  let error = $state("");

  async function handleExplore() {
    error = "";
    const trimmed = path.trim();
    if (!trimmed) {
      error = "Please enter a file or directory path";
      return;
    }

    loading = true;
    try {
      const result = await explore([trimmed]);
      if (!result.success) {
        error = result.error || "Failed to explore files";
        onError(error);
        return;
      }
      onExplored();
    } catch {
      error = "Failed to connect to server";
      onError(error);
    } finally {
      loading = false;
    }
  }

  function handleKeydown(e: KeyboardEvent) {
    if (e.key === "Enter") handleExplore();
  }
</script>

<div
  class="bg-surface border border-border"
  style="padding: 28px; border-radius: 18px;
         box-shadow: inset 0 1px 0 rgba(255,255,255,0.25), 0 14px 30px rgba(2,61,96,0.05);"
>
  <div
    class="inline-flex items-center text-teal font-bold uppercase"
    style="padding: 4px 9px; margin-bottom: 10px; border-radius: 999px;
           background: color-mix(in srgb, var(--teal) 14%, transparent);
           font-size: 0.72rem; letter-spacing: 0.05em;"
  >
    Quick open
  </div>
  <div class="text-text-primary" style="font-size: 1.1rem; font-weight: 600; margin-bottom: 10px;">
    Explore Files
  </div>
  <p class="text-text-secondary" style="font-size: 0.85rem; margin-bottom: 20px; line-height: 1.5;">
    Open CSV, Parquet, or DuckDB files to start exploring immediately.
  </p>
  <div class="flex" style="gap: 8px;">
    <input
      type="text"
      bind:value={path}
      onkeydown={handleKeydown}
      placeholder="Enter file or directory path..."
      class="flex-1 border border-border bg-bg text-text-primary
        focus:outline-none focus:border-teal"
      style="padding: 10px 12px; border-radius: 10px;
             font-family: 'JetBrains Mono', monospace; font-size: 0.8rem;"
    />
    <button
      onclick={handleExplore}
      disabled={loading}
      class="bg-teal text-white font-medium cursor-pointer
        hover:opacity-90 transition-opacity disabled:opacity-50 disabled:cursor-not-allowed whitespace-nowrap"
      style="padding: 10px 20px; border: none; border-radius: 8px; font-family: inherit; font-size: 0.85rem;"
    >
      {loading ? "Loading..." : "Explore"}
    </button>
  </div>
  {#if error}
    <p style="font-size: 0.75rem; color: #e55; margin-top: 8px;">{error}</p>
  {/if}
</div>
