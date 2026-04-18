<script lang="ts">
  import { onMount } from "svelte";
  import { explore } from "$lib/api/projects";
  import {
    scanCwdForDataFiles,
    type ScannedDataFile,
  } from "$lib/api/projects";
  import { dashboardStore } from "$lib/stores/dashboard.svelte";

  interface Props {
    onExplored: () => void;
    onError: (msg: string) => void;
  }

  let { onExplored, onError }: Props = $props();

  let files = $state<ScannedDataFile[]>([]);
  let directory = $state("");
  let truncated = $state(false);
  let scanned = $state(false);
  let loading = $state(false);
  let error = $state("");

  onMount(async () => {
    try {
      const result = await scanCwdForDataFiles();
      files = result.files;
      directory = result.directory;
      truncated = result.truncated;
    } catch {
      // Silent — this card just hides itself if the scan fails.
    } finally {
      scanned = true;
    }
  });

  async function handleQuery() {
    error = "";
    if (files.length === 0) return;
    loading = true;
    try {
      const result = await explore(files.map((f) => f.path));
      if (!result.success) {
        error = result.error || "Failed to load files";
        onError(error);
        return;
      }
      dashboardStore.currentView = "sql";
      onExplored();
    } catch {
      error = "Failed to connect to server";
      onError(error);
    } finally {
      loading = false;
    }
  }

  function formatBytes(n: number): string {
    if (n < 1024) return `${n} B`;
    if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
    if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(1)} MB`;
    return `${(n / (1024 * 1024 * 1024)).toFixed(1)} GB`;
  }
</script>

{#if scanned && files.length > 0}
  <div
    class="bg-surface border border-border"
    style="padding: 28px; border-radius: 18px; margin-bottom: 18px;
           box-shadow: inset 0 1px 0 rgba(255,255,255,0.25), 0 14px 30px rgba(2,61,96,0.05);"
  >
    <div
      class="inline-flex items-center text-orange font-bold uppercase"
      style="padding: 4px 9px; margin-bottom: 10px; border-radius: 999px;
             background: color-mix(in srgb, var(--orange) 14%, transparent);
             font-size: 0.72rem; letter-spacing: 0.05em;"
    >
      Detected in this folder
    </div>
    <div
      class="text-text-primary"
      style="font-size: 1.1rem; font-weight: 600; margin-bottom: 6px;"
    >
      {files.length}
      {files.length === 1 ? "data file" : "data files"} ready to query
    </div>
    <p
      class="text-text-secondary"
      style="font-size: 0.82rem; margin-bottom: 14px; line-height: 1.5;
             font-family: 'JetBrains Mono', monospace; word-break: break-all;"
    >
      {directory}
    </p>

    <ul
      class="border border-border"
      style="list-style: none; padding: 6px 10px; margin: 0 0 14px 0;
             max-height: 180px; overflow-y: auto; border-radius: 10px;
             background: var(--bg);"
    >
      {#each files as file (file.path)}
        <li
          class="flex items-center justify-between"
          style="padding: 4px 2px; font-family: 'JetBrains Mono', monospace; font-size: 0.78rem;"
        >
          <span class="text-text-primary" style="word-break: break-all;">
            <span class="text-teal" style="font-weight: 600;">{file.type}</span>
            &nbsp;{file.name}
          </span>
          <span class="text-text-secondary" style="margin-left: 12px; white-space: nowrap;">
            {formatBytes(file.size_bytes)}
          </span>
        </li>
      {/each}
    </ul>

    {#if truncated}
      <p class="text-text-secondary" style="font-size: 0.75rem; margin-bottom: 10px;">
        Showing first {files.length} files. Only these will be loaded.
      </p>
    {/if}

    <button
      onclick={handleQuery}
      disabled={loading}
      class="bg-teal text-white font-medium cursor-pointer
        hover:opacity-90 transition-opacity disabled:opacity-50 disabled:cursor-not-allowed"
      style="padding: 10px 20px; border: none; border-radius: 8px;
             font-family: inherit; font-size: 0.9rem;"
    >
      {loading
        ? "Loading..."
        : `Query ${files.length === 1 ? "this file" : `these ${files.length} files`} in SQL`}
    </button>

    {#if error}
      <p style="font-size: 0.75rem; color: #e55; margin-top: 8px;">{error}</p>
    {/if}
  </div>
{/if}
