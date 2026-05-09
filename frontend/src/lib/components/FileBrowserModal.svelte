<script lang="ts">
  import { browseDirectory, type BrowseDirectoryResult } from "$lib/api/projects";

  interface Props {
    open: boolean;
    onClose: () => void;
    onSelect: (path: string) => void;
    /** Optional starting directory; defaults to the server's CWD. */
    initialPath?: string | null;
  }

  let { open, onClose, onSelect, initialPath = null }: Props = $props();

  let listing = $state<BrowseDirectoryResult | null>(null);
  let loading = $state(false);
  let errorMsg = $state("");
  let manualPath = $state("");

  $effect(() => {
    if (open && listing === null) {
      void load(initialPath);
    }
  });

  async function load(path: string | null | undefined) {
    loading = true;
    errorMsg = "";
    try {
      const result = await browseDirectory(path ?? undefined);
      if (result.error) {
        errorMsg = result.error;
        if (listing === null) {
          // First load failed (likely a bogus initialPath) — fall back to CWD
          // so the user has *something* to navigate from.
          const fallback = await browseDirectory();
          listing = fallback.error ? null : fallback;
          manualPath = listing?.path ?? "";
        }
      } else {
        listing = result;
        manualPath = result.path;
      }
    } catch {
      errorMsg = "Failed to read directory";
    } finally {
      loading = false;
    }
  }

  function handleManualPath(e: KeyboardEvent) {
    if (e.key === "Enter") {
      void load(manualPath.trim() || null);
    }
  }

  function pickDir(path: string) {
    void load(path);
  }

  function pickFile(path: string) {
    onSelect(path);
    handleClose();
  }

  function pickCurrentDir() {
    if (listing) {
      onSelect(listing.path);
      handleClose();
    }
  }

  function handleClose() {
    listing = null;
    errorMsg = "";
    manualPath = "";
    onClose();
  }

  function handleKeydown(e: KeyboardEvent) {
    if (e.key === "Escape") handleClose();
  }

  function formatBytes(n: number): string {
    if (n < 1024) return `${n} B`;
    if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
    if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(1)} MB`;
    return `${(n / (1024 * 1024 * 1024)).toFixed(1)} GB`;
  }
</script>

{#if open}
  <!-- svelte-ignore a11y_no_static_element_interactions -->
  <div class="fixed inset-0 z-50" onkeydown={handleKeydown}>
    <!-- svelte-ignore a11y_click_events_have_key_events -->
    <!-- svelte-ignore a11y_no_static_element_interactions -->
    <div class="absolute inset-0" style="background: rgba(0,0,0,0.5);" onclick={handleClose}></div>

    <div
      class="fb-modal"
      role="dialog"
      aria-modal="true"
      aria-label="File browser"
    >
      <div class="fb-header">
        <div>
          <h3>Browse files</h3>
          <p>Pick a CSV, Parquet, Excel, DuckDB, or SQLite file — or a directory of CSV/Parquet shards.</p>
        </div>
        <button class="fb-close-btn" onclick={handleClose} title="Close">&times;</button>
      </div>

      <div class="fb-toolbar">
        <button
          class="fb-up-btn"
          disabled={!listing?.parent}
          title="Up to parent directory"
          aria-label="Go to parent directory"
          onclick={() => listing?.parent && pickDir(listing.parent)}
        >
          ↑
        </button>
        <input
          type="text"
          bind:value={manualPath}
          onkeydown={handleManualPath}
          placeholder="/absolute/path"
          aria-label="Current directory path"
        />
      </div>

      {#if errorMsg}
        <div class="fb-error">{errorMsg}</div>
      {/if}

      <div class="fb-listing">
        {#if loading}
          <div class="fb-empty">Loading…</div>
        {:else if !listing}
          <div class="fb-empty">No directory loaded.</div>
        {:else if listing.dirs.length === 0 && listing.files.length === 0}
          <div class="fb-empty">No subdirectories or supported files here.</div>
        {:else}
          <ul>
            {#each listing.dirs as d}
              <li>
                <button class="fb-row fb-row-dir" onclick={() => pickDir(d.path)}>
                  <span class="fb-icon" aria-hidden="true">📁</span>
                  <span class="fb-name">{d.name}/</span>
                </button>
              </li>
            {/each}
            {#each listing.files as f}
              <li>
                <button class="fb-row fb-row-file" onclick={() => pickFile(f.path)}>
                  <span class="fb-icon" aria-hidden="true">📄</span>
                  <span class="fb-name">{f.name}</span>
                  <span class="fb-meta">{f.type} · {formatBytes(f.size_bytes)}</span>
                </button>
              </li>
            {/each}
          </ul>
          {#if listing.truncated}
            <div class="fb-empty">Listing truncated — type a path above to navigate further.</div>
          {/if}
        {/if}
      </div>

      <div class="fb-footer">
        <button class="fb-btn secondary" onclick={handleClose}>Cancel</button>
        <button
          class="fb-btn primary"
          disabled={!listing}
          onclick={pickCurrentDir}
        >
          Use this folder
        </button>
      </div>
    </div>
  </div>
{/if}

<style>
  .fb-modal {
    position: fixed;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    width: min(720px, 92vw);
    max-height: 80vh;
    display: flex;
    flex-direction: column;
    border: 1px solid color-mix(in srgb, var(--teal) 16%, var(--border));
    border-radius: 18px;
    overflow: hidden;
    background: var(--surface);
    box-shadow: 0 24px 60px rgba(0, 0, 0, 0.28);
    z-index: 60;
  }

  .fb-header {
    display: flex;
    justify-content: space-between;
    gap: 16px;
    padding: 16px 20px;
    border-bottom: 1px solid var(--border);
  }

  .fb-header h3 {
    margin: 0 0 2px 0;
    font-size: 1rem;
    color: var(--text);
  }

  .fb-header p {
    margin: 0;
    font-size: 0.78rem;
    color: var(--text-secondary);
    line-height: 1.4;
  }

  .fb-close-btn {
    background: none;
    border: none;
    color: var(--text-secondary);
    font-size: 1.4rem;
    line-height: 1;
    cursor: pointer;
    padding: 4px;
  }
  .fb-close-btn:hover {
    color: var(--text);
  }

  .fb-toolbar {
    display: flex;
    gap: 8px;
    padding: 12px 20px;
    border-bottom: 1px solid var(--border);
    background: color-mix(in srgb, var(--bg) 60%, var(--surface));
  }

  .fb-up-btn {
    flex-shrink: 0;
    width: 36px;
    border: 1px solid var(--border);
    border-radius: 8px;
    background: var(--surface);
    color: var(--text);
    cursor: pointer;
    font-size: 1rem;
  }
  .fb-up-btn:disabled {
    opacity: 0.4;
    cursor: not-allowed;
  }
  .fb-up-btn:hover:not(:disabled) {
    background: color-mix(in srgb, var(--teal) 8%, var(--surface));
  }

  .fb-toolbar input {
    flex: 1;
    padding: 8px 12px;
    border: 1px solid var(--border);
    border-radius: 8px;
    background: var(--bg);
    color: var(--text);
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.8rem;
  }
  .fb-toolbar input:focus {
    outline: none;
    border-color: color-mix(in srgb, var(--teal) 55%, var(--border));
  }

  .fb-error {
    padding: 8px 20px;
    color: #ef4444;
    font-size: 0.78rem;
    background: color-mix(in srgb, #ef4444 8%, transparent);
  }

  .fb-listing {
    flex: 1;
    overflow-y: auto;
    padding: 6px 0;
  }

  .fb-listing ul {
    list-style: none;
    margin: 0;
    padding: 0;
  }

  .fb-empty {
    padding: 24px 20px;
    text-align: center;
    font-size: 0.82rem;
    color: var(--text-secondary);
  }

  .fb-row {
    display: flex;
    align-items: center;
    gap: 10px;
    width: 100%;
    padding: 8px 20px;
    border: none;
    background: transparent;
    color: var(--text);
    text-align: left;
    cursor: pointer;
    font: inherit;
    font-size: 0.86rem;
  }
  .fb-row:hover {
    background: color-mix(in srgb, var(--teal) 10%, transparent);
  }

  .fb-icon {
    flex-shrink: 0;
    width: 20px;
    text-align: center;
  }

  .fb-name {
    flex: 1;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .fb-row-dir .fb-name {
    color: color-mix(in srgb, var(--teal) 70%, var(--text));
    font-weight: 500;
  }

  .fb-meta {
    flex-shrink: 0;
    font-size: 0.72rem;
    color: var(--text-secondary);
    font-family: 'JetBrains Mono', monospace;
  }

  .fb-footer {
    display: flex;
    justify-content: flex-end;
    gap: 10px;
    padding: 12px 20px;
    border-top: 1px solid var(--border);
    background: color-mix(in srgb, var(--bg) 40%, var(--surface));
  }

  .fb-btn {
    padding: 8px 16px;
    border: none;
    border-radius: 10px;
    font: inherit;
    font-size: 0.82rem;
    font-weight: 600;
    cursor: pointer;
  }
  .fb-btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
  .fb-btn.secondary {
    background: color-mix(in srgb, var(--surface) 85%, var(--bg));
    border: 1px solid var(--border);
    color: var(--text);
  }
  .fb-btn.secondary:hover:not(:disabled) {
    background: color-mix(in srgb, var(--teal) 6%, var(--surface));
  }
  .fb-btn.primary {
    background: var(--teal);
    color: white;
  }
  .fb-btn.primary:hover:not(:disabled) {
    opacity: 0.9;
  }
</style>
