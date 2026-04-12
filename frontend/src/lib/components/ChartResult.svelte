<script lang="ts">
  interface Props {
    html: string;
    title?: string;
    onPin?: () => void;
    onBookmark?: () => void;
    onSaveReport?: () => void;
    onDelete?: () => void;
  }

  let { html, title, onPin, onBookmark, onSaveReport, onDelete }: Props =
    $props();

  let iframeEl = $state<HTMLIFrameElement | null>(null);
  let bookmarked = $state(false);
  let reportSaved = $state(false);

  /** Sync theme into chart iframe via postMessage. */
  function syncTheme() {
    if (!iframeEl?.contentWindow) return;
    const theme =
      document.documentElement.getAttribute("data-theme") || "light";
    try {
      iframeEl.contentWindow.postMessage({ type: "theme-change", theme }, "*");
    } catch {
      // cross-origin safety
    }
  }

  function handleBookmark() {
    onBookmark?.();
    bookmarked = true;
    setTimeout(() => (bookmarked = false), 1200);
  }

  function handleSaveReport() {
    onSaveReport?.();
    reportSaved = true;
    setTimeout(() => (reportSaved = false), 1200);
  }
</script>

<div class="tool-result group w-full animate-fade-in" style="position: relative; margin-bottom: 16px;">
  <!-- Floating action buttons (visible on hover) -->
  {#if onBookmark || onSaveReport || onPin || onDelete}
    <div class="tool-action-bar opacity-0 group-hover:opacity-100">
      {#if onBookmark}
        <button
          class="tool-action-btn"
          onclick={handleBookmark}
        >
          {bookmarked ? "Saved!" : "Bookmark"}
        </button>
      {/if}
      {#if onSaveReport}
        <button
          class="tool-action-btn"
          onclick={handleSaveReport}
        >
          {reportSaved ? "Saved!" : "Report"}
        </button>
      {/if}
      {#if onPin}
        <button
          class="tool-action-btn"
          onclick={onPin}
        >
          Pin
        </button>
      {/if}
      {#if onDelete}
        <button
          class="tool-action-btn tool-action-btn-delete"
          onclick={onDelete}
          title="Remove result"
        >
          &times;
        </button>
      {/if}
    </div>
  {/if}

  <iframe
    bind:this={iframeEl}
    sandbox="allow-scripts allow-same-origin allow-downloads"
    srcdoc={html}
    title={title || "Chart"}
    onload={syncTheme}
    class="w-full h-[480px] border border-border bg-surface"
    style="border-radius: var(--radius); box-shadow: var(--shadow);"
  ></iframe>
</div>
