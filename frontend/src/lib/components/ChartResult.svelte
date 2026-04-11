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

<div class="tool-result group max-w-[900px] mx-auto w-full animate-fade-in" style="position: relative; margin-bottom: 16px;">
  <!-- Floating action buttons (visible on hover) -->
  {#if onBookmark}
    <button
      class="tool-action-btn opacity-0 group-hover:opacity-100"
      style="right: 112px;"
      onclick={handleBookmark}
    >
      {bookmarked ? "Saved!" : "Bookmark"}
    </button>
  {/if}
  {#if onSaveReport}
    <button
      class="tool-action-btn opacity-0 group-hover:opacity-100"
      style="right: 76px;"
      onclick={handleSaveReport}
    >
      {reportSaved ? "Saved!" : "Report"}
    </button>
  {/if}
  {#if onPin}
    <button
      class="tool-action-btn opacity-0 group-hover:opacity-100"
      style="right: 40px;"
      onclick={onPin}
    >
      Pin
    </button>
  {/if}
  {#if onDelete}
    <button
      class="tool-action-btn tool-action-btn-delete opacity-0 group-hover:opacity-100"
      style="right: 4px;"
      onclick={onDelete}
    >
      &times;
    </button>
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
